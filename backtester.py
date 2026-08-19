# backtester.py
# =============================================================================
# Funding 策略回測框架
# =============================================================================
#
# 【功能說明】
#   對 Funding 借貸策略做歷史回測，評估績效與風險
#   可同時跑多個策略做橫向比較
#
# 【核心流程】
#   1. load_historical_ohlcv() — 載入 K 線（本地 DB 或 Bitfinex API）
#   2. simulate_funding_market() — 根據 K 線計算波動率，產生 Funding 市場快照
#   3. BacktestEngine.run() — 逐根 K 線餵給策略函數，策略返回 action，引擎執行
#   4. 引擎自動平倉、計算績效指標
#   5. print_report() / save_results() — 輸出報告或寫入 JSON
#
# 【績效指標（PerformanceMetrics）】
#   - 總報酬率、年化報酬、夏普率、Sortino、卡爾馬比率
#   - 最大回撤（% 與持續天數）
#   - 勝率、盈虧比、平均獲利、平均虧損
#   - 總交易次數、平均持倉時間
#
# 【策略函數簽名】
#   def strategy(candle, funding, portfolio, state) -> dict:
#       # candle: Candle 單根 K 線（timestamp/open/high/low/close/vol）
#       # funding: FundingSnapshot 當前市場快照（frr/top_wall/波動率）
#       # portfolio: {cash, position, equity, total_trades}
#       # state: dict — 策略自訂狀態（可跨 K 線保留）
#       #
#       # 返回：{
#       #     "action": "lend" | "wait" | "close",
#       #     "amount": USD金額,
#       #     "rate": 日利率（小數）,
#       #     "period": 天數（2/7/120）,
#       #     "state": 策略狀態名稱,
#       #     "note": 備註
#       # }
#
# 【內建策略工廠】
#   - make_frr_bottom_strategy()   低波動：盯 FRR 掛單
#   - make_ladder_strategy()       高波動：階梯掛單
#   - make_wall_grab_strategy()    中波動：偵測大牆，掛在牆前
#   - make_momentum_strategy()     動量策略（用 K 線高低區間）
#
# 【利率單位說明】
#   - frr_daily: 日利率（小數形式，如 0.00031205）
#   - frr_annual: 年化（小數形式，如 0.1471 = 14.71%）
#   - display: 顯示時乘以 100 變成百分比
# =============================================================================

import json
import time
import math
import logging
import sqlite3
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

# =============================================================================
# 資料類型
# =============================================================================

@dataclass
class Candle:
    """
    一根 K 線資料

    屬性：
        timestamp: 毫秒 Unix 時間戳
        open/high/low/close: 開高低收
        volume: 成交量
    """
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class FundingSnapshot:
    """
    Funding 市場快照（某一時刻的市場狀態）

    屬性：
        timestamp: 時間戳
        frr_daily: FRR 日利率（小數）
        frr_annual: FRR 年化（小數）
        top_bid_rate: 最佳買牆利率（小數）
        top_bid_amount: 最佳買牆金額
        top_ask_rate: 最佳賣牆利率（小數）
        top_ask_amount: 最佳賣牆金額
        volatility: 波動率
    """
    timestamp: int
    frr_daily: float        # 日利率（小數）
    frr_annual: float      # 年化（小數）
    top_bid_rate: float
    top_bid_amount: float
    top_ask_rate: float
    top_ask_amount: float
    volatility: float


@dataclass
class Trade:
    """
    一筆借出交易（平倉後記錄）

    屬性：
        entry_time/exit_time: 入出時間戳
        side: "lend"（借出方向）
        entry_rate/exit_rate: 入出利率（小數）
        amount: 本金（USD）
        pnl: 利息收入（USD）
        pnl_pct: 利息回報率（%）
        duration_hours: 持倉小時數
        strategy_name: 策略名稱
        state: 平倉時的策略狀態
    """
    entry_time: int
    exit_time: int
    side: str
    entry_rate: float
    exit_rate: float
    amount: float
    pnl: float
    pnl_pct: float
    duration_hours: float
    strategy_name: str
    state: str


@dataclass
class PortfolioSnapshot:
    """
    每個時間點的帳戶狀態快照（目前未使用，保留擴充）
    """
    timestamp: int
    cash: float
    position_value: float
    total_value: float
    unrealized_pnl: float


# =============================================================================
# 績效指標
# =============================================================================

@dataclass
class PerformanceMetrics:
    """
    回測績效指標全集

    包含報酬、風險、交易統計三大類指標
    """
    total_return: float           # 總報酬（USD）
    total_return_pct: float      # 總報酬率（%）
    sharpe_ratio: float          # 夏普率（年化）
    sortino_ratio: float         # Sortino 比率
    max_drawdown: float          # 最大回撤（小數）
    max_drawdown_pct: float     # 最大回撤（%）
    max_drawdown_duration_days: float
    win_rate: float              # 勝率
    avg_win: float               # 平均獲利（USD）
    avg_loss: float              # 平均虧損（USD）
    profit_factor: float         # 盈虧比
    total_trades: int            # 總交易次數
    winning_trades: int          # 獲利次數
    losing_trades: int           # 虧損次數
    avg_duration_hours: float   # 平均持倉（小時）
    avg_daily_return: float
    daily_volatility: float
    annual_return: float         # 年化報酬率（小數）
    calmar_ratio: float          # 卡爾馬比率（年化 / 最大回撤）
    avg_trade_pnl: float        # 平均每筆收益（USD）

    @classmethod
    def from_trades(cls, trades: List[Trade], equity_curve: List[float],
                    timestamps: List[int], initial_capital: float,
                    risk_free_rate: float = 0.0) -> "PerformanceMetrics":
        """
        從交易記錄和權益曲線計算績效指標

        參數：
            trades: 平倉交易列表
            equity_curve: 權益曲線（每根 K 線的總資產）
            timestamps: 對應時間戳
            initial_capital: 初始本金
            risk_free_rate: 無風險利率（年化，預設 0）

        回傳：
            PerformanceMetrics 實例
        """

        if not trades:
            return cls(
                total_return=0, total_return_pct=0, sharpe_ratio=0,
                sortino_ratio=0, max_drawdown=0, max_drawdown_pct=0,
                max_drawdown_duration_days=0, win_rate=0, avg_win=0,
                avg_loss=0, profit_factor=0, total_trades=0, winning_trades=0,
                losing_trades=0, avg_duration_hours=0, avg_daily_return=0,
                daily_volatility=0, annual_return=0, calmar_ratio=0,
                avg_trade_pnl=0
            )

        # 基本統計
        pnls = [t.pnl for t in trades]
        winning = [p for p in pnls if p > 0]
        losing = [p for p in pnls if p < 0]

        total_return = sum(pnls)
        total_return_pct = (total_return / initial_capital) * 100

        # 勝率與盈虧
        win_rate = len(winning) / len(pnls) if pnls else 0
        avg_win = np.mean(winning) if winning else 0
        avg_loss = abs(np.mean(losing)) if losing else 0
        profit_factor = (sum(winning) / sum(losing)) if losing and sum(losing) != 0 else float('inf')

        # 持倉時間
        durations = [t.duration_hours for t in trades]
        avg_duration = np.mean(durations) if durations else 0

        # 最大回撤計算
        peak = initial_capital
        max_dd = 0.0
        max_dd_duration = 0.0
        dd_start = None

        equity_arr = np.array(equity_curve)
        for i, eq in enumerate(equity_arr):
            if eq > peak:
                peak = eq
                dd_start = None
            else:
                dd = (peak - eq) / peak
                if dd > max_dd:
                    max_dd = dd
                    if dd_start is None:
                        dd_start = i
                    dd_duration = 0
                    for j in range(i, len(equity_arr)):
                        if equity_arr[j] >= peak * (1 - max_dd):
                            break
                        dd_duration += 1
                    max_dd_duration = dd_duration
                elif dd_start is not None:
                    max_dd_duration += 1

        max_dd_pct = max_dd * 100

        # 日報酬率分析（從 equity curve）
        if len(equity_curve) > 1:
            returns = np.diff(equity_curve) / equity_curve[:-1]
            returns = returns[~np.isnan(returns) & ~np.isinf(returns)]
            avg_daily = np.mean(returns) if len(returns) > 0 else 0
            daily_vol = np.std(returns) if len(returns) > 0 else 0

            # 年化
            annual_return = avg_daily * 365
            annual_vol = daily_vol * math.sqrt(365)

            # 夏普率
            excess = annual_return - risk_free_rate
            sharpe = excess / annual_vol if annual_vol > 0 else 0

            # Sortino（只看負報酬）
            downside = returns[returns < 0]
            downside_vol = np.std(downside) * math.sqrt(365) if len(downside) > 1 else 0.001
            sortino = excess / downside_vol if downside_vol > 0 else 0

            # 卡爾馬比率
            calmar = annual_return / max_dd if max_dd > 0 else 0
        else:
            avg_daily = daily_vol = annual_return = sharpe = sortino = calmar = 0

        return cls(
            total_return=total_return,
            total_return_pct=total_return_pct,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            max_drawdown=max_dd,
            max_drawdown_pct=max_dd_pct,
            max_drawdown_duration_days=max_dd_duration,
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss,
            profit_factor=profit_factor,
            total_trades=len(trades),
            winning_trades=len(winning),
            losing_trades=len(losing),
            avg_duration_hours=avg_duration,
            avg_daily_return=avg_daily,
            daily_volatility=daily_vol,
            annual_return=annual_return,
            calmar_ratio=calmar,
            avg_trade_pnl=np.mean(pnls) if pnls else 0
        )


# =============================================================================
# 資料載入器
# =============================================================================

def load_historical_ohlcv(symbol: str = "BTC/USD", timeframe: str = "15m",
                          since: str = None, until: str = None,
                          limit: int = 10000) -> List[Candle]:
    """
    從本地資料庫載入歷史 K 線數據
    （若本地無資料，自動從 Bitfinex API 同步）

    參數：
        symbol: 交易對（"BTC/USD"）
        timeframe: 時間框（"1m", "5m", "15m", "1h", "4h", "1D"）
        since: 起始日期 ISO 字串（如 "2026-01-01"）
        until: 結束日期 ISO 字串（如 "2026-06-18"）
        limit: 最多載入多少根 K 線

    回傳：
        Candle 列表
    """
    # 優先從本地資料庫讀取
    try:
        from market_db import MarketDB
        db = MarketDB()

        start_ts = int(datetime.fromisoformat(since).timestamp() * 1000) if since else None
        end_ts = int(datetime.fromisoformat(until).timestamp() * 1000) if until else None

        candles = db.get_ohlcv(symbol, timeframe, since=start_ts, until=end_ts, limit=limit)
        db.close()

        if candles:
            logger.info(f"[load_ohlcv] 從本地資料庫取得 {len(candles)} 根 K 線 {symbol} {timeframe}")
            return candles

        logger.info(f"[load_ohlcv] 本地資料庫無資料，嘗試從 API 同步...")
    except Exception as e:
        logger.warning(f"[load_ohlcv] 資料庫讀取失敗: {e}")

    # Fallback：直接從 Bitfinex API 拉（需要網路）
    import ccxt
    exchange = ccxt.bitfinex({"enableRateLimit": True})

    start_ts = int(datetime.fromisoformat(since).timestamp() * 1000) if since else None
    end_ts = int(datetime.fromisoformat(until).timestamp() * 1000) if until else None

    all_candles = []
    current_ts = start_ts

    while True:
        params = {"sort": 1}
        if current_ts:
            params["start"] = current_ts

        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit, params=params)
        except Exception as e:
            logger.error(f"載入 K 線失敗: {e}")
            break

        if not ohlcv:
            break

        for c in ohlcv:
            ts = int(c[0])
            if end_ts and ts > end_ts:
                break
            all_candles.append(Candle(
                timestamp=ts,
                open=float(c[1]),
                high=float(c[2]),
                low=float(c[3]),
                close=float(c[4]),
                volume=float(c[5])
            ))

        if end_ts and ohlcv[-1][0] >= end_ts:
            break

        current_ts = ohlcv[-1][0] + 1
        if len(ohlcv) < limit:
            break
        if len(all_candles) >= limit:
            break

    logger.info(f"載入 {len(all_candles)} 根 K 線 {symbol} {timeframe}")
    return all_candles


def simulate_funding_market(candles: List[Candle],
                             frr_snapshots: List[FundingSnapshot] = None,
                             base_frr_annual: float = 0.04,
                             vol_multiplier: float = 3.0) -> List[FundingSnapshot]:
    """
    模擬 Funding 市場數據

    邏輯：
    - 若有真實 frr_snapshots：使用真實 FRR，否則用 base_frr_annual 模擬
    - 波動率：根據 K 線收盤價真實計算（滾動 20 根標準差）
    - 牆單：根據 FRR 上下浮動 5%，隨機金額

    參數：
        candles: K 線列表
        frr_snapshots: 真實 FRR 快照（可選）
        base_frr_annual: 預設年化 FRR（無真實數據時使用）
        vol_multiplier: 波動率對 FRR 的加成倍數

    回傳：
        FundingSnapshot 列表（與 K 線一一對應）
    """
    # 建立 FRR 查找表（timestamp → snapshot）
    frr_map: Dict[int, FundingSnapshot] = {}
    if frr_snapshots:
        for fs in frr_snapshots:
            frr_map[fs.timestamp] = fs

    frr_keys = sorted(frr_map.keys()) if frr_map else []
    snapshots = []

    for i, c in enumerate(candles):
        # 計算波動率（滾動 20 根收盤價的日化標準差）
        lookback = min(20, i)
        if lookback > 0:
            window = candles[i - lookback:i]
            returns = [(window[j].close - window[j - 1].close) / window[j - 1].close
                      for j in range(1, len(window))]
            if returns:
                mean_r = sum(returns) / len(returns)
                variance = sum((r - mean_r) ** 2 for r in returns) / len(returns)
                daily_vol = variance ** 0.5
                volatility = daily_vol
            else:
                volatility = 0.0
        else:
            volatility = 0.0

        # 找最接近的真實 FRR（funding snapshot 是固定間隔的）
        fs = None
        if frr_keys:
            for k in reversed(frr_keys):
                if k <= c.timestamp:
                    fs = frr_map[k]
                    break
            if fs is None and frr_keys:
                fs = frr_map[frr_keys[0]]

        if fs:
            frr_daily = fs.frr_daily
            frr_annual = fs.frr_annual
        else:
            # 模擬 FRR：base + 波動率加成 + 隨機種子
            import random
            noise = random.uniform(-0.001, 0.001)
            frr_annual = max(0.001, base_frr_annual + volatility * vol_multiplier + noise)
            frr_daily = frr_annual / 365

        # 模擬牆單（根據 FRR 上下浮動）
        import random
        top_bid_rate = frr_daily * 0.95
        top_ask_rate = frr_daily * 1.05
        top_bid_amount = random.uniform(100, 2000)
        top_ask_amount = random.uniform(100, 2000)

        snapshots.append(FundingSnapshot(
            timestamp=c.timestamp,
            frr_daily=frr_daily,
            frr_annual=frr_annual,
            top_bid_rate=top_bid_rate,
            top_bid_amount=top_bid_amount,
            top_ask_rate=top_ask_rate,
            top_ask_amount=top_ask_amount,
            volatility=volatility
        ))

    return snapshots


def load_ledger_history(user_id: str = "920397507",
                         ledger_path: str = "data/ledger.json") -> List[Dict]:
    """
    載入歷史帳本（利息收入）

    參數：
        user_id: 使用者 ID
        ledger_path: 帳本 JSON 檔案路徑

    回傳：
        該使用者的帳本記錄列表
    """
    try:
        with open(ledger_path, "r") as f:
            data = json.load(f)
        return data.get("ledger", {}).get(user_id, [])
    except Exception as e:
        logger.warning(f"無法載入帳本: {e}")
        return []


# =============================================================================
# 回測引擎
# =============================================================================

class BacktestEngine:
    """
    通用回測引擎

    功能：
    - 逐根 K 線餵給策略函數，策略返回 action
    - 支援三種 action：lend（借出）、close（結算）、wait（觀望）
    - 自動維護倉位、計算利息、更新權益曲線
    - 最後計算完整績效指標

    使用方式：
        engine = BacktestEngine(candles, funding_snapshots, initial_capital=1000, name="FRR_BOTTOM")
        results = engine.run(my_strategy)
        engine.print_report(results)
    """

    def __init__(self, candles: List[Candle],
                 funding_snapshots: List[FundingSnapshot] = None,
                 initial_capital: float = 1000.0,
                 name: str = "Backtest"):

        self.name = name
        self.candles = candles
        self.funding_snapshots = funding_snapshots or []

        # 建立 funding 查找表（timestamp → snapshot）
        self._funding_map: Dict[int, FundingSnapshot] = {}
        for fs in self.funding_snapshots:
            self._funding_map[fs.timestamp] = fs

        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.position: Optional[Dict] = None  # 當前倉位（None = 無倉位）

        self.trades: List[Trade] = []        # 已平倉交易列表
        self.equity_curve: List[float] = []  # 權益曲線
        self.timestamps: List[int] = []       # 對應時間戳

        # 策略自訂狀態（可跨 K 線保留）
        self.current_state: Dict[str, Any] = {}

        logger.info(f"[{name}] 初始化：{len(candles)} 根 K 線，初始本金=${initial_capital}")

    def _get_funding(self, timestamp: int) -> Optional[FundingSnapshot]:
        """
        取得最接近該時間戳的 Funding 快照

        往前找最近的一筆（保證不用未來的數據）
        """
        keys = sorted(self._funding_map.keys(), reverse=True)
        for k in keys:
            if k <= timestamp:
                return self._funding_map[k]
        return self._funding_map.get(sorted(keys)[0]) if keys else None

    def _equity(self) -> float:
        """
        計算當前總資產

        包含：現金 + 倉位估值（本金 + 累計利息）
        """
        total = self.cash
        if self.position:
            days_held = (self.candles[-1].timestamp - self.position["entry_time"]) / (1000 * 86400)
            accrued = self.position["amount"] * self.position["entry_rate"] * days_held
            total += self.position["amount"] + accrued
        return total

    def run(self, strategy_fn: Callable, progress: bool = True) -> Dict:
        """
        執行回測

        參數：
            strategy_fn: 策略函數
                def strategy(candle, funding, portfolio, state) -> dict
                返回：{
                    "action": "lend" | "wait" | "close",
                    "amount": USD金額,
                    "rate": 日利率（小數）,
                    "period": 天數（可選）,
                    "state": 策略狀態名（可選）,
                    "note": 備註（可選）
                }
            progress: 是否顯示進度（每 100 根印一次）

        回傳：
            包含 metrics/trades/equity_curve 的字典
        """
        total = len(self.candles)

        for i, candle in enumerate(self.candles):
            funding = self._get_funding(candle.timestamp)

            # 建構 portfolio 供策略參考
            portfolio = {
                "cash": self.cash,
                "position": self.position,
                "equity": self._equity(),
                "total_trades": len(self.trades)
            }

            # 策略決策
            try:
                decision = strategy_fn(candle, funding, portfolio, self.current_state)
            except Exception as e:
                logger.error(f"策略函數錯誤 @ candle {i}: {e}")
                decision = {"action": "wait"}

            action = decision.get("action", "wait")

            # 執行 action
            if action == "lend" and self.cash >= decision.get("amount", 0):
                self._execute_lend(decision, candle)
            elif action == "close" and self.position:
                self._execute_close(decision, candle)

            # 記錄 equity
            self.equity_curve.append(self._equity())
            self.timestamps.append(candle.timestamp)

            if progress and i % 100 == 0:
                pct = i / total * 100
                logger.info(f"[{self.name}] 進度 {pct:.1f}% ({i}/{total})")

        # 最終平倉（回測結束時强制結算）
        if self.position:
            final_candle = self.candles[-1]
            self._execute_close({}, final_candle)

        # 計算績效指標
        metrics = PerformanceMetrics.from_trades(
            self.trades,
            self.equity_curve,
            self.timestamps,
            self.initial_capital
        )

        return {
            "metrics": metrics,
            "trades": self.trades,
            "equity_curve": self.equity_curve,
            "timestamps": self.timestamps,
            "initial_capital": self.initial_capital,
            "final_capital": self.equity_curve[-1] if self.equity_curve else self.initial_capital,
        }

    def _execute_lend(self, decision: dict, candle: Candle):
        """
        執行借出（掛單）

        參數：
            decision: 策略決策 dict
            candle: 當前 K 線
        """
        amount = decision.get("amount", 0)
        if amount <= 0 or amount > self.cash:
            return

        self.cash -= amount
        self.position = {
            "entry_time": candle.timestamp,
            "entry_rate": decision.get("rate", 0),
            "amount": amount,
            "period": decision.get("period", 7),
            "strategy": decision.get("state", "UNKNOWN"),
            "note": decision.get("note", "")
        }

        logger.debug(f"[{self.name}] 借出 ${amount} @ {decision.get('rate', 0)*365*100:.2f}%/年")

    def _execute_close(self, decision: dict, candle: Candle):
        """
        執行結算（平倉）

        參數：
            decision: 策略決策 dict（可為空，表示强制結算）
            candle: 當前 K 線
        """
        if not self.position:
            return

        pos = self.position
        days_held = (candle.timestamp - pos["entry_time"]) / (1000 * 86400)
        rate = decision.get("rate", pos["entry_rate"]) if decision else pos["entry_rate"]

        earned = pos["amount"] * rate * days_held
        pnl = earned

        trade = Trade(
            entry_time=pos["entry_time"],
            exit_time=candle.timestamp,
            side="lend",
            entry_rate=pos["entry_rate"],
            exit_rate=rate,
            amount=pos["amount"],
            pnl=pnl,
            pnl_pct=(pnl / pos["amount"]) * 100,
            duration_hours=days_held * 24,
            strategy_name=pos["strategy"],
            state=pos["strategy"]
        )

        self.trades.append(trade)
        self.cash += pos["amount"] + earned
        self.position = None

        logger.debug(f"[{self.name}] 結算 ${pos['amount']} + ${earned:.2f} ({days_held:.2f}天)")

    def print_report(self, results: Dict):
        """
        將績效報告輸出到標準輸出

        參數：
            results: BacktestEngine.run() 的回傳值
        """
        m = results["metrics"]

        print()
        print("=" * 60)
        print(f"  📊 回測報告：{self.name}")
        print("=" * 60)
        print(f"  初始本金    ：${results['initial_capital']:.2f}")
        print(f"  最終資產    ：${results['final_capital']:.2f}")
        print(f"  總報酬      ：{m.total_return_pct:+.2f}%")
        print(f"  年化報酬    ：{m.annual_return*100:+.2f}%")
        print()
        print(f"  ── 風險調整 ──────────────────────────────────")
        print(f"  夏普率      ：{m.sharpe_ratio:+.2f}")
        print(f"  Sortino     ：{m.sortino_ratio:+.2f}")
        print(f"  卡爾馬比率  ：{m.calmar_ratio:+.2f}")
        print(f"  最大回撤    ：{m.max_drawdown_pct:.2f}%")
        print(f"  日波動率    ：{m.daily_volatility*100:.2f}%")
        print()
        print(f"  ── 交易統計 ──────────────────────────────────")
        print(f"  總交易次數  ：{m.total_trades}")
        print(f"  獲利次數    ：{m.winning_trades}")
        print(f"  虧損次數    ：{m.losing_trades}")
        print(f"  勝率        ：{m.win_rate*100:.1f}%")
        print(f"  盈虧比      ：{m.profit_factor:.2f}" if m.profit_factor != float('inf') else f"  盈虧比      ：∞")
        print(f"  平均每筆    ：${m.avg_trade_pnl:.2f}")
        print(f"  平均持倉    ：{m.avg_duration_hours:.1f} 小時")
        print()
        print(f"  ── 收益分析 ──────────────────────────────────")
        print(f"  平均獲利    ：${m.avg_win:.2f}")
        print(f"  平均虧損    ：${m.avg_loss:.2f}")
        print(f"  日均報酬    ：{m.avg_daily_return*100:.4f}%")
        print("=" * 60)

    def save_results(self, results: Dict, path: str = "backtest_results.json"):
        """
        將回測結果寫入 JSON 檔案

        參數：
            results: BacktestEngine.run() 的回傳值
            path: 輸出檔案路徑
        """
        out = {
            "name": self.name,
            "initial_capital": results["initial_capital"],
            "final_capital": results["final_capital"],
            "metrics": {
                "total_return_pct": results["metrics"].total_return_pct,
                "sharpe_ratio": results["metrics"].sharpe_ratio,
                "sortino_ratio": results["metrics"].sortino_ratio,
                "max_drawdown_pct": results["metrics"].max_drawdown_pct,
                "win_rate": results["metrics"].win_rate,
                "profit_factor": results["metrics"].profit_factor,
                "total_trades": results["metrics"].total_trades,
                "annual_return": results["metrics"].annual_return,
            },
            "num_trades": len(results["trades"]),
            "equity_curve_len": len(results["equity_curve"]),
        }
        with open(path, "w") as f:
            json.dump(out, f, indent=2)
        logger.info(f"結果已儲存到 {path}")


# =============================================================================
# 內建策略工廠
# =============================================================================

def make_frr_bottom_strategy(threshold_vol: float = 0.15,
                              frr_premium: float = 1.10) -> Callable:
    """
    FRR_BOTTOM 策略工廠

    策略邏輯：
    - 波動率 < threshold_vol（15%）：低波動，借出（掛在 FRR × premium）
    - 否則：觀望

    參數：
        threshold_vol: 波動率門檻
        frr_premium: FRR 加成倍率

    回傳：
        策略函數
    """
    def strategy(candle: Candle, funding, portfolio: dict, state: dict) -> dict:
        if funding is None:
            return {"action": "wait"}

        vol = funding.volatility

        if vol < threshold_vol:
            rate = funding.frr_daily * frr_premium
            return {
                "action": "lend",
                "amount": min(150, portfolio["cash"]),
                "rate": rate,
                "period": 2,
                "state": "FRR_BOTTOM",
                "note": f"vol={vol:.3f}"
            }
        else:
            return {"action": "wait", "state": "WATCH"}

    return strategy


def make_ladder_strategy(levels: int = 5, max_offer_usd: float = 150) -> Callable:
    """
    LADDER 策略工廠

    策略邏輯：
    - 波動率 > 30%：高波動，掛在 FRR × 1.20
    - 否則：觀望

    參數：
        levels: 階梯層級數（目前未使用，保留擴充）
        max_offer_usd: 每筆最大借出金額

    回傳：
        策略函數
    """
    def strategy(candle: Candle, funding, portfolio: dict, state: dict) -> dict:
        if funding is None:
            return {"action": "wait"}

        vol = funding.volatility

        if vol > 0.30:
            base_rate = funding.frr_daily * 1.20
            rate = round(base_rate * 100000) / 100000

            return {
                "action": "lend",
                "amount": min(max_offer_usd, portfolio["cash"]),
                "rate": rate,
                "period": 7,
                "state": "LADDER",
                "note": f"vol={vol:.3f}"
            }
        else:
            return {"action": "wait", "state": "WATCH"}

    return strategy


def make_wall_grab_strategy(wall_min_amount: float = 1000) -> Callable:
    """
    GRAB_WALL 策略工廠

    策略邏輯：
    - 當 ask wall 金額 > wall_min_amount：掛在 ask rate × 0.98
    - 否則：觀望

    參數：
        wall_min_amount: 最小 wall 金額門檻

    回傳：
        策略函數
    """
    def strategy(candle: Candle, funding, portfolio: dict, state: dict) -> dict:
        if funding is None:
            return {"action": "wait"}

        if funding.top_ask_amount > wall_min_amount:
            rate = funding.top_ask_rate * 0.98
            return {
                "action": "lend",
                "amount": min(150, portfolio["cash"]),
                "rate": rate,
                "period": 2,
                "state": "GRAB_WALL",
                "note": f"ask_wall=${funding.top_ask_amount:.0f}"
            }
        else:
            return {"action": "wait", "state": "WATCH"}

    return strategy


def make_momentum_strategy(vol_threshold: float = 0.20,
                            momentum_window: int = 12) -> Callable:
    """
    動量策略工廠

    策略邏輯：
    - 當 K 線動量（(high-low)/close）> threshold：掛在 FRR × 1.20
    - 否則：觀望

    參數：
        vol_threshold: 動量門檻
        momentum_window: 滾動視窗（目前未使用，保留擴充）

    回傳：
        策略函數
    """
    def strategy(candle: Candle, funding, portfolio: dict, state: dict) -> dict:
        if funding is None:
            return {"action": "wait"}

        momentum = (candle.high - candle.low) / candle.close

        if momentum > vol_threshold:
            rate = funding.frr_daily * 1.20
            return {
                "action": "lend",
                "amount": min(150, portfolio["cash"]),
                "rate": rate,
                "period": 2,
                "state": "MOMENTUM",
                "note": f"momentum={momentum:.4f}"
            }
        else:
            return {"action": "wait", "state": "NEUTRAL"}

    return strategy


# =============================================================================
# 主程式 — 示範回測
# =============================================================================

def main():
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    print()
    print("=" * 60)
    print("  🚀 Funding 策略回測框架")
    print("=" * 60)

    # 從本地資料庫讀取 K 線 + 真實 FRR
    from market_db import MarketDB

    db = MarketDB()
    candles = db.get_ohlcv("BTC/USD", "15m", limit=5000)

    # 讀取真實 FRR 歷史
    frr_raw = db.get_funding(limit=5000)

    # 同步最新的（如果本地沒有）
    if not frr_raw:
        print("📡 同步 FRR 歷史...")
        db.sync_funding_history("fUSD", days=30)
        frr_raw = db.get_funding(limit=5000)

    db.close()

    if candles:
        print(f"\n📥 K 線：{len(candles)} 根（本地）")
    else:
        print("\n📥 從 API 載入 K 線...")
        candles = load_historical_ohlcv("BTC/USD", "15m", since="2026-06-01", until="2026-06-18", limit=5000)

    if not candles:
        print("❌ 無法載入 K 線，退出")
        return

    if frr_raw:
        print(f"📊 Funding：{len(frr_raw)} 筆真實 FRR（本地）")
    else:
        print("⚠️ 無 FRR 數據，使用模擬")

    # 生成 Funding 市場快照
    print("📊 生成 Funding 市場快照...")
    funding_data = simulate_funding_market(candles, frr_snapshots=frr_raw)

    # 執行多策略回測
    strategies = [
        ("FRR_BOTTOM", make_frr_bottom_strategy()),
        ("LADDER", make_ladder_strategy()),
        ("WALL_GRAB", make_wall_grab_strategy()),
        ("MOMENTUM", make_momentum_strategy()),
    ]

    all_results = {}

    for name, strategy_fn in strategies:
        print(f"\n{'─'*50}")
        print(f"  測試策略：{name}")
        print(f"{'─'*50}")

        engine = BacktestEngine(
            candles=candles,
            funding_snapshots=funding_data,
            initial_capital=1000.0,
            name=name
        )

        results = engine.run(strategy_fn, progress=False)
        engine.print_report(results)
        all_results[name] = results

        # 存入資料庫
        try:
            db2 = MarketDB()
            db2.save_backtest_result(name, {
                "initial_capital": results["initial_capital"],
                "final_capital": results["final_capital"],
                "total_return_pct": results["metrics"].total_return_pct,
                "sharpe_ratio": results["metrics"].sharpe_ratio,
                "max_drawdown_pct": results["metrics"].max_drawdown_pct,
                "win_rate": results["metrics"].win_rate,
                "total_trades": results["metrics"].total_trades,
            })
            db2.close()
        except Exception as e:
            print(f"  ⚠️ 無法儲存結果：{e}")

    # 比較總結
    print()
    print("=" * 60)
    print("  📈 策略比較")
    print("=" * 60)
    print(f"  {'策略':<12} {'總報酬':>10} {'夏普率':>8} {'最大回撤':>10} {'交易次':>6}")
    print("  " + "-" * 48)

    for name, results in sorted(all_results.items(),
                                key=lambda x: x[1]["metrics"].total_return_pct,
                                reverse=True):
        m = results["metrics"]
        ret_str = f"{m.total_return_pct:+.2f}%"
        sharpe_str = f"{m.sharpe_ratio:+.2f}"
        dd_str = f"{m.max_drawdown_pct:.1f}%"
        trades_str = f"{m.total_trades}"
        print(f"  {name:<12} {ret_str:>10} {sharpe_str:>8} {dd_str:>10} {trades_str:>6}")

    print("=" * 60)


if __name__ == "__main__":
    main()
