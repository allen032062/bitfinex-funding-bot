# market_db.py
# =============================================================================
# 本地 SQLite 資料庫 — 儲存 K 線、Funding Rate 等市場歷史數據
# =============================================================================
#
# 【模組職責】
#   為回測系統提供本地持久化儲存，避免每次回測都從 Bitfinex API 拉資料
#   同時支援指令列工具對資料庫做查詢與同步
#
# 【資料庫路徑】
#   data/market_data.db（WAL 模式，支援並發讀寫）
#
# 【資料表】
#   1. ohlcv              — K 線（BTC/USD 等交易對，1m/5m/15m/1h 等時間框）
#      PK: (symbol, timeframe, timestamp)
#
#   2. ohlcv_meta         — 每組 (symbol, timeframe) 的最新 K 線時間戳
#      用於增量同步（只拉本地沒有的新資料）
#      PK: (symbol, timeframe)
#
#   3. funding_snapshots  — Funding 市場快照（每 15 分鐘一筆）
#      包含 FRR（Flash Return Rate）、買賣牆、volatility
#      PK: timestamp
#
#   4. backtest_results   — 歷史回測績效記錄
#      供日後橫向比較不同策略版本
#
# 【同步策略】
#   - 增量同步：從 ohlcv_meta 取出最新 timestamp，只往後拉新資料
#   - 若本地完全沒有：回溯 30 天開始拉
#   - API 使用 Bitfinex 官方 public API，有 rate limit 保護
#
# 【指令列用法】
#   python market_db.py init                        # 初始化 + 統計
#   python market_db.py sync --symbol BTC/USD --timeframe 15m
#   python market_db.py sync_frr --days 30
#   python market_db.py stats
#   python market_db.py query --symbol BTC/USD --timeframe 15m
#
# 【利率單位說明】
#   - frr_daily: 日利率（小數形式，如 0.00031205）
#   - frr_annual: 年化（小數形式，如 0.1471 = 14.71%）
#   - 顯示時乘以 100 變成百分比
# =============================================================================

import sqlite3
import json
import time
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional, Tuple
from datetime import datetime

from backtester import Candle, FundingSnapshot

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "data" / "market_data.db"


# =============================================================================
# 資料庫初始化
# =============================================================================

def init_db(conn: sqlite3.Connection):
    """
    初始化資料庫 schema（建立所有資料表與索引）

    參數：
        conn: sqlite3 連線
    """
    cur = conn.cursor()

    # OHLCV K 線表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            timeframe   TEXT NOT NULL,
            timestamp   INTEGER NOT NULL,
            open        REAL NOT NULL,
            high        REAL NOT NULL,
            low         REAL NOT NULL,
            close       REAL NOT NULL,
            volume      REAL NOT NULL,
            created_at  INTEGER DEFAULT (strftime('%s', 'now')),
            UNIQUE(symbol, timeframe, timestamp)
        )
    """)

    # K 線元數據（追蹤最新一根的時間戳，用於增量同步）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv_meta (
            symbol      TEXT NOT NULL,
            timeframe   TEXT NOT NULL,
            latest_ts   INTEGER,
            updated_at  INTEGER DEFAULT (strftime('%s', 'now')),
            PRIMARY KEY (symbol, timeframe)
        )
    """)

    # Funding 市場快照表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS funding_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp       INTEGER NOT NULL UNIQUE,
            frr_daily       REAL,
            frr_annual      REAL,
            top_bid_rate    REAL,
            top_bid_amount  REAL,
            top_ask_rate    REAL,
            top_ask_amount  REAL,
            volatility      REAL,
            btc_price       REAL,
            created_at      INTEGER DEFAULT (strftime('%s', 'now'))
        )
    """)

    # 策略回測結果表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS backtest_results (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_name   TEXT NOT NULL,
            run_at          INTEGER DEFAULT (strftime('%s', 'now')),
            initial_capital REAL,
            final_capital   REAL,
            total_return_pct REAL,
            sharpe_ratio    REAL,
            max_drawdown_pct REAL,
            win_rate        REAL,
            total_trades    INTEGER,
            params          TEXT,
            notes           TEXT
        )
    """)

    # 索引（加速查詢）
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ohlcv_st ON ohlcv(symbol, timeframe, timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_funding_ts ON funding_snapshots(timestamp)")

    conn.commit()


# =============================================================================
# MarketDB 類別
# =============================================================================

class MarketDB:
    """
    本地市場資料庫客戶端

    功能：
    - 儲存/查詢 K 線（OHLCV）
    - 儲存/查詢 Funding 市場快照
    - 從 Bitfinex API 增量同步數據
    - 儲存/查詢回測結果

    使用方式：
        db = MarketDB()
        candles = db.get_ohlcv("BTC/USD", "15m", limit=1000)
        db.close()
    """

    def __init__(self, db_path: str = None):
        """
        初始化資料庫連線

        參數：
            db_path: 資料庫路徑（預設 data/market_data.db）
        """
        self.db_path = db_path or str(DB_PATH)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA busy_timeout = 5000")
        init_db(self.conn)

        logger.info(f"[MarketDB] 開啟資料庫：{self.db_path}")

    def close(self):
        """關閉資料庫連線"""
        self.conn.close()
        logger.info("[MarketDB] 已關閉")

    # =========================================================================
    # OHLCV K 線
    # =========================================================================

    def save_ohlcv(self, candles: List[Candle], symbol: str = "BTC/USD",
                   timeframe: str = "15m", replace: bool = False) -> int:
        """
        批量存入 K 線（UPSERT）

        使用 INSERT OR IGNORE（不覆蓋）或 INSERT OR REPLACE（覆蓋）

        參數：
            candles: Candle 列表
            symbol: 交易對
            timeframe: 時間框
            replace: True = 覆蓋，False = 忽略（預設）

        回傳：
            實際新增的筆數
        """
        if not candles:
            return 0

        cur = self.conn.cursor()
        inserted = 0

        for c in candles:
            try:
                cur.execute("""
                    INSERT OR """ + ("REPLACE" if replace else "IGNORE") + """ INTO ohlcv
                    (symbol, timeframe, timestamp, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (symbol, timeframe, c.timestamp, c.open, c.high, c.low, c.close, c.volume))
                if cur.rowcount > 0:
                    inserted += 1
            except Exception as e:
                logger.debug(f"插入 K 線失敗: {e}")

        # 更新 meta（最新時間戳）
        latest_ts = max(c.timestamp for c in candles)
        cur.execute("""
            INSERT OR REPLACE INTO ohlcv_meta (symbol, timeframe, latest_ts, updated_at)
            VALUES (?, ?, ?, strftime('%s', 'now'))
        """, (symbol, timeframe, latest_ts))

        self.conn.commit()
        logger.info(f"[MarketDB] 存入 K 線 {symbol} {timeframe} × {inserted} 根")
        return inserted

    def get_ohlcv(self, symbol: str = "BTC/USD", timeframe: str = "15m",
                  since: int = None, until: int = None,
                  limit: int = 10000) -> List[Candle]:
        """
        查詢 K 線

        參數：
            symbol: 交易對
            timeframe: 時間框
            since: 起始時間戳（Unix ms）
            until: 結束時間戳（Unix ms）
            limit: 最大筆數

        回傳：
            Candle 列表
        """
        cur = self.conn.cursor()

        query = "SELECT timestamp, open, high, low, close, volume FROM ohlcv WHERE symbol=? AND timeframe=?"
        params: List = [symbol, timeframe]

        if since:
            query += " AND timestamp >= ?"
            params.append(since)
        if until:
            query += " AND timestamp <= ?"
            params.append(until)

        query += " ORDER BY timestamp ASC LIMIT ?"
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()

        return [
            Candle(timestamp=r[0], open=r[1], high=r[2], low=r[3], close=r[4], volume=r[5])
            for r in rows
        ]

    def get_latest_ohlcv_ts(self, symbol: str = "BTC/USD",
                            timeframe: str = "15m") -> Optional[int]:
        """
        取得最新一根 K 線的時間戳（用於增量同步）

        回傳：
            時間戳（Unix ms）或 None
        """
        cur = self.conn.cursor()
        cur.execute(
            "SELECT latest_ts FROM ohlcv_meta WHERE symbol=? AND timeframe=?",
            (symbol, timeframe)
        )
        row = cur.fetchone()
        return row[0] if row else None

    def sync_ohlcv(self, symbol: str = "BTC/USD", timeframe: str = "15m",
                   limit: int = 1000) -> int:
        """
        從 Bitfinex API 增量同步 K 線到本地資料庫

        邏輯：
        - 查詢本地最新時間戳（ohlcv_meta）
        - 若本地完全沒有：回溯 30 天
        - 只往後拉比本地更新的資料

        參數：
            symbol: 交易對
            timeframe: 時間框
            limit: 每次最多拉多少根

        回傳：
            實際同步的新 K 線筆數
        """
        import ccxt

        exchange = ccxt.bitfinex({"enableRateLimit": True})

        latest_ts = self.get_latest_ohlcv_ts(symbol, timeframe)

        # 如果本地完全沒有，從 30 天前開始
        if latest_ts is None:
            since = int((datetime.now().timestamp() - 30 * 86400) * 1000)
        else:
            since = latest_ts + 1

        params = {"sort": 1}
        params["start"] = since

        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit, params=params)
        except Exception as e:
            logger.error(f"[MarketDB] 同步 K 線失敗: {e}")
            return 0

        if not ohlcv:
            logger.info(f"[MarketDB] 無新 K 線需要同步")
            return 0

        candles = [
            Candle(
                timestamp=int(c[0]),
                open=float(c[1]),
                high=float(c[2]),
                low=float(c[3]),
                close=float(c[4]),
                volume=float(c[5])
            )
            for c in ohlcv
        ]

        inserted = self.save_ohlcv(candles, symbol, timeframe)
        logger.info(f"[MarketDB] 同步完成：{inserted} 根新 K 線")
        return inserted

    # =========================================================================
    # Funding Snapshots
    # =========================================================================

    def sync_funding_history(self, symbol: str = "fUSD",
                              days: int = 30) -> int:
        """
        從 Bitfinex public API 同步 Funding FRR 歷史到本地資料庫

        API：https://api-pub.bitfinex.com/v2/funding/stats/{symbol}/hist

        參數：
            symbol: fUSD（USD 借貸）或 fBTC（BTC 借貸）
            days: 回溯天數（若本地完全沒有）

        回傳：
            實際同步的新 FRR 筆數
        """
        import requests
        from typing import List

        url = f"https://api-pub.bitfinex.com/v2/funding/stats/{symbol}/hist"

        # 從本地最新一筆往後拉（增量）
        latest_ts = self.conn.execute(
            "SELECT MAX(timestamp) FROM funding_snapshots WHERE frr_annual > 0"
        ).fetchone()[0]

        if latest_ts is None:
            # 沒有任何記錄，從 N 天前開始
            since_ms = int((datetime.now().timestamp() - days * 86400) * 1000)
        else:
            since_ms = latest_ts + 1

        all_rows = []
        current_ts = since_ms

        while True:
            params = {"limit": 250, "sort": 1}
            if current_ts:
                params["start"] = current_ts

            try:
                resp = requests.get(url, params=params, timeout=15)
                if resp.status_code != 200:
                    logger.error(f"[MarketDB] FRR API 錯誤: {resp.status_code} {resp.text[:100]}")
                    break

                rows = resp.json()
                if not rows or rows[0] is None:
                    break

                all_rows.extend(rows)

                last_ts = rows[-1][0]
                if last_ts >= int(datetime.now().timestamp() * 1000):
                    break

                current_ts = last_ts + 1

                if len(rows) < 250:
                    break
            except Exception as e:
                logger.error(f"[MarketDB] FRR API 請求失敗: {e}")
                break

        if not all_rows:
            logger.info(f"[MarketDB] 無新 FRR 數據")
            return 0

        # 轉換並存入
        snapshots = []
        for row in all_rows:
            ts = int(row[0])
            frr_daily = float(row[3]) if row[3] else 0.0

            snapshots.append(FundingSnapshot(
                timestamp=ts,
                frr_daily=frr_daily,
                frr_annual=frr_daily * 365,
                top_bid_rate=frr_daily * 0.95,
                top_bid_amount=float(row[4]) if row[4] else 0,
                top_ask_rate=frr_daily * 1.05,
                top_ask_amount=float(row[4]) if row[4] else 0,
                volatility=0.0
            ))

        inserted = self.save_funding(snapshots)
        logger.info(f"[MarketDB] FRR 歷史同步完成：{inserted} 筆")
        return inserted

    def save_funding(self, snapshots: List[FundingSnapshot]) -> int:
        """
        存入 Funding 快照

        參數：
            snapshots: FundingSnapshot 列表

        回傳：
            實際新增的筆數
        """
        if not snapshots:
            return 0

        cur = self.conn.cursor()
        inserted = 0

        for s in snapshots:
            try:
                cur.execute("""
                    INSERT OR REPLACE INTO funding_snapshots
                    (timestamp, frr_daily, frr_annual, top_bid_rate, top_bid_amount,
                     top_ask_rate, top_ask_amount, volatility, btc_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (s.timestamp, s.frr_daily, s.frr_annual,
                      s.top_bid_rate, s.top_bid_amount,
                      s.top_ask_rate, s.top_ask_amount, s.volatility, 0))
                if cur.rowcount > 0:
                    inserted += 1
            except Exception as e:
                logger.debug(f"存入 Funding 失敗: {e}")

        self.conn.commit()
        logger.info(f"[MarketDB] 存入 Funding 快照 × {inserted}")
        return inserted

    def get_funding(self, since: int = None, until: int = None,
                    limit: int = 10000) -> List[FundingSnapshot]:
        """
        查詢 Funding 快照

        參數：
            since: 起始時間戳（Unix ms）
            until: 結束時間戳（Unix ms）
            limit: 最大筆數

        回傳：
            FundingSnapshot 列表
        """
        cur = self.conn.cursor()

        query = """SELECT timestamp, frr_daily, frr_annual, top_bid_rate, top_bid_amount,
                   top_ask_rate, top_ask_amount, volatility
                   FROM funding_snapshots WHERE 1=1"""
        params: List = []

        if since:
            query += " AND timestamp >= ?"
            params.append(since)
        if until:
            query += " AND timestamp <= ?"
            params.append(until)

        query += " ORDER BY timestamp ASC LIMIT ?"
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()

        return [
            FundingSnapshot(
                timestamp=r[0],
                frr_daily=r[1],
                frr_annual=r[2],
                top_bid_rate=r[3],
                top_bid_amount=r[4],
                top_ask_rate=r[5],
                top_ask_amount=r[6],
                volatility=r[7]
            )
            for r in rows
        ]

    # =========================================================================
    # 回測結果
    # =========================================================================

    def save_backtest_result(self, strategy_name: str,
                             metrics: dict,
                             params: dict = None,
                             notes: str = ""):
        """
        儲存回測結果到資料庫

        參數：
            strategy_name: 策略名稱
            metrics: 績效指標字典
            params: 策略參數（可選）
            notes: 備註（可選）
        """
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO backtest_results
            (strategy_name, initial_capital, final_capital, total_return_pct,
             sharpe_ratio, max_drawdown_pct, win_rate, total_trades, params, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            strategy_name,
            metrics.get("initial_capital"),
            metrics.get("final_capital"),
            metrics.get("total_return_pct"),
            metrics.get("sharpe_ratio"),
            metrics.get("max_drawdown_pct"),
            metrics.get("win_rate"),
            metrics.get("total_trades"),
            json.dumps(params) if params else None,
            notes
        ))
        self.conn.commit()
        logger.info(f"[MarketDB] 回測結果已儲存：{strategy_name}")

    def get_backtest_history(self, strategy_name: str = None,
                             limit: int = 50) -> List[dict]:
        """
        查詢歷史回測結果

        參數：
            strategy_name: 策略名稱（可選，不填則查全部）
            limit: 最大筆數

        回傳：
            回測結果字典列表
        """
        cur = self.conn.cursor()

        query = "SELECT * FROM backtest_results WHERE 1=1"
        params: List = []

        if strategy_name:
            query += " AND strategy_name = ?"
            params.append(strategy_name)

        query += " ORDER BY run_at DESC LIMIT ?"
        params.append(limit)

        cur.execute(query, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    # =========================================================================
    # 資料統計
    # =========================================================================

    def stats(self) -> dict:
        """
        取得資料庫統計資訊

        回傳：
            包含 ohlcv_records/funding_records/symbol_pairs/earliest_ts/latest_ts/db_path 的字典
        """
        cur = self.conn.cursor()

        cur.execute("SELECT COUNT(*) FROM ohlcv")
        ohlcv_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM funding_snapshots")
        funding_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(DISTINCT symbol || timeframe) FROM ohlcv_meta")
        pairs = cur.fetchone()[0]

        cur.execute("SELECT MIN(timestamp), MAX(timestamp) FROM ohlcv")
        ts_range = cur.fetchone()

        return {
            "ohlcv_records": ohlcv_count,
            "funding_records": funding_count,
            "symbol_pairs": pairs,
            "earliest_ts": ts_range[0],
            "latest_ts": ts_range[1],
            "db_path": self.db_path
        }


# =============================================================================
# 命令列工具
# =============================================================================

def main():
    """MarketDB 命令列工具"""
    import argparse

    logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="MarketDB CLI")
    parser.add_argument("action", choices=["init", "sync", "sync_frr", "stats", "query"])
    parser.add_argument("--symbol", default="BTC/USD")
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--since", default=None)
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()

    db = MarketDB()

    if args.action == "init":
        logger.info("資料庫初始化完成")
        print(f"資料庫路徑：{db.db_path}")
        print(f"統計：{db.stats()}")

    elif args.action == "sync":
        inserted = db.sync_ohlcv(args.symbol, args.timeframe)
        print(f"同步完成：{inserted} 根新 K 線")
        print(f"統計：{db.stats()}")

    elif args.action == "sync_frr":
        inserted = db.sync_funding_history("fUSD", days=args.days)
        print(f"同步完成：{inserted} 筆 FRR 數據")
        s = db.stats()
        print(f"Funding 快照：{s['funding_records']:,} 筆")

    elif args.action == "stats":
        s = db.stats()
        print(f"資料庫：{s['db_path']}")
        print(f"K 線總數：{s['ohlcv_records']:,} 筆")
        print(f"Funding 快照：{s['funding_records']:,} 筆")
        print(f"交易對/周期：{s['symbol_pairs']} 組")
        if s['earliest_ts']:
            from_ts = datetime.fromtimestamp(s['earliest_ts'] / 1000).isoformat()
            to_ts = datetime.fromtimestamp(s['latest_ts'] / 1000).isoformat()
            print(f"資料範圍：{from_ts} → {to_ts}")

    elif args.action == "query":
        candles = db.get_ohlcv(args.symbol, args.timeframe, limit=5)
        print(f"最新 5 根 K 線 {args.symbol} {args.timeframe}：")
        for c in candles:
            ts = datetime.fromtimestamp(c.timestamp / 1000).isoformat()
            print(f"  {ts} O={c.open:.1f} H={c.high:.1f} L={c.low:.1f} C={c.close:.1f}")

        funding = db.get_funding(limit=5)
        if funding:
            print(f"\n最新 {len(funding)} 筆 FRR：")
            for f in funding:
                ts = datetime.fromtimestamp(f.timestamp / 1000).isoformat()
                print(f"  {ts} 日率={f.frr_daily:.8f} ({f.frr_annual*100:.4f}%/年)")

    db.close()


if __name__ == "__main__":
    main()
