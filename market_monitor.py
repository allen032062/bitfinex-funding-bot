# market_monitor.py
# =============================================================================
# 市場數據監控模組（Phase 1）
# =============================================================================
#
# 【功能說明】
#   透過 Bitfinex WebSocket 即時監控市場數據：
#   - BTC 價格（1 分鐘 K 線）
#   - 市場波動率（根據 BTC 價格變化計算）
#   - FRR（Flash Return Rate，市場平均利率）
#   - 訂單簿牆（top wall：最大量的 bid/ask）
#
# 【資料來源】
#   - Bitfinex WebSocket API v2: wss://api-pub.bitfinex.com/ws/2
#   - 訂閱頻道：candles (1m tBTCUSD) + book (fUSD)
#   - FRR API：https://api-pub.bitfinex.com/v2/ticker/fUSD
#
# 【利率單位說明】
#   - frr_rate: 日利率（小數形式，如 0.00031205）
#   - frr_annual: 年化利率（百分比，如 14.71）
#   - top_wall_rate: 牆單利率（小數形式）
# =============================================================================

import json
import time
import math
import logging
import asyncio
import requests
from dataclasses import dataclass, field
from typing import Dict

logger = logging.getLogger(__name__)

# =============================================================================
# MarketSnapshot：市場快照 dataclass
# =============================================================================

@dataclass
class MarketSnapshot:
    """
    市場快照：某一時刻的市場數據集合

    屬性：
        volatility: 波動率（年化標準差）
        btc_price: 比特幣價格（USD）
        frr_rate: FRR 日利率（小數）
        frr_annual: FRR 年化利率（%）
        top_wall_rate: 訂單簿最大牆的利率（小數）
        top_wall_amount: 訂單簿最大牆的金額（USD）
        top_wall_side: 最大牆的方向（"bid" 或 "ask"）
        updated_at: 快照時間戳
    """
    volatility: float = 0.0
    btc_price: float = 0.0
    frr_rate: float = 0.0       # 日利率（小數）
    frr_annual: float = 0.0     # 年化 %（如 14.71）
    top_wall_rate: float = 0.0  # 小數形式
    top_wall_amount: float = 0.0
    top_wall_side: str = ""
    updated_at: float = field(default_factory=time.time)


# =============================================================================
# OrderBookSide：訂單簿單側（bid 或 ask）
# =============================================================================

class OrderBookSide:
    """
    訂單簿單側（買方或賣方）的價格層級管理

    用於追蹤每個價位的掛單金額，並快速找到最大牆（max wall）

    內部以 Dict[rate, amount] 儲存，支援快照更新和增量更新
    """

    def __init__(self):
        self.rates: Dict[float, float] = {}  # {rate: amount}

    def apply_snapshot(self, entries):
        """
        套用全量快照（初始化或重置時使用）

        參數：
            entries: Bitfinex book snapshot 條目列表
            格式：[rate, period, ... , amount]
        """
        self.rates.clear()
        for entry in entries:
            if len(entry) >= 4:
                r = float(entry[0])
                a = float(entry[3])
                if a != 0:
                    self.rates[r] = a

    def apply_delta(self, entry):
        """
        套用增量更新（訂單簿變化時使用）

        格式：[rate, period, count, amount]
        - count = 0 表示刪除該價位
        - amount > 0 表示新增或更新
        - amount < 0 表示減少（但 Bitfinex 通常直接刪除後重發）

        參數：
            entry: 增量條目
        """
        if len(entry) < 4:
            return
        r = float(entry[0])
        c = int(entry[2])
        a = float(entry[3])

        if c == 0:
            # count = 0：刪除該價位
            self.rates.pop(r, None)
        else:
            if a <= 0:
                # 減少：先疊加再看是否歸零
                if r in self.rates:
                    self.rates[r] += a
                    if self.rates[r] <= 0:
                        self.rates.pop(r, None)
            else:
                self.rates[r] = a

    def max_wall(self):
        """
        找出最大牆（最大掛單量的價位）

        回傳：
            (wall_rate, wall_amount)：最大牆的利率和金額
        """
        if not self.rates:
            return 0.0, 0.0
        wall_r = max(self.rates, key=lambda r: self.rates[r])
        return wall_r, self.rates[wall_r]


# =============================================================================
# VolTracker：波動率追蹤器
# =============================================================================

class VolTracker:
    """
    波動率追蹤器（根據收盤價對數收益率計算）

    算法：對數收益率的標準差 × √(一年分鐘數)
    - window: 滾動視窗大小（通常 20 筆）
    - 波動率為年化值（0.1 = 10%）
    """

    def __init__(self, window=20):
        """
        參數：
            window: 滾動視窗大小（預設 20 筆收盤價）
        """
        self.window = window
        self.closes = []  # 收盤價列表

    def update(self, close_price):
        """
        放入新的收盤價，並計算波動率

        參數：
            close_price: 新的收盤價

        回傳：
            年化波動率（小數形式）或 None（數據不足時）
        """
        if close_price <= 0:
            return None
        self.closes.append(close_price)
        if len(self.closes) > self.window:
            self.closes.pop(0)
        if len(self.closes) < 2:
            return None
        try:
            # 對數收益率
            returns = [math.log(self.closes[i] / self.closes[i - 1]) for i in range(1, len(self.closes))]
            mean = sum(returns) / len(returns)
            variance = sum((r - mean) ** 2 for r in returns) / len(returns)
            # 年化：標準差 × √(一年分鐘數 525600)
            return math.sqrt(variance) * math.sqrt(525600)
        except:
            return None


# =============================================================================
# MarketMonitor：市場監控主類別
# =============================================================================

class MarketMonitor:
    """
    市場監控器

    功能：
    1. 訂閱 Bitfinex WebSocket 取得 BTC 價格和訂單簿
    2. 定期從 REST API 取得 FRR
    3. 計算市場波動率
    4. 維護一個 MarketSnapshot 供外部查詢

    使用方式：
        mon = MarketMonitor()
        mon.start()
        snap = mon.get_snapshot()  # 取得當前市場快照
    """

    # Bitfinex WebSocket 訂閱符號
    CANDLE_SYMBOL = "trade:1m:tBTCUSD"  # BTC 1 分鐘 K 線
    BOOK_SYMBOL = "fUSD"               # Funding USD 訂單簿

    def __init__(self):
        self._vol = VolTracker(window=20)      # 波動率追蹤器
        self._btc_price = 0.0                 # BTC 最新價格
        self._frr = 0.0                       # FRR 日利率（小數）
        self._frr_annual = 0.0                # FRR 年化（%）
        self._bids = OrderBookSide()          # 買方訂單簿
        self._asks = OrderBookSide()          # 賣方訂單簿
        self._latest = MarketSnapshot()        # 最新市場快照
        self._running = False                  # 運行標誌
        self._candle_chan = None             # K 線頻道 ID
        self._book_chan = None               # 訂單簿頻道 ID

    def start(self):
        """啟動市場監控（在獨立執行緒中運行）"""
        if self._running:
            return
        self._running = True
        import threading
        self._thread = threading.Thread(target=self._run_thread, daemon=True)
        self._thread.start()
        logger.info("[MarketMonitor] 啟動")

    def stop(self):
        """停止市場監控"""
        self._running = False
        logger.info("[MarketMonitor] 已停止")

    def _run_thread(self):
        """執行緒主函式"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._connect_ws())
        except Exception as e:
            logger.error(f"[MarketMonitor] exception: {e}")

    def get_snapshot(self):
        """
        取得當前市場快照

        回傳：
            MarketSnapshot 物件（包含所有市場數據）
        """
        return self._latest

    # =========================================================================
    # WebSocket 連線管理
    # =========================================================================

    async def _connect_ws(self):
        """
        異步建立並管理 WebSocket 連線

        流程：
        1. 連接 Bitfinex WebSocket
        2. 訂閱 K 線和訂單簿頻道
        3. 立即更新 FRR
        4. 進入訊息接收迴圈
        5. 斷線時自動重連
        """
        import websockets
        uri = "wss://api-pub.bitfinex.com/ws/2"

        while self._running:
            try:
                async with websockets.connect(uri, ping_interval=30) as ws:
                    # 訂閱 BTC 1 分鐘 K 線
                    await ws.send(json.dumps({"event": "subscribe", "channel": "candles", "key": self.CANDLE_SYMBOL}))
                    # 訂閱 Funding USD 訂單簿（P0 精度，25 層）
                    await ws.send(json.dumps({"event": "subscribe", "channel": "book", "symbol": self.BOOK_SYMBOL, "prec": "P0", "len": "25"}))

                    logger.info("[MarketMonitor] 已連線，訂閱 candles + book")

                    # 立即取得 FRR
                    self._update_frr()

                    # 訊息接收迴圈
                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw)
                            self._process(msg)
                        except:
                            pass
            except Exception as e:
                logger.error(f"[MarketMonitor] ws error: {e}")
                await asyncio.sleep(5)

    def _update_frr(self):
        """
        從 Bitfinex REST API 取得官方 FRR

        API：https://api-pub.bitfinex.com/v2/ticker/fUSD
        回傳：[FRR, ...]（FRR 已是日利率形式）
        """
        try:
            r = requests.get("https://api-pub.bitfinex.com/v2/ticker/fUSD", timeout=5)
            data = r.json()
            frr_daily = float(data[0])  # data[0] = FRR 日利率（小數）
            self._frr = frr_daily
            self._frr_annual = frr_daily * 365 * 100  # 年化（存為 %，如 14.71）
            logger.info(f"[Market] FRR={self._frr:.6f} ({frr_daily*100:.4f}%/day = {self._frr_annual:.2f}%/年)")
        except Exception as e:
            logger.warning(f"[Market] FRR error: {e}")

    # =========================================================================
    # 訊息處理
    # =========================================================================

    def _process(self, msg):
        """
        統一的訊息分發函式

        格式：[chanId, channel, data]
        """
        if isinstance(msg, dict):
            if msg.get('event') == 'subscribed':
                ch = msg.get('channel')
                if ch == 'candles':
                    self._candle_chan = msg.get('chanId')
                elif ch == 'book':
                    self._book_chan = msg.get('chanId')
                logger.info(f"[MarketMonitor] subscribed: {ch}")
            return

        if len(msg) < 2:
            return

        cid = msg[0]
        data = msg[1]

        if not isinstance(data, list) or len(data) == 0:
            return
        if isinstance(data[0], str):
            return  # heartbeat，忽略

        if cid == self._book_chan:
            self._process_orderbook(data)
        elif cid == self._candle_chan:
            self._process_candle(data)

    def _process_orderbook(self, data):
        """
        處理訂單簿訊息

        格式：
        - 快照：[bid_snapshot, ask_snapshot]
        - 增量：[rate, period, count, amount]
        """
        if isinstance(data[0], list):
            # 快照（全量）
            self._bids.apply_snapshot(data[0] if isinstance(data[0], list) else [])
            self._asks.apply_snapshot(data[1] if len(data) > 1 and isinstance(data[1], list) else [])
            logger.info(f"[Market] OrderBook: bids={len(self._bids.rates)}, asks={len(self._asks.rates)}")
        else:
            # 增量
            a = float(data[3]) if len(data) >= 4 else 0
            if a < 0:
                self._bids.apply_delta(data)
            else:
                self._asks.apply_delta(data)

        # 找出最大牆並更新快照
        bid_r, bid_a = self._bids.max_wall()
        ask_r, ask_a = self._asks.max_wall()
        if bid_a > ask_a:
            self._latest.top_wall_rate = bid_r
            self._latest.top_wall_amount = bid_a
            self._latest.top_wall_side = "bid"
        else:
            self._latest.top_wall_rate = ask_r
            self._latest.top_wall_amount = ask_a
            self._latest.top_wall_side = "ask"

    def _process_candle(self, data):
        """
        處理 K 線訊息

        K 線格式：[mts, open, close, high, low, ...]
        這裡只用收盤價（close）計算波動率
        """
        candle = data[-1] if isinstance(data[0], list) else data
        if isinstance(candle, list) and len(candle) >= 3:
            close = float(candle[2])
            if close < 1000:
                return
            self._btc_price = close

            vol = self._vol.update(close)
            if vol is not None:
                self._latest.volatility = vol
                self._latest.btc_price = close
                self._latest.frr_rate = self._frr
                self._latest.frr_annual = self._frr_annual
                self._latest.updated_at = time.time()
                logger.info(f"[Market] BTC={close:.0f} vol={vol:.4f}")


# =============================================================================
# 測試主程式
# =============================================================================

def main():
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    mon = MarketMonitor()
    mon.start()

    try:
        while True:
            time.sleep(15)
            snap = mon.get_snapshot()
            print(f"\n{'='*50}")
            print(f"  BTC: ${snap.btc_price:.0f}")
            print(f"  Vol: {snap.volatility*100:.1f}%")
            print(f"  FRR: {snap.frr_rate:.6f} ({snap.frr_rate*100:.4f}%/day = {snap.frr_annual:.2f}%/年)")
            print(f"  Wall: {snap.top_wall_rate*100:.4f}% ({snap.top_wall_amount:.0f})")
    except KeyboardInterrupt:
        mon.stop()


if __name__ == "__main__":
    main()
