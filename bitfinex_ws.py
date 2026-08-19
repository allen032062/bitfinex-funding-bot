# bitfinex_ws.py
# =============================================================================
# Bitfinex Authenticated WebSocket Client v2 - 雙向通道版
# =============================================================================
#
# 【功能說明】
#   連接 Bitfinex Authenticated WebSocket，支援：
#   - 接收：wallet (wu)、Offer (fon/fou/foc)、Credit (fcn/fcu/fcc)
#   - 發送：on (新掛單)、oc (撤單)
#
# 【API 文件】
#   https://docs.bitfinex.com/reference/ws-auth-input-offer-new
#
# 【使用方式】
#   ws = BitfinexWS(api_key, api_secret,
#                   on_wallet=..., on_offer=..., on_credit=...,
#                   on_offer_new_conf=..., on_offer_cancel_conf=...)
#   ws.start()
#   ws.send_offer_new("fUSD", 150, 0.00027, 2)
#   ws.send_offer_cancel("123456")
#   ws.stop()
#
# 【利率單位說明】
#   - rate 參數：日利率（小數形式，如 0.00027 = 0.027%/天）
#   - 顯示給用戶：乘以 100 變成百分比
#   - 年化 = rate × 365（已是 % 形式，如 9.855%）
# =============================================================================

import json
import time
import hmac
import hashlib
import logging
import threading
import asyncio
import traceback
import sys
from pathlib import Path
from collections import defaultdict

logger = logging.getLogger(__name__)

# =============================================================================
# 全域常數
# =============================================================================

# Bitfinex WebSocket API 端點
WS_URI = "wss://api.bitfinex.com/ws/2"

# 錢包事件類型
EVENT_WALLET = "wu"


# =============================================================================
# BitfinexWS 類別
# =============================================================================

class BitfinexWS:
    """
    Bitfinex 雙向 Auth WebSocket 客戶端

    功能：
    - 接收：wallet/offer/credit 事件
    - 發送：on（新掛單）、oc（撤單）

    事件說明：
    - wu：錢包更新（USDT 餘額變動）
    - fon：Funding Offer New（新掛單確認）
    - fou：Funding Offer Update（掛單更新）
    - foc：Funding Offer Cancel（成交/取消）
    - fcn：Funding Credit New（借出成功）
    - fcc：Funding Credit Close（還款完成）
    - fcu：Funding Credit Update（還款更新）
    - fct：Funding Trade（市場成交利率）

    執行緒模型：
    - WS 接收在一個独立執行緒（_thread）中運行
    - 發送（send_offer_new 等）可從任何執行緒呼叫，執行緒安全
    """

    def __init__(self, api_key: str, api_secret: str,
                 on_wallet=None, on_offer=None, on_credit=None,
                 on_offer_new_conf=None, on_offer_cancel_conf=None,
                 on_auth_success=None, on_auth_error=None):
        """
        初始化 BitfinexWS

        參數：
            api_key: Bitfinex API Key
            api_secret: Bitfinex API Secret
            on_wallet: 錢包更新回調 (wtype, currency, balance, available) -> None
            on_offer: Offer 事件回調 (event, offer_id, status, amount, period, rate_pct, ts_created, ts_updated, data) -> None
            on_credit: Credit 事件回調 (event, credit_id, status, amount, period, rate_pct, ts_created, ts_updated, data) -> None
            on_offer_new_conf: 新掛單確認回調 (offer_id, data, ok) -> None
            on_offer_cancel_conf: 撤單確認回調 (offer_ids, ok) -> None
            on_auth_success: Auth 成功回調 () -> None
            on_auth_error: Auth 失敗回調 (error) -> None
        """
        self.api_key = api_key
        self.api_secret = api_secret

        # 回調函式（預設為空函式，避免 None 检查）
        self.on_wallet = on_wallet or (lambda *a, **k: None)
        self.on_offer   = on_offer   or (lambda *a, **k: None)
        self.on_credit  = on_credit  or (lambda *a, **k: None)
        self.on_offer_new_conf    = on_offer_new_conf    or (lambda *a, **k: None)
        self.on_offer_cancel_conf = on_offer_cancel_conf or (lambda *a, **k: None)
        self.on_auth_success = on_auth_success or (lambda: None)
        self.on_auth_error  = on_auth_error  or (lambda e: None)

        # 內部狀態
        self._running  = False              # 運行標誌
        self._thread   = None               # WS 執行緒
        self._ws       = None               # websockets 客戶端實例
        self._loop     = None               # asyncio event loop（本執行緒）
        self._api_loop = None               # WS 執行緒中的 loop（給外部執行緒發送用）
        self._lock     = threading.Lock()    # 發送鎖

        # 待確認指令叢集（cid → (timestamp, callback)）
        self._pending    = {}
        self._pending_lk = threading.Lock()

        # fct 市場成交利率追蹤
        self._trade_lock      = threading.Lock()
        self._last_trade_rate = 0.0    # 年化%（最近一筆成交利率）
        self._last_trade_ts  = 0        # ms timestamp

    # =========================================================================
    # Auth 輔助函式
    # =========================================================================

    def _make_nonce(self) -> str:
        """
        產生唯一的 nonce（用於 HMAC 認證）

        回傳：
            字串形式的毫秒時間戳
        """
        return str(int(time.time() * 1000))

    def _make_auth_payload(self, nonce: str) -> str:
        """
        建立 Auth 簽名 payload

        Bitfinex 要求格式：AUTH{nonce}

        參數：
            nonce: 唯一的 nonce 字串

        回傳：
            用於 HMAC 簽名的 payload 字串
        """
        return f"AUTH{nonce}"

    def _make_auth_sig(self, payload: str) -> str:
        """
        計算 HMAC-SHA384 簽名

        Bitfinex Auth 流程：
        1. 產生 nonce
        2. 建立 payload = "AUTH{nonce}"
        3. 用 api_secret 對 payload 做 HMAC-SHA384
        4. 發送 {event: "auth", apiKey, authSig, authPayload}

        參數：
            payload: 要簽名的字串

        回傳：
            十六進位簽名字串
        """
        return hmac.new(
            self.api_secret.encode(),
            payload.encode(),
            hashlib.sha384
        ).hexdigest()

    # =========================================================================
    # 公開 API：生命周期管理
    # =========================================================================

    def start(self):
        """啟動 WS 連線（在獨立執行緒中運行）"""
        if self._running:
            logger.warning("[BitfinexWS] 已在執行中")
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("[BitfinexWS] 啟動")

    def stop(self):
        """停止 WS 連線"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("[BitfinexWS] 已停止")

    # =========================================================================
    # 公開 API：發送指令
    # =========================================================================

    def send_offer_new(self, symbol: str, amount: float, rate: float, period: int,
                       callback=None) -> str:
        """
        發送「Funding Offer New」（on）指令 - 新掛單

        參數：
            symbol: 交易對（如 "fUSD"）
            amount: 掛單金額（USD）
            rate: 日利率（小數形式，如 0.00027 = 0.027%/天）
            period: 天期（2 或 120 天）
            callback: 確認回調（conf_data）-> None

        回傳：
            cid 字串（client order ID，用於追蹤指令）

        注意：offer_id（Bitfinex 分配的 ID）會在確認後透過 on_offer_new_conf 回調取得
        """
        cid = self._make_nonce()
        cmd = [0, "fon", None, {
            'type': 'LIMIT',
            'symbol': symbol,
            'amount': str(amount),
            'rate': str(rate),
            'period': period
        }]
        self._register_pending(cid, callback)
        self._send(cmd, cid=cid)
        logger.info(f"[BitfinexWS] fon 發送 cid={cid} symbol={symbol} amount={amount} rate={rate} period={period}")
        return cid

    def send_offer_cancel(self, offer_id: str, callback=None) -> str:
        """
        發送「Funding Offer Cancel」（oc）指令 - 撤單

        參數：
            offer_id: 要取消的 offer ID（字串形式）
            callback: 確認回調

        回傳：
            cid 字串
        """
        cid = self._make_nonce()
        cmd = [0, "foc", None, {'id': int(offer_id)}]
        self._register_pending(cid, callback)
        self._send(cmd, cid=cid)
        logger.info(f"[BitfinexWS] foc 發送 cid={cid} offer_id={offer_id}")
        return cid

    def send_offer_cancel_multi(self, offer_ids: list, callback=None) -> str:
        """
        批量撤單：一次取消多個 offer

        參數：
            offer_ids: offer ID 列表
            callback: 確認回調

        回傳：
            cid 字串
        """
        cid = self._make_nonce()
        ids = [[int(oid)] for oid in offer_ids]
        cmd = [0, "oc_multi", None, {'ids': ids}]
        self._register_pending(cid, callback)
        self._send(cmd, cid=cid)
        logger.info(f"[BitfinexWS] oc_multi 發送 cid={cid} count={len(offer_ids)}")
        return cid

    # =========================================================================
    # 內部發送機制
    # =========================================================================

    def _send(self, cmd, cid=None):
        """
        執行緒安全的 WS 發送（供內部指令呼叫）

        機制：
        - asyncio.run_coroutine_threadsafe() 將協同程序排程到 WS loop 執行
        - 確保從任何執行緒呼叫都不會阻塞

        參數：
            cmd: 要發送的 JSON 命令
            cid: 選用的 client order ID

        回傳：
            True = 成功，False = 失敗
        """
        with self._lock:
            if self._ws is None:
                logger.warning("[BitfinexWS] WS 未連線，無法發送")
                return False
            target_loop = self._api_loop
            if target_loop is None:
                logger.warning("[BitfinexWS] 無可用 event loop")
                return False
            try:
                future = asyncio.run_coroutine_threadsafe(
                    self._ws.send(json.dumps(cmd)), target_loop
                )
                future.result(5)  # 最多等 5 秒
                return True
            except Exception as e:
                logger.error(f"[BitfinexWS] 發送失敗: {e}")
                return False

    def _register_pending(self, cid, callback):
        """
        註冊待確認的指令

        參數：
            cid: client order ID
            callback: 確認後的回調函式
        """
        with self._pending_lk:
            self._pending[cid] = (time.time(), callback)

    def _resolve_pending(self, cid, conf_data):
        """
        解析待確認指令並呼叫回調

        參數：
            cid: client order ID
            conf_data: 確認資料
        """
        with self._pending_lk:
            entry = self._pending.pop(cid, None)
        if entry:
            _, callback = entry
            if callback:
                try:
                    callback(conf_data)
                except Exception as e:
                    logger.error(f"[_resolve_pending] callback error: {e}")

    # =========================================================================
    # 主執行緒
    # =========================================================================

    def _run(self):
        """
        WS 執行緒主函式

        建立新的 event loop 並執行 _connect_ws
        """
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._connect_ws(loop))
        except Exception as e:
            logger.error(f"[BitfinexWS] 執行例外: {e}\n{traceback.format_exc()}")

    async def _connect_ws(self, loop):
        """
        異步建立並管理 WS 連線

        流程：
        1. 連接 WS
        2. 發送 auth 認證
        3. 等待認證結果
        4. 訂閱 funding / wallet / fct 頻道
        5. 進入訊息接收迴圈
        6. 斷線時自動重連

        參數：
            loop: asyncio event loop
        """
        import websockets

        # 建立 Auth 認證訊息
        nonce = self._make_nonce()
        auth_payload = self._make_auth_payload(nonce)
        auth_sig = self._make_auth_sig(auth_payload)

        auth_msg = json.dumps({
            "event": "auth",
            "apiKey": self.api_key,
            "authSig": auth_sig,
            "authPayload": auth_payload,
            "calc": 0
        })

        while self._running:
            try:
                async with websockets.connect(WS_URI, ping_interval=30) as ws:
                    self._ws = ws
                    self._loop = asyncio.get_running_loop()
                    logger.info("[BitfinexWS] 已連線，傳送 auth")
                    await ws.send(auth_msg)

                    ok = await self._wait_auth(ws)
                    if not ok:
                        logger.error("[BitfinexWS] Auth 失敗，10秒後重連...")
                        await asyncio.sleep(10)
                        continue

                    # 綁定 loop 給 _send() 使用（從其他執行緒呼叫時需要）
                    self._api_loop = asyncio.get_running_loop()
                    logger.info("[BitfinexWS] 已與 WS loop 綁定")

                    # 訂閱頻道
                    # funding: 掛單事件（fon/fou/foc）
                    await ws.send(json.dumps({"event": "subscribe", "channel": "funding", "symbol": "fUSD"}))
                    # wallet: 錢包更新（wu）
                    await ws.send(json.dumps({"event": "subscribe", "channel": "wallet"}))
                    # fct: 市場成交利率
                    await ws.send(json.dumps({"event": "subscribe", "channel": "fct", "symbol": "fUSD"}))

                    logger.info("[BitfinexWS] 訂閱完成，開始接收事件")

                    # 訊息接收迴圈
                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw)
                            self._handle_message(msg)
                        except json.JSONDecodeError:
                            pass

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"[BitfinexWS] 連線中斷: {e}，5秒後重新連線...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"[BitfinexWS] 錯誤: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(5)
            finally:
                self._api_loop = None

    async def _wait_auth(self, ws):
        """
        等待並處理 Auth 認證結果

        參數：
            ws: websockets 客戶端

        回傳：
            True = 認證成功，False = 認證失敗或超時
        """
        try:
            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=15)
                data = json.loads(msg)
                if isinstance(data, dict):
                    if data.get("event") == "auth":
                        if data.get("status") == "OK":
                            logger.info("[BitfinexWS] Auth 成功")
                            self.on_auth_success()
                            return True
                        else:
                            err = data.get("error", str(data))
                            logger.error(f"[BitfinexWS] Auth 失敗: {err}")
                            self.on_auth_error(err)
                            return False
                    elif data.get("event") == "error":
                        logger.error(f"[BitfinexWS] WebSocket error: {data}")
                        self.on_auth_error(str(data))
                        return False
        except asyncio.TimeoutError:
            logger.error("[BitfinexWS] Auth 回應超時")
            return False

    # =========================================================================
    # 訊息處理
    # =========================================================================

    def _handle_message(self, msg):
        """
        統一的訊息分發函式

        Bitfinex WS 訊息格式：
        - 系統訊息：{"event": "...", ...}
        - 頻道訊息：[chanId, channel, data]

        頻道說明：
        - "n": 私人通知（指令確認）
        - "wu": 錢包更新
        - "fon/fou/foc": Funding Offer 事件
        - "fcn/fcu/fcc": Funding Credit 事件
        - "fct": Funding Trade（市場成交利率）

        參數：
            msg: 解析後的 JSON 訊息
        """
        try:
            # chanId = 0 的原始訊息優先裸印（避免解析錯誤吞掉）
            if isinstance(msg, list) and len(msg) > 0:
                chan_id = msg[0]
                if chan_id == 0:
                    logger.info(f"[RAW CHAN=0] {str(msg)[:500]}")

            if not isinstance(msg, list) or len(msg) < 2:
                return

            channel = msg[1] if len(msg) > 1 else None
            data    = msg[2] if len(msg) > 2 else None

            # 重要頻道記錄 RAW log
            if channel in ["n", "fon", "foc", "fon", "fou", "foc", "fcn", "fcu", "fcc"]:
                logger.info(f"[BitfinexWS] RAW [{channel}]: {str(msg)[:400]}")

            if channel == "n":
                # 私人通知（指令確認）
                self._handle_notification(data)
            elif channel == "fon":
                # Funding offer confirmation
                # Bitfinex sends conf as [offer_id, ...] directly
                if isinstance(data, list) and len(data) >= 2:
                    offer_id = str(data[0]) if data[0] else None
                    logger.info(f"[BitfinexWS] fon conf OK: offer_id={offer_id}")
                    self.on_offer_new_conf(offer_id, data, True)
                else:
                    logger.warning(f"[BitfinexWS] fon conf unexpected format: {data}")
                self._handle_offer(channel, data)
            elif channel == "fou":
                # Funding offer update
                self._handle_offer(channel, data)
            elif channel == "foc":
                # Funding offer cancel confirmation
                self._handle_conf(channel, data)
                self._handle_offer(channel, data)
            elif channel in ["fcn", "fcu", "fcc"]:
                self._handle_credit(channel, data)
            elif channel == "wu":
                # Wallet update
                self._handle_wallet(data)
            elif channel == "fct":
                # Funding Trade（市場成交利率）
                self._handle_fct(data)
        except Exception as e:
            logger.error(f"[_handle_message] {e}")

    def _handle_conf(self, channel, data):
        """
        處理 on/oc 的伺服器確認

        Bitfinex 指令確認格式（conf 頻道）：
          [chanId, "on",  [status_code, null, offer_info]]
          [chanId, "oc",  [status_code, null, [offer_id, ...]]]

        status 0 = 成功

        參數：
            channel: 頻道名稱
            data: 確認資料
        """
        try:
            if not isinstance(data, list) or len(data) < 2:
                return
            status = data[0]
            ok = (status == 0)

            if channel == "fon":
                if len(data) >= 3 and isinstance(data[2], list) and len(data[2]) > 0:
                    offer_id = str(data[2][0])
                    logger.info(f"[BitfinexWS] fon conf OK: offer_id={offer_id} status={status}")
                    self.on_offer_new_conf(offer_id, data[2], ok)
                else:
                    logger.warning(f"[BitfinexWS] fon conf status={status} data={data}")
                    self.on_offer_new_conf(None, data, ok)

            elif channel == "foc":
                if len(data) >= 3 and isinstance(data[2], list):
                    logger.info(f"[BitfinexWS] foc conf OK: {data[2]} status={status}")
                    self.on_offer_cancel_conf(data[2], ok)
                else:
                    logger.warning(f"[BitfinexWS] foc conf status={status} data={data}")
                    self.on_offer_cancel_conf([], ok)
        except Exception as e:
            logger.error(f"[_handle_conf] {e}")

    # =========================================================================
    # 事件處理器
    # =========================================================================

    def _handle_wallet(self, data):
        """
        處理錢包更新事件（wu）

        data 格式：
          [wtype, currency, balance, available, ...]

        參數：
            data: 錢包資料列表
        """
        try:
            if not isinstance(data, list) or len(data) < 4:
                return
            wtype, currency, balance, available = data[0], data[1], data[2], data[3]
            logger.info(f"[BitfinexWS] 錢包更新: {wtype} {currency} {balance}")
            self.on_wallet(wtype, currency, balance, available)
        except Exception as e:
            logger.error(f"[_handle_wallet] {e}")

    def _handle_offer(self, event, data):
        """
        處理 Funding Offer 事件

        data 格式：
          [offer_id, cid, mts_created, mts_updated, amount, ...,
           status, period, rate, ...]

        索引對照：
          [0] = offer_id
          [2] = mts_created (ms)
          [4] = amount
          [9] = mts_updated (ms)
          [10] = status
          [14] = rate（小數形式）
          [15] = period

        參數：
            event: 事件名稱（fon/fou/foc）
            data: offer 資料列表
        """
        try:
            if not isinstance(data, list) or len(data) < 3:
                return
            offer_id   = str(data[0]) if data[0] else ""
            status     = str(data[10]) if len(data) > 10 else ""
            amount     = float(data[4]) if len(data) > 4 and data[4] else 0.0
            rate       = float(data[14]) if len(data) > 14 and data[14] else 0.0
            rate_pct   = rate * 100                      # 小數 → 百分比
            period     = int(data[15]) if len(data) > 15 and data[15] else 0
            ts_created = int(data[2]) if len(data) > 2 and data[2] else 0
            ts_updated = int(data[9]) if len(data) > 9 and data[9] else ts_created

            logger.info(f"[BitfinexWS] offer id={offer_id} event={event} status={status} "
                        f"amount={amount} rate={rate_pct}% period={period}d")
            self.on_offer(event, offer_id, status, amount, period, rate_pct, ts_created, ts_updated, data)
        except Exception as e:
            logger.error(f"[_handle_offer] {e}")

    def _handle_credit(self, event, data):
        """
        處理 Funding Credit 事件

        data 格式：
          [credit_id, ...status, amount, daily_rate, period, ...]

        索引對照：
          [0] = credit_id
          [5] = amount（本金）
          [7] = status
          [11] = rate（小數形式）
          [12] = period
          [13] = mts_created
          [14] = mts_updated

        參數：
            event: 事件名稱（fcn/fcu/fcc）
            data: credit 資料列表
        """
        try:
            # fcc 格式特殊性：data[0] = 'fcc' 字串，data[1] = 真正的資料
            if isinstance(data, list) and len(data) == 2 and data[0] == 'fcc':
                data = data[1]
            if not isinstance(data, list) or len(data) < 3:
                return
            credit_id  = str(data[0]) if data[0] else ""
            status     = str(data[7]) if len(data) > 7 else ""
            amount     = float(data[5]) if len(data) > 5 and data[5] else 0.0
            rate       = float(data[11]) if len(data) > 11 and data[11] else 0.0
            rate_pct   = rate * 100                          # 小數 → 百分比
            period     = int(data[12]) if len(data) > 12 and data[12] else 0
            ts_created = int(data[13]) if len(data) > 13 and data[13] else 0
            ts_updated = int(data[14]) if len(data) > 14 and data[14] else ts_created

            logger.info(f"[BitfinexWS] credit id={credit_id} event={event} status={status} "
                        f"amount={amount} rate={rate_pct}% period={period}d")
            self.on_credit(event, credit_id, status, amount, period, rate_pct, ts_created, ts_updated, data)
        except Exception as e:
            logger.error(f"[_handle_credit] {e}")

    # =========================================================================
    # fct 市場成交利率處理
    # =========================================================================

    def _handle_fct(self, data):
        """
        處理 Funding Trade（fct）事件：市場成交利率

        用於自動掛單策略：當市場成交利率高時下修第一筆掛單

        data 格式（list）：
          [mts, offer_id, amount, rate_period, rate_pct, hidden, postonly, ...]

        - mts: millisecond timestamp
        - rate_pct: 年化利率（直接是 %，如 11.76）

        參數：
            data: fct 資料列表
        """
        try:
            if not isinstance(data, list) or len(data) < 5:
                return
            ts       = int(data[0]) if data[0] else 0
            amount   = float(data[2]) if data[2] else 0.0
            rate_pct = float(data[4]) if data[4] else 0.0  # 年化%

            if rate_pct <= 0:
                return

            with self._trade_lock:
                self._last_trade_rate = rate_pct
                self._last_trade_ts   = ts

            logger.info(f"[fct] 市場成交: rate={rate_pct}% amount={amount:.2f} ts={ts}")
        except Exception as e:
            logger.error(f"[_handle_fct] {e}")

    def get_last_trade_rate(self):
        """
        取得最近一筆市場成交利率（執行緒安全）

        回傳：
            (rate_pct, ts_ms)
            - rate_pct: 年化利率（%）
            - ts_ms: 時間戳（毫秒）
        """
        with self._trade_lock:
            return self._last_trade_rate, self._last_trade_ts

    # =========================================================================
    # 私人通知處理
    # =========================================================================

    def _handle_notification(self, data):
        """
        處理私人通知（chanId=0 回傳）

        Bitfinex 私人通知有兩種格式：

        格式1（Conf 風）：
          msg = [chanId, "n", [notify_type, notify_id, timestamp, msg_info, extra..., status, message]]
          例如：[0, 'n', ['on-req', 1778962818415, null, null, [...], None, 'ERROR', 'action: disabled']]

        格式2（舊版直接）：
          msg = [notify_type, notify_id, timestamp, msg_info]
          msg_info = [code, text, ...]
          code < 0 = 成功, code > 0 = 錯誤

        參數：
            data: 通知資料
        """
        try:
            if not isinstance(data, list) or len(data) < 2:
                return

            # 格式1：data[1] 是 'on-req' / 'oc-req' 之類
            if isinstance(data[1], str) and data[1].endswith('-req'):
                notify_type = data[0]   # 'on-req' or 'oc-req'
                notify_id   = data[1]
                status_str  = data[6] if len(data) > 6 else None
                error_text  = data[7] if len(data) > 7 else str(data)

                logger.info(f"[BitfinexWS] NOTIFICATION conf-format: type={notify_type} id={notify_id} status={status_str} msg={error_text}")

                if status_str == 'ERROR':
                    logger.warning(f"[BitfinexWS] {notify_type} FAILED: {error_text}")
                    if notify_type == 'on-req':
                        self.on_offer_new_conf(None, data, False)
                    elif notify_type == 'oc-req':
                        self.on_offer_cancel_conf([], False)
                    return

                if status_str == 'SUCCESS':
                    logger.info(f"[BitfinexWS] {notify_type} SUCCESS")
                    # 從 data[4][0] 取出真正的 offer_id
                    offer_id = None
                    if len(data) > 4 and isinstance(data[4], list) and len(data[4]) > 0 and data[4][0]:
                        offer_id = str(data[4][0])
                        logger.info(f"[BitfinexWS] {notify_type} SUCCESS: offer_id={offer_id}")
                    if notify_type in ('on-req', 'fon-req'):
                        self.on_offer_new_conf(offer_id, data, True)
                    elif notify_type in ('oc-req', 'foc-req'):
                        self.on_offer_cancel_conf([], True)
                    return

                return

            # 格式2：data[0] 是 notify_type 字串
            if not isinstance(data[0], str):
                logger.info(f"[BitfinexWS] n unknown format: {str(data)[:200]}")
                return

            notify_type = data[0]
            notify_id   = data[1]
            timestamp   = data[2]
            msg_info    = data[3] if len(data) > 3 else None

            logger.info(f"[BitfinexWS] NOTIFICATION: type={notify_type} id={notify_id} msg={msg_info}")

            if not isinstance(msg_info, list) or len(msg_info) < 2:
                return

            code = msg_info[0]
            text = msg_info[1]

            if code == 0:
                if notify_type in ("on", "fon"):
                    offer_id = str(msg_info[1]) if msg_info[1] else None
                    logger.info(f"[BitfinexWS] {notify_type} SUCCESS: offer_id={offer_id}")
                    self.on_offer_new_conf(offer_id, data, True)
                elif notify_type in ("oc", "foc"):
                    self.on_offer_cancel_conf([], True)
            else:
                logger.warning(f"[BitfinexWS] {notify_type} FAILED: code={code} text={text}")
                if notify_type == "on":
                    self.on_offer_new_conf(None, data, False)
                elif notify_type in ("oc", "foc"):
                    self.on_offer_cancel_conf([], False)

        except Exception as e:
            logger.error(f"[_handle_notification] {e}")


# =============================================================================
# 測試
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    import yaml
    cfg_path = Path(__file__).parent / "config" / "settings.yaml"
    with open(cfg_path) as f:
        cfg = yaml.safe_load(f)["api"]

    result = {"done": False}

    def on_conf(offer_id, data, ok):
        print(f"✅ on conf: offer_id={offer_id} ok={ok} data={data}")
        result["done"] = True

    ws = BitfinexWS(
        api_key=cfg["key"],
        api_secret=cfg["secret"],
        on_auth_success=lambda: print("✅ Auth 成功"),
        on_auth_error=lambda e: print(f"❌ Auth 失敗: {e}"),
        on_offer_new_conf=on_conf,
    )
    ws.start()

    # 等 auth 完成
    time.sleep(3)

    # 發送 on（小額 10 USD）
    print(">>> 發送 on: fUSD 10USD rate=0.00027 period=2")
    ws.send_offer_new("fUSD", 10, 0.00027, 2)

    # 等 conf
    for _ in range(20):
        if result["done"]:
            break
        time.sleep(0.5)

    if not result["done"]:
        print("❌ on conf 未收到，等待逾時")

    ws.stop()
    print("測試結束")
