"""
Bitfinex Authenticated WebSocket Client v2 - 雙向通道版
接收：錢包 (wu)、Offer (fon/fou/foc)、Credit (fcn/fcu/fcc)
發送：on (Offer New)、oc (Offer Cancel)

API 文件：https://docs.bitfinex.com/reference/ws-auth-input-offer-new
"""

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

# ===== 常數 =====
WS_URI = "wss://api.bitfinex.com/ws/2"
EVENT_WALLET = "wu"


class BitfinexWS:
    """
    Bitfinex 雙向 Auth WebSocket：
    - 接收：wallet/offer/credit 事件
    - 發送：on (掛單)、oc (撤單)

    使用方式：
        ws = BitfinexWS(api_key, api_secret,
                        on_wallet=..., on_offer=..., on_credit=...,
                        on_offer_new_conf=..., on_offer_cancel_conf=...)
        ws.start()
        ws.send_offer_new("fUSD", 150, 0.00027, 2)
        ws.send_offer_cancel("123456")
        ws.stop()
    """

    def __init__(self, api_key: str, api_secret: str,
                 on_wallet=None, on_offer=None, on_credit=None,
                 on_offer_new_conf=None, on_offer_cancel_conf=None,
                 on_auth_success=None, on_auth_error=None):
        self.api_key = api_key
        self.api_secret = api_secret

        # callbacks
        self.on_wallet = on_wallet or (lambda *a, **k: None)
        self.on_offer   = on_offer   or (lambda *a, **k: None)
        self.on_credit  = on_credit  or (lambda *a, **k: None)
        self.on_offer_new_conf    = on_offer_new_conf    or (lambda *a, **k: None)
        self.on_offer_cancel_conf = on_offer_cancel_conf or (lambda *a, **k: None)
        self.on_auth_success = on_auth_success or (lambda: None)
        self.on_auth_error  = on_auth_error  or (lambda e: None)

        # 內部狀態
        self._running  = False
        self._thread   = None
        self._ws       = None
        self._loop     = None       # asyncio event loop
        self._api_loop = None       # WS 執行緒中的 loop（給外部執行緒發送用）
        self._lock     = threading.Lock()

        # 待確認的指令（cid → callback）
        self._pending    = {}
        self._pending_lk = threading.Lock()

    # ── Auth helpers ───────────────────────────────────────────────────

    def _make_nonce(self) -> str:
        return str(int(time.time() * 1000))

    def _make_auth_payload(self, nonce: str) -> str:
        return f"AUTH{nonce}"

    def _make_auth_sig(self, payload: str) -> str:
        return hmac.new(
            self.api_secret.encode(),
            payload.encode(),
            hashlib.sha384
        ).hexdigest()

    # ── 公開 API：生命周期 ─────────────────────────────────────────────

    def start(self):
        if self._running:
            logger.warning("[BitfinexWS] 已在執行中")
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("[BitfinexWS] 啟動")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("[BitfinexWS] 已停止")

    # ── 公開 API：發送指令 ─────────────────────────────────────────────

    def send_offer_new(self, symbol: str, amount: float, rate: float, period: int,
                       callback=None) -> str:
        """
        發送「Funding Offer New」（on）
        Args:
            symbol:   "fUSD"
            amount:   掛單金額
            rate:     日利率（小數，如 0.00027）
            period:   天期（2 或 120）
            callback: (conf_data) -> None
        Returns:
            cid 字串
        """
        cid = self._make_nonce()
        cmd = [0, "on", [
            symbol,
            str(amount),
            str(rate),
            period,
            "LIMIT"
        ]]
        self._register_pending(cid, callback)
        self._send(cmd, cid=cid)
        logger.info(f"[BitfinexWS] on 發送 cid={cid} symbol={symbol} amount={amount} rate={rate} period={period}")
        return cid

    def send_offer_cancel(self, offer_id: str, callback=None) -> str:
        """發送「Funding Offer Cancel」（oc）"""
        cid = self._make_nonce()
        cmd = [0, "oc", [int(offer_id)]]
        self._register_pending(cid, callback)
        self._send(cmd, cid=cid)
        logger.info(f"[BitfinexWS] oc 發送 cid={cid} offer_id={offer_id}")
        return cid

    def send_offer_cancel_multi(self, offer_ids: list, callback=None) -> str:
        """批量撤單：oc_multi"""
        cid = self._make_nonce()
        ids = [[int(oid)] for oid in offer_ids]
        cmd = [0, "oc_multi", [ids]]
        self._register_pending(cid, callback)
        self._send(cmd, cid=cid)
        logger.info(f"[BitfinexWS] oc_multi 發送 cid={cid} count={len(offer_ids)}")
        return cid

    # ── 內部發送 ───────────────────────────────────────────────────────

    def _send(self, cmd, cid=None):
        """執行緒安全的 WS 發送（供內部指令呼叫）"""
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
                future.result(5)
                return True
            except Exception as e:
                logger.error(f"[BitfinexWS] 發送失敗: {e}")
                return False

    def _register_pending(self, cid, callback):
        with self._pending_lk:
            self._pending[cid] = (time.time(), callback)

    def _resolve_pending(self, cid, conf_data):
        with self._pending_lk:
            entry = self._pending.pop(cid, None)
        if entry:
            _, callback = entry
            if callback:
                try:
                    callback(conf_data)
                except Exception as e:
                    logger.error(f"[_resolve_pending] callback error: {e}")

    # ── 主執行緒 ───────────────────────────────────────────────────────

    def _run(self):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._connect_ws(loop))
        except Exception as e:
            logger.error(f"[BitfinexWS] 執行例外: {e}\n{traceback.format_exc()}")

    async def _connect_ws(self, loop):
        import websockets

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

                    # 訂閱
                    await ws.send(json.dumps({"event": "subscribe", "channel": "funding", "symbol": "fUSD"}))
                    await ws.send(json.dumps({"event": "subscribe", "channel": "wallet"}))

                    logger.info("[BitfinexWS] 訂閱完成，開始接收事件")

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

    # ── 訊息處理 ───────────────────────────────────────────────────────

    def _handle_message(self, msg):
        try:
            if not isinstance(msg, list) or len(msg) < 2:
                return

            channel = msg[1] if len(msg) > 1 else None
            data    = msg[2] if len(msg) > 2 else None

            if channel in ["oc", "on"]:
                self._handle_conf(channel, data)
            elif channel == EVENT_WALLET:
                self._handle_wallet(data)
            elif channel in ["fon", "fou", "foc"]:
                self._handle_offer(channel, data)
            elif channel in ["fcn", "fcu", "fcc"]:
                self._handle_credit(channel, data)
            elif channel == "hb":
                pass  # heartbeat，忽略
            elif channel in ["ps", "os", "fos", "fcs", "fls", "ws", "bu"]:
                # 狀態快照，紀錄一下就好
                pass
            else:
                logger.info(f"[BitfinexWS] 未知頻道 [{channel}]: {str(data)[:200]}")
        except Exception as e:
            logger.error(f"[_handle_message] {e}")

    def _handle_conf(self, channel, data):
        """
        on / oc 的伺服器回應。
        Bitfinex 指令確認格式（conf 頻道）：
          [chanId, "on",  [status_code, null, offer_info]]
          [chanId, "oc",  [status_code, null, [offer_id, ...]]]

        status 0 = 成功
        """
        try:
            if not isinstance(data, list) or len(data) < 2:
                return
            status = data[0]
            ok = (status == 0)

            if channel == "on":
                if len(data) >= 3 and isinstance(data[2], list) and len(data[2]) > 0:
                    offer_id = str(data[2][0])
                    logger.info(f"[BitfinexWS] on conf OK: offer_id={offer_id} status={status}")
                    self.on_offer_new_conf(offer_id, data[2], ok)
                else:
                    logger.warning(f"[BitfinexWS] on conf status={status} data={data}")
                    self.on_offer_new_conf(None, data, ok)

            elif channel == "oc":
                if len(data) >= 3 and isinstance(data[2], list):
                    logger.info(f"[BitfinexWS] oc conf OK: {data[2]} status={status}")
                    self.on_offer_cancel_conf(data[2], ok)
                else:
                    logger.warning(f"[BitfinexWS] oc conf status={status} data={data}")
                    self.on_offer_cancel_conf([], ok)
        except Exception as e:
            logger.error(f"[_handle_conf] {e}")

    # ── 事件 handlers ───────────────────────────────────────────────────

    def _handle_wallet(self, data):
        try:
            if not isinstance(data, list) or len(data) < 4:
                return
            wtype, currency, balance, available = data[0], data[1], data[2], data[3]
            logger.info(f"[BitfinexWS] 錢包更新: {wtype} {currency} {balance}")
            self.on_wallet(wtype, currency, balance, available)
        except Exception as e:
            logger.error(f"[_handle_wallet] {e}")

    def _handle_offer(self, event, data):
        try:
            if not isinstance(data, list) or len(data) < 3:
                return
            offer_id   = str(data[0]) if data[0] else ""
            status     = str(data[10]) if len(data) > 10 else ""
            amount     = float(data[4]) if len(data) > 4 and data[4] else 0.0
            rate       = float(data[14]) if len(data) > 14 and data[14] else 0.0
            rate_pct   = rate * 100
            period     = int(data[15]) if len(data) > 15 and data[15] else 0
            ts_created = int(data[2]) if len(data) > 2 and data[2] else 0
            ts_updated = int(data[9]) if len(data) > 9 and data[9] else ts_created

            logger.info(f"[BitfinexWS] offer id={offer_id} event={event} status={status} "
                        f"amount={amount} rate={rate_pct}% period={period}d")
            self.on_offer(event, offer_id, status, amount, period, rate_pct, ts_created, ts_updated, data)
        except Exception as e:
            logger.error(f"[_handle_offer] {e}")

    def _handle_credit(self, event, data):
        try:
            if isinstance(data, list) and len(data) == 2 and data[0] == 'fcc':
                data = data[1]
            if not isinstance(data, list) or len(data) < 3:
                return
            credit_id  = str(data[0]) if data[0] else ""
            status     = str(data[7]) if len(data) > 7 else ""
            amount     = float(data[5]) if len(data) > 5 and data[5] else 0.0
            rate       = float(data[11]) if len(data) > 11 and data[11] else 0.0
            rate_pct   = rate * 100
            period     = int(data[12]) if len(data) > 12 and data[12] else 0
            ts_created = int(data[2]) if len(data) > 2 and data[2] else 0
            ts_updated = int(data[3]) if len(data) > 3 and data[3] else ts_created

            logger.info(f"[BitfinexWS] credit id={credit_id} event={event} status={status} "
                        f"amount={amount} rate={rate_pct}% period={period}d")
            self.on_credit(event, credit_id, status, amount, period, rate_pct, ts_created, ts_updated, data)
        except Exception as e:
            logger.error(f"[_handle_credit] {e}")


# ── 測試 ─────────────────────────────────────────────────────────────

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