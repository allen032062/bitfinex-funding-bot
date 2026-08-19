# bitfinex_monitor.py
# =============================================================================
# Bitfinex Authenticated WebSocket 即時監控（多租戶版）
# =============================================================================
#
# 【架構說明】
#   每個 Telegram 使用者（uid）啟動一條獨立的 Bitfinex WS 連線，
#   所有 WS 連線集中管理在模組層級的 _ws_clients {}。
#
# 【收到的事件與處理邏輯】
#   1. wu（錢包更新）
#      → 金額變動時，自動更新 Telegram 置頂狀態訊息
#      → 同時寫入 telegram_bot 的餘額快取（供 UI 按鈕使用）
#
#   2. fon（Funding Offer New，掛單確認）
#      → 發送 Telegram 訊息，並附上「❌ 取消掛單」快速按鈕
#      → 訊息由 msg_map 追蹤，後續以原地編輯取代反覆發送
#
#   3. foc（Funding Offer Cancel，成交/取消）
#      → 成交（EXECUTED）：編輯訊息為 🔔 成交，並移除追蹤
#      → 取消（CANCELED）：編輯訊息為 ❌ 取消，並移除追蹤
#
#   4. fcn / fcc（Credit 新建/關閉，即借方還款）
#      → fcn：借出成功通知
#      → fcc：還款完成通知
#
#   5. on / oc 確認（指令執行結果）
#      → 由 funding_monitor 注入 _offer_new_conf_cb / _offer_cancel_conf_cb
#      → 觸發時呼叫 funding_monitor 的確認邏輯（重試/狀態同步）
#
# 【訊息追蹤機制（_track_msg / _track_close）】
#   - 每筆 offer/credit 有唯一 msg_id（= offer_id / credit_id）
#   - msg_map[chat_id][msg_id] = Telegram message_id
#   - 訊息存在 → 原地 edit；編輯失敗（如內容相同）→ 跳過
#   - 成交/取消後 → 編輯成結束狀態，並從 msg_map 移除
#
# 【Bitfinex WS 異常對策】
#   - Bitfinex WS bug：funding wallet 有時 avail=0 但 balance>0
#     → 自動用 balance 取代 avail
#   - 置頂訊息編輯失敗（非 not-modified 錯誤）→ 砍掉重建（由 ensure_pinned_welcome 處理）
#
# 【與 telegram_bot 的協作】
#   - update_user_balance_cache()：每次 wu 更新時寫入快取
#   - render_funding_status()：產生置頂訊息的格式化文字
#   - telegram_ws.global_edit_message()：編輯已發送的追蹤訊息
# =============================================================================

import json
import logging
import os
import threading
from datetime import datetime, timezone

import telegram_ws  # 用於編輯追蹤訊息（由 telegram_bot.py 初始化）

logger = logging.getLogger(__name__)

# =============================================================================
# 全域變數（多租戶 WS 客戶端管理）
# =============================================================================

# 所有 WS 客戶端：{uid: ws_instance}
_ws_clients = {}
_ws_lock = threading.Lock()

# 多租戶訊息追蹤地圖：{chat_id: {msg_id: message_id}}
# msg_id = offer_id 或 credit_id，統一管理
_ws_msg_map = {}
_ws_msg_lock = threading.Lock()  # 保護 _ws_msg_map 的讀寫操作


# =============================================================================
# 公開 API
# =============================================================================

def get_ws_client(uid):
    """
    取得指定 uid 的 WS 實例

    供 funding_monitor / telegram_bot 呼叫以下單/撤單

    參數：
        uid: 用戶 ID（chat_id 字串）

    回傳：
        BitfinexWS 實例或 None
    """
    with _ws_lock:
        return _ws_clients.get(uid)


# =============================================================================
# 輔助函式
# =============================================================================

def _dt_taiwan(ts_ms):
    """
    將 UTC 毫秒時間戳轉換為「MM/DD HH:mm」格式（台灣時區）

    參數：
        ts_ms: UTC 毫秒時間戳

    回傳：
        格式化時間字串（如 "08/19 14:30"）
    """
    if not ts_ms:
        return "?"
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(None).strftime("%m/%d %H:%M")


def _fmt_msg(emoji, text):
    """
    統一的訊息格式包裝

    參數：
        emoji: emoji 符號
        text: 訊息本文

    回傳：
        格式：[emoji]《狀況》\n\n{text}
    """
    return f"[{emoji}]《狀況》\n\n{text}"


# =============================================================================
# 單一用戶 WS 啟動函式
# =============================================================================

def _start_user_ws(uid, user, send_message, update_pinned, pin_message, get_pinned_message, create_exchange, load_users):
    """
    為單一用戶啟動 Bitfinex WS 連線

    這是一個執行緒函式，每個用戶獨立執行

    參數：
        uid: 用戶 ID
        user: 用戶資料字典
        send_message: 發送 Telegram 訊息函式
        update_pinned: 編輯置頂訊息函式
        pin_message: 置頂訊息函式
        get_pinned_message: 取得置頂訊息函式
        create_exchange: 建立交易所實例函式
        load_users: 載入用戶資料函式
    """
    try:
        api_key = user.get("api_key")
        api_secret = user.get("api_secret")
        chat_id = user.get("chat_id")
        if not all([api_key, api_secret, chat_id]):
            logger.warning(f"[BitfinexWS] uid={uid} 缺少 credentials")
            return

        from bitfinex_ws import BitfinexWS

        # =====================================================================
        # Closure 變數（在巢狀函式之前宣告）
        # =====================================================================
        prev_bal_usd = 0.0       # 上次錢包總額（用於判斷是否變動）
        prev_avail_usd = 0.0     # 上次錢包可用（用於判斷是否變動）
        last_status_msg = ""     # 快取上次發送的置頂訊息（避免重複編輯）

        # 啟動時從 users.json 讀取 pin_message_id（唯一正確來源）
        # 注意：on_wallet 只編輯此訊息，不重建
        # 訊息創建/重建完全由 telegram_bot.ensure_pinned_welcome() 處理
        users_data_init = load_users()
        user_init = users_data_init["users"].get(uid, {})
        pinned_msg_id = user_init.get("pin_message_id")
        logger.info(f"[Pin] uid={uid} 載入 pin_message_id={pinned_msg_id} (只編輯，不重建)")

        # 統一訊息追蹤（Offer + Credit 共用）
        # 從模組層級讀取，支援 WS 斷線重連後繼續追蹤
        msg_map = _ws_msg_map.setdefault(int(chat_id), {})

        # =====================================================================
        # 訊息追蹤輔助函式
        # =====================================================================

        def _try_edit_msg(msg_id, text):
            """
            嘗試編輯已追蹤的訊息

            忽略 "not modified" 錯誤（內容相同）；其他錯誤回傳 False

            參數：
                msg_id: Telegram message_id
                text: 新的訊息內容

            回傳：
                True = 成功或忽略，False = 失敗
            """
            try:
                result = telegram_ws.global_edit_message(int(chat_id), msg_id, text)
                if result and result.get("ok"):
                    return True
                err = result.get("description", "") if result else ""
                if "not modified" not in err and "exactly the same" not in err:
                    logger.warning(f"[Track] edit msg_id={msg_id} failed: {err}")
                    return False
                return True
            except Exception as e:
                logger.error(f"[Track] edit msg_id={msg_id} exception: {e}")
                return False

        def _track_msg(msg_id, text, reply_markup=None):
            """
            統一追蹤：有 msg_id 就原地編輯，沒有或失敗就發新訊息並記錄

            參數：
                msg_id: offer_id 或 credit_id（作為 map 的 key）
                text: 訊息本文（不包含 _fmt_msg 包裝）
                reply_markup: 選用的 inline keyboard（僅新訊息時有效）

            回傳：
                Telegram message_id 或 None
            """
            # 先用 lock 讀取，避免 concurrent _track_close 刪除時讀到過期資料
            with _ws_msg_lock:
                existing_msg_id = msg_map.get(msg_id)
            if existing_msg_id:
                ok = _try_edit_msg(existing_msg_id, text)
                if ok:
                    return existing_msg_id
            # 沒有或 edit 失敗 → 發新訊息並記錄
            result = send_message(int(chat_id), text, reply_markup=reply_markup)
            if result and result.get("ok"):
                new_msg_id = result.get("result", {}).get("message_id")
                if new_msg_id:
                    with _ws_msg_lock:
                        msg_map[msg_id] = new_msg_id
                    logger.info(f"[Track] {msg_id} -> msg {new_msg_id}")
                    return new_msg_id
            return None

        def _track_close(msg_id, text, emoji=None):
            """
            編輯成結束狀態後移除追蹤

            用於：成交、取消、還款完成等結束狀態

            參數：
                msg_id: offer_id 或 credit_id
                text: 結束狀態訊息本文
                emoji: 可選的 emoji（前綴到 _fmt_msg）
            """
            content = _fmt_msg(emoji, text) if emoji else text
            _track_msg(msg_id, content)
            with _ws_msg_lock:
                if msg_id in msg_map:
                    del msg_map[msg_id]
                    logger.info(f"[Track] {msg_id} closed and removed")

        # =====================================================================
        # wu（錢包更新）事件處理
        # =====================================================================

        def on_wallet(wtype, currency, balance, available):
            """
            處理錢包更新事件

            Bitfinex WS 會在餘額變動時推送 wu 事件
            用途：更新置頂訊息、寫入餘額快取

            參數：
                wtype: 錢包類型（"funding" / "exchange"）
                currency: 幣種（"USD" / "BTC" / ...）
                balance: 總餘額
                available: 可用餘額
            """
            nonlocal prev_bal_usd, prev_avail_usd, pinned_msg_id, last_status_msg
            balance_f = float(balance)
            avail_f = float(available)
            avail_raw = avail_f  # 記錄原始值（用於通知顯示）

            # Bitfinex WS bug: funding wallet 有時 avail=0 但 balance > 0
            # 自動用 balance 取代 avail
            if wtype == "funding" and currency == "USD" and avail_f == 0 and balance_f > 0:
                avail_f = balance_f

            # 只處理 funding USD
            if currency != "USD" or wtype != "funding":
                return

            # 寫入餘額快取（每次 wu 更新都寫！）
            try:
                import telegram_bot
                telegram_bot.update_user_balance_cache(chat_id, f"{balance_f:.2f}", f"{avail_f:.2f}")
            except Exception as e:
                logger.error(f"[Wu] cache error: {e}")

            # 判斷是否需要更新置頂訊息（餘額變動 > 0.001 USD）
            usd_changed = (abs(balance_f - prev_bal_usd) > 0.001 or abs(avail_f - prev_avail_usd) > 0.001)

            # 產生置頂訊息內容（精簡版）
            status_msg = telegram_bot.render_funding_status(avail_f, balance_f, include_full_dashboard=False)

            # =================================================================
            # 置頂訊息更新（只編輯，不重建）
            # pinned_msg_id 由 telegram_bot.ensure_pinned_welcome() 獨家管理
            # =================================================================
            if pinned_msg_id is None:
                # 第一筆 wu 事件早於 ensure_pinned_welcome 完成，靜默跳過
                prev_bal_usd = balance_f
                prev_avail_usd = avail_f
                return

            if usd_changed:
                edit_result = update_pinned(int(chat_id), pinned_msg_id, status_msg)
                if edit_result and edit_result.get("ok", False):
                    last_status_msg = status_msg
                    logger.info(f"[Wu] uid={uid} edited msg_id={pinned_msg_id}")
                else:
                    err = edit_result.get("description", "") if isinstance(edit_result, dict) else str(edit_result)
                    if "not modified" in err or "exactly the same" in err:
                        # 內容相同，忽略
                        last_status_msg = status_msg
                        logger.info(f"[Wu] uid={uid} edit not modified, skip")
                    else:
                        # edit 失敗（訊息被刪）：只 log，不重建
                        # 下次 ensure_pinned_welcome 執行時會重建
                        logger.warning(f"[Wu] uid={uid} edit failed ({err}), awaiting ensure_pinned_welcome to heal")

            prev_bal_usd = balance_f
            prev_avail_usd = avail_f

        # =====================================================================
        # fon/foc（Funding Offer New/Confirm）事件處理
        # =====================================================================

        def on_offer(event, offer_id, status, amount, period, rate_pct, ts_created, ts_updated, data):
            """
            處理 Funding Offer 事件

            事件類型：
            - fon（Funding Offer New）：掛單確認
            - fou（Funding Offer Update）：掛單更新（忽略）
            - foc（Funding Offer Cancel）：成交/取消

            參數：
                event: 事件名稱（"fon" / "fou" / "foc"）
                offer_id: 掛單 ID
                status: 狀態（"ACTIVE" / "EXECUTED" / "CANCELED"）
                amount: 金額
                period: 天期
                rate_pct: 日利率（百分比）
                ts_created: 建立時間（ms）
                ts_updated: 更新時間（ms）
                data: 原始資料
            """
            if event == "fou":
                logger.info(f"[Fou] uid={uid} {offer_id} fou update, skipped")
                return

            if event == "foc":
                # 成交或取消
                ts_str = _dt_taiwan(ts_updated)
                if "CANCELED" in status:
                    text = (
                        f"❌ Funding Offer 已取消\n\n"
                        f"ID: <code>{offer_id}</code>\n"
                        f"金額: {amount:.6f} USD\n"
                        f"利率: {rate_pct:.4f}%/天({rate_pct*365:.2f}%/年)\n"
                        f"天期: {period}天\n"
                        f"取消時間: {ts_str}"
                    )
                    _track_close(offer_id, text, emoji="❌ 取消")
                    logger.info(f"[Fou] uid={uid} {offer_id} CANCELED")
                elif "EXECUTED" in status:
                    orig_amount = amount
                    if not amount or amount == 0:
                        if isinstance(data, (list, tuple)) and len(data) > 4:
                            orig_amount = float(data[4] or data[5] or 0)
                    text = (
                        f"🔔 Funding Offer 已成交!\n\n"
                        f"ID: <code>{offer_id}</code>\n"
                        f"金額: {orig_amount:.6f} USD\n"
                        f"利率: {rate_pct:.4f}%/天({rate_pct*365:.2f}%/年)\n"
                        f"天期: {period}天\n"
                        f"成交時間: {ts_str}\n\n"
                        f"注意: 這筆錢已被借走"
                    )
                    _track_close(offer_id, text, emoji="🔔 成交")
                    logger.info(f"[Fou] uid={uid} {offer_id} EXECUTED: {orig_amount} USD")
                else:
                    logger.warning(f"[Fou] uid={uid} {offer_id} foc unknown status={status}")
                return

            if event == "fon":
                # 新掛單確認
                ts_str = _dt_taiwan(ts_created)
                if isinstance(rate_pct, (int, float)) and rate_pct > 100:
                    logger.error(f"[Fou] uid={uid} {offer_id} fon: 利率異常 rate_pct={rate_pct}")
                    return
                cancel_btn = {"inline_keyboard": [[
                    {"text": "❌ 取消掛單", "callback_data": f"cancel_offer_{offer_id}"}
                ]]}
                text = (
                    f"📝 <b>Funding Offer 已掛出</b>\n\n"
                    f"ID: <code>{offer_id}</code>\n"
                    f"金額: <code>{amount:.6f} USD</code>\n"
                    f"利率: {rate_pct:.4f}%/天 ({float(rate_pct)*365:.2f}%/年)\n"
                    f"天期: {period}天\n"
                    f"掛單時間: {ts_str}"
                )
                _track_msg(offer_id, _fmt_msg("📝 掛單", text), reply_markup=cancel_btn)
                logger.info(f"[Fou] uid={uid} {offer_id} FON (New)")
                return

        # =====================================================================
        # fcn/fcc（Credit New/Close）事件處理
        # =====================================================================

        def on_credit(event, credit_id, status, amount, period, rate_pct, ts_created, ts_updated, data):
            """
            處理 Funding Credit 事件（借款人借還款相關）

            事件類型：
            - fcn（Funding Credit New）：借出成功
            - fcc（Funding Credit Close）：還款完成
            - fcu（Funding Credit Update）：更新（忽略）

            參數：
                event: 事件名稱
                credit_id: Credit ID
                status: 狀態
                amount: 本金金額
                period: 天期
                rate_pct: 日利率（百分比）
                ts_created: 建立時間
                ts_updated: 更新時間
                data: 原始資料
            """
            if event == "fcu":
                logger.info(f"[Fcu] uid={uid} {credit_id} fcu update, skipped")
                return

            ts_str = _dt_taiwan(ts_updated)

            if event == "fcc":
                # 還款完成
                # amount = 本金（principal），利息另行結算進 swap
                # 利息 = 本金 × 日利率 × 天期
                principal = float(amount) if amount else 0.0
                daily_rate = float(rate_pct) / 100.0 if rate_pct else 0.0
                interest_earned = principal * daily_rate * float(period)
                text = (
                    f"✅ <b>Funding 已還款</b>\n\n"
                    f"Credit ID: <code>{credit_id}</code>\n"
                    f"本金: <code>{principal:.6f} USD</code>\n"
                    f"利息: <code>+{interest_earned:.6f} USD</code>\n"
                    f"合計: <code>{principal + interest_earned:.6f} USD</code>\n"
                    f"利率: {rate_pct:.4f}%/天 ({float(rate_pct)*365:.2f}%/年)\n"
                    f"天期: {period}天\n"
                    f"還款時間: {ts_str}"
                )
                _track_close(credit_id, text, emoji="✅ 還款")
                logger.info(f"[Fcu] uid={uid} {credit_id} FCC 還款本金={principal} 利息={interest_earned:.6f}")
                return

            if event == "fcn":
                # 借出成功
                text = (
                    f"🔔 Funding 借出成功\n\n"
                    f"Credit ID: <code>{credit_id}</code>\n"
                    f"金額: {amount:.6f} USD\n"
                    f"利率: {rate_pct:.4f}%/天({rate_pct*365:.2f}%/年)\n"
                    f"天期: {period}天\n"
                    f"到期: {_dt_taiwan(ts_created + period * 86400000)}"
                )
                _track_msg(credit_id, _fmt_msg("🔔 借出", text))
                logger.info(f"[Fcu] uid={uid} {credit_id} FCN (New)")
                return

        # =====================================================================
        # on/oc 指令確認 callback（由 funding_monitor 動態注入）
        # =====================================================================
        _offer_new_conf_cb = None
        _offer_cancel_conf_cb = None

        def on_offer_new_conf(offer_id, data, ok):
            """
            WS 收到 on（新掛單）指令確認

            由 funding_monitor 註冊處理邏輯
            """
            logger.info(f"[BitfinexWS] uid={uid} on conf: offer_id={offer_id} ok={ok} cb_exists={_offer_new_conf_cb is not None}")
            if _offer_new_conf_cb:
                try:
                    _offer_new_conf_cb(uid, offer_id, data, ok)
                except Exception as e:
                    logger.error(f"[on_offer_new_conf] callback error: {e}")

        def on_offer_cancel_conf(offer_ids, ok):
            """
            WS 收到 oc（撤單）指令確認

            由 funding_monitor 註冊處理邏輯
            """
            logger.info(f"[BitfinexWS] uid={uid} oc conf: offer_ids={offer_ids} ok={ok}")
            if _offer_cancel_conf_cb:
                try:
                    _offer_cancel_conf_cb(uid, offer_ids, ok)
                except Exception as e:
                    logger.error(f"[on_offer_cancel_conf] callback error: {e}")

        # =====================================================================
        # 建立並啟動 WS
        # =====================================================================
        ws = BitfinexWS(
            api_key=api_key,
            api_secret=api_secret,
            on_wallet=on_wallet,
            on_offer=on_offer,
            on_credit=on_credit,
            on_offer_new_conf=on_offer_new_conf,
            on_offer_cancel_conf=on_offer_cancel_conf,
        )
        ws.start()

        # 對外導出 conf callback 註冊函式（供 funding_monitor 呼叫）
        def register_offer_callbacks(new_cb, cancel_cb):
            nonlocal _offer_new_conf_cb, _offer_cancel_conf_cb
            _offer_new_conf_cb = new_cb
            _offer_cancel_conf_cb = cancel_cb
            logger.info(f"[BitfinexWS] uid={uid} callbacks registered")

        ws._register_offer_callbacks = register_offer_callbacks

        with _ws_lock:
            _ws_clients[uid] = ws
        logger.info(f"[BitfinexWS] uid={uid} WS 啟動")

    except Exception as e:
        logger.error(f"[_start_user_ws] uid={uid}: {e}")


# =============================================================================
# 啟動/停止所有用戶 WS
# =============================================================================

def start_all_user_ws(load_users, create_exchange, send_message, update_pinned, pin_message, get_pinned_message):
    """
    為所有已註冊用戶啟動 Bitfinex WS 連線

    每個用戶一個獨立執行緒

    參數：
        load_users: 載入用戶資料回調
        create_exchange: 建立交易所實例回調
        send_message: 發送訊息函式
        update_pinned: 編輯置頂訊息函式
        pin_message: 置頂訊息函式
        get_pinned_message: 取得置頂訊息函式
    """
    users_data = load_users()
    for uid, user in users_data.get("users", {}).items():
        t = threading.Thread(
            target=_start_user_ws,
            args=(uid, user, send_message, update_pinned, pin_message, get_pinned_message, create_exchange, load_users),
            daemon=True
        )
        t.start()


def stop_all_user_ws():
    """
    停止所有 Bitfinex WS 連線

    通常用於程式關閉時
    """
    with _ws_lock:
        for uid, ws in _ws_clients.items():
            try:
                ws.stop()
            except:
                pass
        _ws_clients.clear()
