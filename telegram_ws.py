# telegram_ws.py
# =============================================================================
# Telegram Bot API 網路通訊層
# =============================================================================
#
# 【模組職責】
#   封裝所有 Telegram Bot API 呼叫（HTTP Long Polling 模式）
#   提供兩種介面：
#   1. global_* 函式：全域單例，供 telegram_bot.py 直接呼叫
#   2. TelegramWebSocket 類別：Long Polling 迴圈，接收新訊息/回調
#
# 【架構說明】
#   底層使用 Telegram Bot API (HTTP)：
#   - 接收：新訊息透過 Long Polling getUpdates（每 30 秒 timeout）
#   - 發送：sendMessage / editMessage 等 REST 呼叫
#
#   Long Polling 封裝成「類 WebSocket」介面：
#   - TelegramWebSocket._run() 在背景執行緒中不斷呼叫 getUpdates
#   - 收到新 update 時呼叫 on_update 回調
#   - 外部無需知道底層是 HTTP，語意上像 WebSocket 一樣即時
#
# 【與 telegram_bot.py 的關係】
#   - telegram_ws.init(bot_token) 由 telegram_bot.py 啟動時呼叫
#   - 全域函式直接供 telegram_bot.py 呼叫，無需实例化
#   - TelegramWebSocket 實例由 telegram_bot.py 建立並呼叫 start()
# =============================================================================

import json
import time
import logging
import threading
import requests as _requests
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# =============================================================================
# 全域變數（由 telegram_bot.py 初始化時設定）
# =============================================================================

_bot_token = None
_base_url = None


def init(bot_token: str):
    """
    初始化全域 Telegram Bot Token

    由 telegram_bot.py 在啟動時呼叫，設定全域的 _bot_token 和 _base_url

    參數：
        bot_token: Telegram Bot Token（如 "123456789:ABCdef..."）
    """
    global _bot_token, _base_url
    _bot_token = bot_token
    _base_url = f"https://api.telegram.org/bot{bot_token}"


def _check_init():
    """
    檢查是否已初始化（保護全域函式）

    例外：
        RuntimeError: 若 _bot_token 未設定
    """
    if not _bot_token:
        raise RuntimeError("telegram_ws.init() 尚未被呼叫")


# =============================================================================
# 全域 helper（全供 telegram_bot.py 直接呼叫）
# =============================================================================

def global_send_message(chat_id, text, parse_mode="HTML", reply_markup=None):
    """
    發送 Telegram 訊息

    參數：
        chat_id: 目標聊天 ID
        text: 訊息內容
        parse_mode: 解析模式（預設 HTML）
        reply_markup: 回覆鍵盤（如 inline keyboard）

    回傳：
        Telegram API 回應或 None（失敗時）
    """
    _check_init()
    url = f"{_base_url}/sendMessage"
    data = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if reply_markup:
        if hasattr(reply_markup, 'to_dict'):
            data["reply_markup"] = json.dumps(reply_markup.to_dict())
        else:
            data["reply_markup"] = json.dumps(reply_markup)
    try:
        resp = _requests.post(url, data=data, timeout=10)
        result = resp.json()
        if result.get("ok"):
            return result
        logger.warning(f"[TelegramWS] send_message failed: {result}")
        return result
    except Exception as e:
        logger.error(f"[TelegramWS] send_message 失敗: {e}")
        return None


def global_send_chat_action(chat_id, action="typing"):
    """
    發送聊天狀態（讓對方看到「正在輸入...」）

    參數：
        chat_id: 目標聊天 ID
        action: 動作類型（typing / upload_photo / ...）
    """
    _check_init()
    url = f"{_base_url}/sendChatAction"
    try:
        _requests.post(url, data={"chat_id": chat_id, "action": action}, timeout=5)
    except:
        pass


def global_edit_message(chat_id, message_id, text, parse_mode="HTML", reply_markup=None):
    """
    編輯已發送的訊息

    參數：
        chat_id: 聊天 ID
        message_id: 要編輯的訊息 ID
        text: 新的訊息內容
        parse_mode: 解析模式
        reply_markup: 新的回覆鍵盤

    回傳：
        Telegram API 回應
    """
    _check_init()
    url = f"{_base_url}/editMessageText"
    data = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": parse_mode}
    if reply_markup:
        if hasattr(reply_markup, 'to_dict'):
            data["reply_markup"] = json.dumps(reply_markup.to_dict())
        else:
            data["reply_markup"] = json.dumps(reply_markup)
    try:
        result = _requests.post(url, data=data, timeout=10).json()
        logger.info(f"[TelegramWS] editMessage resp: {result}")

        if not result.get("ok"):
            err = result.get("description", "")
            if "message is not modified" in err or "exactly the same" in err:
                # 內容相同，不是錯誤
                return result
            logger.error(f"[TelegramWS] edit_message failed: {err}")
        return result
    except Exception as e:
        # 編輯錯誤靜默忽略（訊息可能已被刪除等）
        return None


def global_get_chat_message(chat_id, message_id):
    """
    取得指定訊息的內容

    參數：
        chat_id: 聊天 ID
        message_id: 訊息 ID

    回傳：
        訊息內容或 None
    """
    _check_init()
    url = f"{_base_url}/getMessage"
    data = {"chat_id": chat_id, "message_id": message_id}
    try:
        return _requests.get(url, params=data, timeout=10).json()
    except Exception as e:
        logger.error(f"[TelegramWS] get_chat_message failed: {e}")
        return None


def global_verify_message(chat_id, message_id):
    """
    驗證特定訊息是否存在

    參數：
        chat_id: 聊天 ID
        message_id: 訊息 ID

    回傳：
        True = 存在，False = 不存在或失敗
    """
    _check_init()
    url = f"{_base_url}/getMessage"
    data = {"chat_id": chat_id, "message_id": message_id}
    try:
        result = _requests.get(url, params=data, timeout=10).json()
        return result.get("ok", False) and result.get("result") is not None
    except Exception as e:
        logger.error(f"[TelegramWS] verify_message failed: {e}")
        return False


def global_answer_callback(callback_id, text=""):
    """
    回應 callback query（關閉 Telegram 按鈕的 loading 狀態）

    參數：
        callback_id: 回調 ID
        text: 顯示的 toast 文字（可選）
    """
    _check_init()
    url = f"{_base_url}/answerCallbackQuery"
    try:
        _requests.post(url, data={"callback_query_id": callback_id, "text": text}, timeout=5)
    except:
        pass


def global_set_my_commands():
    """
    設定 Bot Menu 命令列表

    在 Telegram 輸入框顯示斜線命令時看到這些描述
    """
    _check_init()
    url = f"{_base_url}/setMyCommands"
    commands = [
        {"command": "start", "description": "開始 / 設定"},
        {"command": "status", "description": "Funding 狀態"},
        {"command": "frr", "description": "目前 FRR 利率"},
        {"command": "funding", "description": "Funding 詳細"},
        {"command": "report", "description": "收益報告"},
        {"command": "menu", "description": "主選單"},
    ]
    try:
        resp = _requests.post(url, json={"commands": commands}, timeout=10)
        result = resp.json()
        if result.get("ok"):
            logger.info("[TelegramWS] setMyCommands succeeded")
        else:
            logger.warning(f"[TelegramWS] setMyCommands failed: {result}")
    except Exception as e:
        logger.error(f"[TelegramWS] setMyCommands error: {e}")


def global_pin_message(chat_id, message_id, disable_notification=True):
    """
    置頂訊息

    參數：
        chat_id: 聊天 ID
        message_id: 訊息 ID
        disable_notification: 是否靜音（預設 True）
    """
    _check_init()
    url = f"{_base_url}/pinChatMessage"
    try:
        _requests.post(url, data={"chat_id": chat_id, "message_id": message_id, "disable_notification": disable_notification}, timeout=5)
    except Exception as e:
        logger.error(f"[TelegramWS] pin_message 失敗: {e}")


def global_unpin_message(chat_id, message_id=None):
    """
    解除置頂訊息

    參數：
        chat_id: 聊天 ID
        message_id: 要解除的訊息 ID（可選，不傳則解除所有）
    """
    _check_init()
    url = f"{_base_url}/unpinChatMessage"
    data = {"chat_id": chat_id}
    if message_id:
        data["message_id"] = message_id
    try:
        _requests.post(url, data=data, timeout=5)
    except Exception as e:
        logger.error(f"[TelegramWS] unpin_message 失敗: {e}")


def global_get_chat_pinned_message(chat_id):
    """
    取得目前置頂訊息

    參數：
        chat_id: 聊天 ID

    回傳：
        pinned_message 的 message_id 或 None
    """
    _check_init()
    url = f"{_base_url}/getChat"
    try:
        resp = _requests.get(url, params={"chat_id": chat_id}, timeout=5).json()
        if resp.get("ok"):
            pinned = resp["result"].get("pinned_message", {})
            return pinned.get("message_id")
    except:
        pass
    return None


# =============================================================================
# TelegramWebSocket 類別（Long Polling 接收訊息）
# =============================================================================

class TelegramWebSocket:
    """
    Telegram Bot WebSocket 客戶端（Long Polling 包裝）

    用法：
        ws = TelegramWebSocket(bot_token, on_update, on_error)
        ws.start()      # 啟動背景接收迴圈
        ws.stop()       # 停止

    底層機制：
        執行緒中每 30 秒呼叫一次 getUpdates Long Polling
        有新訊息時透過 on_update 回調通知

    與真正 WebSocket 的差異：
        - 接收模式：Long Polling（非真正 WS）
        - 發送模式：HTTP REST（sendMessage 等）
        - 兩者封裝成統一介面，外部視為同一個雙向通道
    """

    def __init__(self, bot_token: str, on_update: Callable, on_error: Optional[Callable] = None):
        """
        初始化 TelegramWebSocket

        參數：
            bot_token: Telegram Bot Token
            on_update: 回調函式，收到新訊息時呼叫 (update) -> None
            on_error: 錯誤回調函式 (error) -> None
        """
        self.bot_token = bot_token
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.on_update = on_update        # 收到新訊息時觸發
        self.on_error = on_error         # 發生錯誤時觸發
        self._running = False             # 運行標誌
        self._thread = None              # 背景執行緒
        self._offset = 0                # getUpdates offset（防止重複）
        self._lock = threading.Lock()     # offset 更新鎖

    def start(self):
        """啟動 WebSocket 背景接收執行緒"""
        if self._running:
            logger.warning("[TelegramWS] 已在執行中")
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("[TelegramWS] 啟動")

    def stop(self):
        """停止 WebSocket"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("[TelegramWS] 已停止")

    def _run(self):
        """
        WebSocket 主接收迴圈

        不斷呼叫 getUpdates Long Polling，有新訊息時呼叫 on_update
        """
        logger.info("[TelegramWS] 開始接收訊息")
        while self._running:
            try:
                updates = self._fetch_updates()
                if updates:
                    for update in updates:
                        # 更新 offset（下次只取新訊息）
                        self._offset = update["update_id"] + 1
                        try:
                            self.on_update(update)
                        except Exception as e:
                            logger.error(f"[TelegramWS] on_update 錯誤: {e}")
            except Exception as e:
                logger.error(f"[TelegramWS] fetch error: {e}")
                if self.on_error:
                    try:
                        self.on_error(e)
                    except:
                        pass
                time.sleep(5)

    def _fetch_updates(self):
        """
        向 Telegram 取得新訊息（Long Polling）

        每 30 秒 timeout一次，有新訊息則立即回傳

        回傳：
            updates 列表（可能為空）
        """
        import requests
        url = f"{self.base_url}/getUpdates"
        try:
            resp = requests.get(
                url,
                params={"offset": self._offset, "timeout": 30},
                timeout=35
            )
            if resp.status_code == 200:
                result = resp.json()
                if result.get("ok"):
                    return result.get("result", [])
            return []
        except Exception as e:
            logger.error(f"[TelegramWS] getUpdates 失敗: {e}")
            return []

    # =========================================================================
    # 發送相關（與全域函式相同，供內部/外部使用）
    # =========================================================================

    def send_message(self, chat_id, text, parse_mode="HTML", reply_markup=None):
        """發送訊息"""
        import requests
        url = f"{self.base_url}/sendMessage"
        data = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
        if reply_markup:
            if hasattr(reply_markup, 'to_dict'):
                data["reply_markup"] = json.dumps(reply_markup.to_dict())
            else:
                data["reply_markup"] = json.dumps(reply_markup)
        try:
            resp = requests.post(url, data=data, timeout=10)
            return resp.json()
        except Exception as e:
            logger.error(f"[TelegramWS] send_message 失敗: {e}")
            return None

    def send_chat_action(self, chat_id, action="typing"):
        """發送 typing 狀態"""
        import requests
        url = f"{self.base_url}/sendChatAction"
        try:
            requests.post(url, data={"chat_id": chat_id, "action": action}, timeout=5)
        except:
            pass

    def pin_message(self, chat_id, message_id):
        """置頂訊息"""
        import requests
        url = f"{self.base_url}/pinChatMessage"
        try:
            requests.post(url, data={"chat_id": chat_id, "message_id": message_id}, timeout=5)
        except Exception as e:
            logger.error(f"[TelegramWS] pin_message 失敗: {e}")

    def unpin_message(self, chat_id, message_id=None):
        """取消置頂"""
        import requests
        url = f"{self.base_url}/unpinChatMessage"
        data = {"chat_id": chat_id}
        if message_id:
            data["message_id"] = message_id
        try:
            requests.post(url, data=data, timeout=5)
        except Exception as e:
            logger.error(f"[TelegramWS] unpin_message 失敗: {e}")

    def edit_message(self, chat_id, message_id, text, parse_mode="HTML"):
        """編輯訊息"""
        import requests
        url = f"{self.base_url}/editMessageText"
        data = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": parse_mode}
        try:
            resp = requests.post(url, data=data, timeout=10)
            return resp.json()
        except Exception as e:
            logger.error(f"[TelegramWS] edit_message 失敗: {e}")
            return None

    def answer_callback(self, callback_id, text=""):
        """回答 callback query"""
        import requests
        url = f"{self.base_url}/answerCallbackQuery"
        try:
            requests.post(url, data={"callback_query_id": callback_id, "text": text}, timeout=5)
        except:
            pass

    def get_chat_pinned_message(self, chat_id):
        """取得目前置頂訊息 ID"""
        import requests
        url = f"{self.base_url}/getChat"
        try:
            resp = requests.get(url, params={"chat_id": chat_id}, timeout=5).json()
            if resp.get("ok"):
                return resp["result"].get("pinned_message", {}).get("message_id")
        except:
            pass
        return None
