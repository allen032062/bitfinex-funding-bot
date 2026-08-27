# telegram_bot.py
# =============================================================================
# Telegram Funding Bot v4.0（WebSocket 重構版）
# =============================================================================
#
# 【程式用途】
#   作為 Bitfinex Funding 借貸機器人的命令列主入口，
#   負責處理所有 Telegram 指令（/start, /menu, /status, /place 等），
#   並啟動 funding_monitor、strategy_engine、bitfinex_monitor 等各模組的執行緒。
#
# 【架構模組說明】
#   telegram_ws       - Telegram Bot API WebHook/WebSocket 底層通訊
#   bitfinex_monitor  - 每用戶一條 Bitfinex Auth WS，接收 wu/fon/foc/fcn/fcc 等事件
#   funding_monitor   - 自動掛單邏輯（自動掛單、重試、停損、利息結算）
#   strategy_engine   - 策略狀態機（FRR_BOTTOM / LADDER / GRAB_WALL / WATCH）
#   market_monitor    - 市場監控（波動率、FRR、買賣牆）
#
# 【Telegram 指令說明】
#   /start            - 開戶設定精靈（引導輸入 API Key / Secret）
#   /menu             - 發送完整 Dashboard（inline keyboard）
#   /status           - 顯示目前帳戶狀態
#   /place <rate>     - 手動掛單（rate 為日利率小數）
#   /cancel           - 取消所有未成交掛單
#   /cancel_offer_<id>- 取消特定掛單（callback button）
#   /frr              - 查看當前市場 FRR
#   /auto             - 開關自動掛單
#   /settings         - 調整自動掛單參數（比例、天期等）
#   /min              - 設定最低掛單金額
#   /report           - 利息收入報表（24h / 7d）
#   /unlock <code>    - 解鎖老哥專屬指令（唯有此人可控制設備）
#
# 【安全設計】
#   - unlock 機制：老哥（ID: 920397507）需先 /unlock 才可執行資金相關指令
#   - 非老哥發送的設備控制指令一律拒絕
#   - API Key/Secret 存在 users.json，僅對應用戶本人可操作
#
# 【Multi-tenant 機制】
#   每個 Telegram chat_id（uid）有獨立的 funding_monitor 執行緒 + Bitfinex WS 連線
#   由 bitfinex_monitor._ws_clients {} / funding_monitor._monitors {} 集中管理
#
# 【快取設計（模組層級）】
#   dashboard_cache  - Dashboard 訊息文字（避免內容相同仍重發）
#   balance_cache    - 帳戶餘額快取（由 bitfinex_monitor.wu 事件更新）
#   offer_msg_map    - offer_id → Telegram message_id 對照表
#   _FRRS_CACHE      - FRR 快取（10 秒 TTL）
#
# 【與 bitfinex_monitor 的協作方式】
#   - bitfinex_monitor._start_user_ws() 啟動 WS
#   - WS 收到 wu（錢包）→ 寫入 balance_cache + 更新置頂訊息
#   - WS 收到 fon（掛單確認）→ 發送 Telegram 追蹤訊息（含取消按鈕）
#   - WS 收到 foc/fcn/fcc → 更新訊息狀態 / 結束追蹤
#   - WS 收到 on/oc（指令確認）→ 觸發 funding_monitor 的狀態同步
#
# 【利率單位說明】
#   - Bitfinex API 回傳的 rate 通常是小數形式（如 0.00031205 = 0.031205%）
#   - 年化 = 日率 × 365
#   - 顯示給用戶時要 ×100 變成百分比
#
# 【重要常數】
#   MIN_IDLE_USD = 0         - 閒置門檻（0 = 不限制，任何金額都嘗試自動掛單）
#   DEFAULT_PERIOD_DAYS = 2  - 預設借貸天期
#   HIGH_RATE_THRESHOLD     - 高利率門檻（觸發長天期 120 天）
#   LOW_RATE_THRESHOLD      - 低利率門檻（不自動掛單）
#   AUTO_CHECK_INTERVAL      - 自動掛單檢查間隔（秒）
#   SETTLE_INTERVAL          - 利息結算檢查間隔（秒）
# =============================================================================

import ccxt
import funding_monitor
import bitfinex_monitor
import market_monitor
import strategy_engine
import telegram_ws
import yaml
import time
import logging
import json
import requests
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from collections import OrderedDict

# =============================================================================
# 全域 API 鎖（防止並發 API 呼叫）
# =============================================================================
_api_lock = threading.Lock()

# 將專案根目錄加入 Python 路徑（確保可以 import utils 等模組）
sys.path.insert(0, str(Path(__file__).parent))

# =============================================================================
# Logging 設定
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/telegram_bot.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# 設定檔路徑
# =============================================================================
BOT_CONFIG = Path(__file__).parent / "config" / "settings.yaml"
USERS_FILE = Path(__file__).parent / "data" / "users.json"


# =============================================================================
# 設定檔與用戶資料載入
# =============================================================================

def load_bot_config():
    """載入 Bot 設定檔（settings.yaml）"""
    with open(BOT_CONFIG, "r") as f:
        return yaml.safe_load(f)


def load_users():
    """
    載入用戶資料（users.json）

    回傳格式：
        {"users": {"chat_id_str": {...user_data...}}}
    """
    if USERS_FILE.exists():
        with open(USERS_FILE, "r") as f:
            return json.load(f)
    return {"users": {}}


def get_user(chat_id):
    """
    依 chat_id 取得特定用戶資料

    參數：
        chat_id: Telegram 聊天 ID（int 或 str）

    回傳：
        用戶資料字典或 None
    """
    data = load_users()
    return data["users"].get(str(int(chat_id)))


def save_users(data):
    """
    將用戶資料寫入 users.json

    參數：
        data: 用戶資料字典（包含 "users" key）
    """
    with open(USERS_FILE, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# =============================================================================
# Telegram API 包裝函式
# =============================================================================

def send_message(chat_id, text, parse_mode="HTML", reply_markup=None):
    """
    發送 Telegram 訊息

    參數：
        chat_id: 目標聊天 ID
        text: 訊息內容（HTML 格式）
        parse_mode: 解析模式（預設 HTML）
        reply_markup: 回覆鍵盤（inline keyboard）

    回傳：
        Telegram API 回應
    """
    return telegram_ws.global_send_message(chat_id, text, parse_mode, reply_markup)


def send_chat_action(chat_id, action="typing"):
    """
    發送聊天狀態（"typing" 讓對方看到「正在輸入...」）

    參數：
        chat_id: 目標聊天 ID
        action: 動作類型（typing, upload_photo, ...）
    """
    telegram_ws.global_send_chat_action(chat_id, action)


def send_status_message(chat_id, text):
    """
    發送狀態訊息（用於進度更新），回傳 message_id

    參數：
        chat_id: 目標聊天 ID
        text: 訊息內容

    回傳：
        message_id 或 None
    """
    result = telegram_ws.global_send_message(chat_id, text, parse_mode="HTML")
    if result and result.get("ok"):
        return result["result"]["message_id"]
    return None


def update_status_message(chat_id, message_id, text):
    """
    編輯已發送的訊息（用於更新進度）

    參數：
        chat_id: 聊天 ID
        message_id: 要編輯的訊息 ID
        text: 新的訊息內容

    回傳：
        Telegram API 回應
    """
    return telegram_ws.global_edit_message(chat_id, message_id, text)


def answer_callback(callback_id, text=""):
    """
    回應 callback query（關閉 Telegram 按鈕的 loading 狀態）

    參數：
        callback_id: 回調 ID
        text: 顯示的 toast 文字
    """
    telegram_ws.global_answer_callback(callback_id, text)


def pin_message(chat_id, message_id, disable_notification=True):
    """
    置頂訊息

    參數：
        chat_id: 聊天 ID
        message_id: 訊息 ID
        disable_notification: 是否靜音（預設 True）
    """
    telegram_ws.global_pin_message(chat_id, message_id, disable_notification=disable_notification)


def get_pinned_message(chat_id):
    """取得置頂訊息"""
    return telegram_ws.global_get_chat_pinned_message(chat_id)


def unpin_message(chat_id, message_id=None):
    """解除置頂訊息"""
    telegram_ws.global_unpin_message(chat_id, message_id)


# =============================================================================
# Bitfinex 交易所實例建立
# =============================================================================

def create_exchange(user):
    """
    根據用戶的 API Key/Secret 建立 ccxt.bitfinex 實例

    參數：
        user: 用戶資料字典，需包含 "api_key" 和 "api_secret"

    回傳：
        ccxt.bitfinex 交易所實例
    """
    return ccxt.bitfinex({
        "apiKey": user["api_key"],
        "secret": user["api_secret"],
        "enableRateLimit": True,
    })


def _safe_api_call(func, *args, **kwargs):
    """
    包裝 API 呼叫，自動重試 3 次

    专门處理 Bitfinex 常見的 nonce 錯誤，發生時等 0.5 秒後重試

    參數：
        func: 要呼叫的函式
        *args, **kwargs: 傳給 func 的參數

    回傳：
        API 呼叫結果

    例外：
        3 次都失敗後拋出最後的例外
    """
    for attempt in range(3):
        try:
            result = func(*args, **kwargs)
            # 檢查是否為 Bitfinex 錯誤（如 nonce 問題）
            if isinstance(result, dict) and not result.get("ok"):
                err = str(result.get("description", "")) + str(result.get("error", ""))
                if "nonce" in err.lower() or "small" in err.lower():
                    logger.warning("[_safe_api_call] nonce 錯誤，等 0.5 秒後重試...")
                    time.sleep(0.5)
                    continue
            return result
        except Exception as e:
            err = str(e)
            if attempt < 2:
                if "nonce" in err.lower() or "small" in err.lower():
                    logger.warning("[_safe_api_call] nonce exception，等 0.5 秒後重試...")
                    time.sleep(0.5)
                    continue
                logger.warning(f"[_safe_api_call] API 錯誤，等 5 秒後重試: {e}")
                time.sleep(5)
                continue
            logger.error(f"[_safe_api_call] 最終失敗: {e}")
            raise
    return None


def get_balance(user):
    """
    取得 Funding 帳戶餘額（USD 和 BTC）

    參數：
        user: 用戶資料字典

    回傳：
        {"USDT": USD可用金額, "BTC": BTC可用金額}
    """
    try:
        exchange = create_exchange(user)
        balance = exchange.fetch_balance({"type": "funding"})
        return {
            "USDT": balance.get("USDT", {}).get("free", 0),
            "BTC": balance.get("BTC", {}).get("free", 0),
        }
    except Exception as e:
        logger.error(f"取得餘額失敗: {e}")
        return {"USDT": 0, "BTC": 0}


# =============================================================================
# 全域常數設定
# =============================================================================

# MIN_IDLE_USD = 0：不限制，任何金額都可自動掛單（推薦）
# 設成很大的數字如 999999 會讓自動掛單永遠不會觸發
# 建議用 user["min_amount"] 做個人化門檻
MIN_IDLE_USD = 0
DEFAULT_PERIOD_DAYS = 2    # 預設借貸天期（2 天）
HIGH_RATE_THRESHOLD = 0.20  # 高利率門檻（觸發長天期策略）
LOW_RATE_THRESHOLD = 0.01   # 低利率門檻（不自動掛單）
AUTO_CHECK_INTERVAL = 120   # 自動掛單檢查間隔（秒）
SETTLE_INTERVAL = 3600      # 利息結算檢查間隔（秒）


# =============================================================================
# FRR（Flash Return Rate）市場利率取得
# =============================================================================

# FRR 快取：避免短時間重複呼叫 Bitfinex API
_FRR_CACHE = {"daily": None, "annual": None, "ts": 0}


def fetch_frr():
    """
    從 Bitfinex v2/ticker/fUSD 取得當前 FRR 日利率

    資料來源：https://api-pub.bitfinex.com/v2/ticker/fUSD

    Bitfinex v2/ticker 回傳格式：
        [0] = FRR (Flash Return Rate) 日利率（小數形式）

    回傳：
        (frr_daily, frr_annual)
        - frr_daily: 日利率（小數，如 0.00031205）
        - frr_annual: 年化利率（百分比，如 11.39）

    注意：Bitfinex 回傳的 FRR 已經是包含%的形式，直接乘 365 即為年化
    """
    try:
        r = requests.get("https://api-pub.bitfinex.com/v2/ticker/fUSD", timeout=5)
        data = r.json()
        if data and len(data) > 0:
            frr_daily = float(data[0])          # 日利率（小數）
            frr_annual = frr_daily * 365         # 年化（已包含%，如 11.39%）
            logger.info(f"[FRR] daily={frr_daily:.6f} annual={frr_annual:.2f}%")
            return frr_daily, frr_annual
    except Exception as e:
        logger.warning(f"[FRR] fetch failed: {e}")
    return None, None


def get_market_funding_rate(exchange=None):
    """
    取得目前市場 Funding 利率（FRR）- 統一使用 v2/ticker/fUSD

    使用 10 秒快取，避免短時間重複呼叫

    參數：
        exchange: Bitfinex 交易所實例（目前未使用，保留向後相容）

    回傳：
        (frr_daily, frr_annual, ref_rate)
        - frr_daily: 日利率（小數）
        - frr_annual: 年化利率（百分比）
        - ref_rate: 同 frr_daily（保留向後相容）
    """
    global _FRR_CACHE
    now = time.time()
    # 快取 10 秒
    if _FRR_CACHE["ts"] and (now - _FRR_CACHE["ts"]) < 10:
        return _FRR_CACHE["daily"], _FRR_CACHE["annual"], _FRR_CACHE["daily"]
    daily, annual = fetch_frr()
    _FRR_CACHE = {"daily": daily, "annual": annual, "ts": now}
    return daily, annual, daily


# =============================================================================
# Multi-tenant 快取管理
# =============================================================================

# Dashboard 訊息文字快取（避免相同內容重複發送）
dashboard_cache = {}   # {chat_id_str: last_dashboard_text}

# 掛單追蹤：offer_id → Telegram message_id
offer_msg_map = {}      # {chat_id_str: {offer_id_str: message_id_int}}

# 餘額快取（由 bitfinex_monitor.wu 事件更新）
balance_cache = {}      # {chat_id_str: {"total": xxx, "avail": xxx, "frr": xxx}}


def _set_balance(chat_id, total, avail, frr=None):
    """
    寫入餘額快取

    參數：
        chat_id: 聊天 ID
        total: 總餘額（USD）
        avail: 可用餘額（USD）
        frr: 市場利率（可選）
    """
    uid = str(chat_id)
    balance_cache[uid] = {"total": total, "avail": avail, "frr": frr}


def _get_balance(chat_id):
    """從快取取得餘額"""
    return balance_cache.get(str(chat_id))


def update_user_balance_cache(chat_id, total, avail, frr=None):
    """
    更新用戶餘額快取（在錢包更新事件時呼叫）
    """
    _set_balance(chat_id, total, avail, frr)


def _cache_get(chat_id):
    """取得快取的 Dashboard 文字"""
    return dashboard_cache.get(str(chat_id))


def _cache_set(chat_id, text):
    """寫入 Dashboard 文字到快取"""
    dashboard_cache[str(chat_id)] = text


def _cache_dirty_check(chat_id, new_text):
    """
    檢查 Dashboard 文字是否有變化

    若新舊內容相同則不回傳 True（避免無意義的訊息更新）

    參數：
        chat_id: 聊天 ID
        new_text: 新的 Dashboard 文字

    回傳：
        True = 有變化，應該更新；False = 相同，跳過
    """
    old_text = _cache_get(chat_id)
    if old_text == new_text:
        logger.info(f"[Cache] uid={chat_id} 狀態無改變，跳過更新")
        return False
    _cache_set(chat_id, new_text)
    return True


def _get_offer_msg(chat_id, offer_id):
    """
    取得某個 offer 追蹤的 Telegram message_id

    參數：
        chat_id: 聊天 ID
        offer_id: 掛單 ID

    回傳：
        message_id 或 None
    """
    return offer_msg_map.get(str(chat_id), {}).get(offer_id)


def _set_offer_msg(chat_id, offer_id, msg_id):
    """
    設定某個 offer 追蹤的 Telegram message_id
    """
    uid = str(chat_id)
    if uid not in offer_msg_map:
        offer_msg_map[uid] = {}
    offer_msg_map[uid][offer_id] = msg_id


def _del_offer_msg(chat_id, offer_id):
    """
    移除某個 offer 的追蹤記錄（成交或取消時呼叫）
    """
    uid = str(chat_id)
    if uid in offer_msg_map and offer_id in offer_msg_map[uid]:
        del offer_msg_map[uid][offer_id]


# =============================================================================
# USD/TWD 匯率取得
# =============================================================================

def get_twd_rate():
    """
    取得 USD -> TWD 匯率（用 Bitfinex API）

    回傳：
        匯率浮點數（如 32.5）或 None
    """
    url = "https://api-pub.bitfinex.com/v2/calc/fx"
    payload = {"ccy1": "USD", "ccy2": "TWD"}
    headers = {"accept": "application/json", "content-type": "application/json"}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=5)
        data = response.json()
        if isinstance(data, list) and len(data) > 0:
            return float(data[0])
        return None
    except Exception as e:
        logger.error(f"[get_twd_rate] {e}")
        return None


# =============================================================================
# Funding 狀態訊息渲染（統一的訊息產生中心）
# =============================================================================

def render_funding_status(avail_usd, total_usd=None, frr="N/A", include_full_dashboard=False, user=None, unfilled_usd=None):
    """
    統一的 Funding 狀態訊息渲染中心

    所有置頂訊息、錢包更新訊息都必須透過此函式產生，確保 DRY（Don't Repeat Yourself）

    參數：
        avail_usd: 可用餘額（USD）
        total_usd: 總餘額（USD），預設與 avail_usd 相同
        frr: 市場利率（FRR），可為數字或 "N/A"
        include_full_dashboard: True=完整 Dashboard（/menu 用），False=精簡餘額卡（置頂訊息用）
        user: 用戶資料字典（可選，用於讀取 auto_place, min_amount）
        unfilled_usd: 未借出金額（USD），完整 Dashboard 時顯示

    回傳：
        HTML 格式的 Telegram 訊息文字
    """
    twd_rate = get_twd_rate()
    avail_twd = f"{float(avail_usd) * twd_rate:.0f}" if twd_rate and isinstance(avail_usd, (int, float)) else "?"
    total_twd = f"{float(total_usd or avail_usd) * twd_rate:.0f}" if twd_rate and isinstance(total_usd or avail_usd, (int, float)) else "?"
    unfilled_twd = f"{float(unfilled_usd) * twd_rate:.0f}" if twd_rate and isinstance(unfilled_usd, (int, float)) else "?"
    frr_str = f"{float(frr):.4f}%/年" if isinstance(frr, (int, float)) else str(frr)

    if include_full_dashboard:
        # 完整 Dashboard（/menu 使用）
        auto = user.get("auto_place", False) if user else False
        min_amt = user.get("min_amount", 150) if user else 150
        auto_status = "✅ 已開啟" if auto else "❌ 已關閉"
        unfilled_line = f"\n💰 未借出: <b>{unfilled_usd}</b> USD (約 NT$ {unfilled_twd})" if unfilled_usd is not None else ""
        return f"""💰 <b>融資帳戶</b>

📊 總餘額: <b>{total_usd or avail_usd}</b> USD (約 NT$ {total_twd})
💵 可用餘額: <b>{avail_usd}</b> USD (約 NT$ {avail_twd}){unfilled_line}

📈 市場利率(FRR): <b>{frr_str}</b>

⚙️ 自動掛單: <b>{auto_status}</b>
🎯 最低金額: <b>{min_amt}</b> USD

<code>點擊下方按鈕操作</code>"""
    else:
        # 精簡版：用於置頂訊息
        return f"目前融資帳戶總金額 : <b>{avail_usd:.2f}</b> USD (約 NT$ {avail_twd})"


# =============================================================================
# Inline Keyboard 按鈕建構
# =============================================================================

def build_inline_keyboard(user=None):
    """
    建立 Dashboard 的 inline keyboard 按鈕

    根據使用者狀態顯示不同按鈕：
    - 已註冊（有 api_key）: Funding / FRR / Status / 自動掛單 / 刷新
    - 新使用者（無 api_key 或失效）: 註冊

    參數：
        user: 用戶資料字典（可選）

    回傳：
        Telegram inline keyboard 格式字典
    """
    has_api = has_valid_api_key(user)

    if not has_api:
        # 新使用者：只有註冊按鈕
        return {"inline_keyboard": [[
            {"text": "📝 註冊 / 設定 API Key", "callback_data": "register"}
        ]]}

    auto = user.get("auto_place", False) if user else False
    auto_btn = "⏹ 關閉自動掛單" if auto else "▶️ 開啟自動掛單"

    keyboard = [
        # 第一排：主要功能
        [
            {"text": "💰 Funding", "callback_data": "funding"},
            {"text": "📈 FRR", "callback_data": "frr"},
            {"text": "📊 Status", "callback_data": "status"},
        ],
        # 第二排：自動掛單開關
        [{"text": auto_btn, "callback_data": "toggle_auto"}],
        # 第三排：刷新
        [{"text": "🔄 重新整理", "callback_data": "refresh"}],
    ]
    return {"inline_keyboard": keyboard}


def has_valid_api_key(user):
    """
    檢查使用者是否有有效的 API Key

    簡單判斷：api_key 和 api_secret 都至少 10 個字元

    參數：
        user: 用戶資料字典

    回傳：
        True = 有效，False = 無效或不存在
    """
    if not user:
        return False
    api_key = user.get("api_key", "")
    api_secret = user.get("api_secret", "")
    return len(api_key) >= 10 and len(api_secret) >= 10


def build_dashboard(user=None, balance_info=None):
    """
    建立主 Dashboard 訊息文字

    參數：
        user: 用戶資料字典
        balance_info: 餘額資訊（可選），格式：{"total": "...", "avail": "...", "frr": "..."}

    回傳：
        Dashboard HTML 訊息文字
    """
    if not user:
        return "❌ 尚未註冊，請使用 /start 設定 API Key"

    if balance_info:
        total = balance_info.get("total", "0")
        avail = balance_info.get("avail", "0")
        frr = balance_info.get("frr", "N/A")
    else:
        total = avail = "載入中..."
        frr = "載入中..."

    return render_funding_status(avail, total, frr, include_full_dashboard=True, user=user)


# =============================================================================
# 取得活躍 Funding 掛單
# =============================================================================

def get_active_funding_offers(exchange):
    """
    取得活躍的 Funding 掛單（排除已成交/已取消）

    Bitfinex API 回傳掛單格式（list of lists）：
        [0] = id, [2] = mts_created (ms), [5] = amount,
        [10] = status, [14] = rate, [15] = period

    索引 [10] 為 status："ACTIVE" 表示未被成交/取消

    參數：
        exchange: ccxt.bitfinex 實例

    回傳：
        活躍掛單列表
    """
    try:
        offers = exchange.private_post_auth_r_funding_offers({"symbol": "fUSD"})
        if isinstance(offers, list):
            return [o for o in offers if isinstance(o, list) and len(o) > 10 and o[10] == "ACTIVE"]
        return []
    except Exception as e:
        logger.warning(f"[get_active_funding_offers] 取得失敗: {e}")
        return []


def funding_offer_placed(chat_id, offer_data):
    """
    掛單成功回調（v4.0 已停用，保留向後相容）

    目前掛單追蹤由 bitfinex_monitor 的 fon 事件處理
    """
    pass


# =============================================================================
# 指令處理函式
# =============================================================================

def get_welcome_text(user=None):
    """
    歡迎文字（置頂訊息用）

    回傳：
        HTML 格式的歡迎訊息
    """
    twd_rate = get_twd_rate()
    example_twd = f"{1000 * twd_rate:.0f}" if twd_rate else "32,000"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    return (
        f"🤖 <b>Funding Bot</b>\n\n"
        f"💰 自動放貸機器人\n\n"
        f"📌 1,000 USD (約 NT$ {example_twd})\n\n"
        f"使用 /menu 打開主控台\n"
        f"-- 更新時間：{ts}"
    )


def _get_balance_info(chat_id, user):
    """
    主動查詢餘額和 FRR 並寫入快取

    用於：/menu、/report 等需要最新資料的場景

    參數：
        chat_id: 聊天 ID
        user: 用戶資料字典

    回傳：
        {"total": "...", "avail": "...", "frr": "..."} 或 None
    """
    try:
        exchange = create_exchange(user)

        # 取得 funding 餘額
        with _api_lock:
            balance = exchange.fetch_balance({"type": "funding"})

        total = 0.0
        avail = 0.0
        for curr, v in balance.get("USD", {}).items():
            if curr == "total":
                total = float(v)
            elif curr == "free":
                avail = float(v)

        # 取得 FRR
        frr = "N/A"
        try:
            frr_daily, frr_annual, _ = get_market_funding_rate(exchange)
            if frr_daily is not None:
                frr = f"{frr_annual:.2f}%"
        except:
            pass

        # 寫入快取
        _set_balance(chat_id, f"{total:.2f}", f"{avail:.2f}", frr)

        return {"total": f"{total:.2f}", "avail": f"{avail:.2f}", "frr": frr}
    except Exception as e:
        logger.error(f"[_get_balance_info] {e}")
        return None


def _build_dashboard_msg(user, chat_id=None):
    """
    建立 Dashboard 訊息（包含快取查詢）

    邏輯：
    1. 先查快取，若有完整資料且非 "?" → 直接使用
    2. 若快取缺失或為 "?" → 主動查詢 REST API
    3. 若完全沒有快取 → 主動查詢

    參數：
        user: 用戶資料字典
        chat_id: 聊天 ID（可選）

    回傳：
        Dashboard HTML 訊息文字
    """
    uid = str(chat_id) if chat_id else (user.get("chat_id") or "")
    cached = _get_balance(uid)

    if cached:
        total = cached.get("total", "0")
        avail = cached.get("avail", "0")
        frr = cached.get("frr", "N/A")
        # 快取 miss：「?」→ 主動查詢 REST API
        if total == "?" or avail == "?" or not total or not avail:
            balance_info = _get_balance_info(int(uid), user)
            if balance_info:
                total = balance_info.get("total", "?")
                avail = balance_info.get("avail", "?")
                frr = balance_info.get("frr", "N/A")
    else:
        # 完全沒有快取，主動查詢
        balance_info = _get_balance_info(int(uid), user)
        if balance_info:
            total = balance_info.get("total", "?")
            avail = balance_info.get("avail", "?")
            frr = balance_info.get("frr", "N/A")
        else:
            total = avail = "載入中..."
            frr = "N/A"

    return render_funding_status(avail, total, frr, include_full_dashboard=True, user=user)


# =============================================================================
# /menu 指令：發送 Dashboard
# =============================================================================

def cmd_menu(chat_id):
    """Dashboard /menu：發送完整功能面板"""
    users_data = load_users()
    uid = str(chat_id)

    if uid not in users_data["users"]:
        send_message(chat_id, get_welcome_text())
        return

    user = users_data["users"][uid]

    # 建立新訊息
    new_text = _build_dashboard_msg(user, chat_id)

    # Dirty check：若內容相同則跳過
    if not _cache_dirty_check(chat_id, new_text):
        return

    keyboard = build_inline_keyboard(user)
    send_message(chat_id, new_text, reply_markup=keyboard)


# =============================================================================
# 置頂訊息管理
# =============================================================================

def ensure_pinned_welcome(chat_id, user=None):
    """
    確保使用者有置頂訊息，並將 ID 持久化到 users.json

    訊息內容：錢包狀態（非 welcome 文字），
    這樣使用者打開 Telegram 就能看到即時帳戶總額。

    邏輯（完全繞過 getChat API）：
      1. 從 user.pin_message_id 讀取已知 ID
      2. 驗證訊息是否存在（getMessage）
         - 存在 → 直接 edit 錢包狀態，回傳該 ID
         - 不存在 → 發新訊息（錢包狀態）→ 置頂 → 回寫 users.json

    參數：
        chat_id: 聊天 ID
        user: 用戶資料字典

    回傳：
        置頂訊息的 message_id 或 None
    """
    if not user:
        return None

    uid = str(chat_id)
    known_id = user.get("pin_message_id")

    # 抓真實錢包狀態當作置頂訊息內容
    status_text = _get_pinned_status_text(chat_id, user)
    reply_markup = build_inline_keyboard(user)

    # 策略：直接用已知 ID edit，失敗了才重建
    if known_id:
        result = telegram_ws.global_edit_message(
            chat_id, known_id, status_text,
            parse_mode="HTML", reply_markup=reply_markup
        )
        if result and result.get("ok"):
            logger.info(f"[Pin] uid={uid} 使用現有 ID={known_id} 更新內容")
            return known_id
        logger.warning(f"[Pin] uid={uid} ID={known_id} edit 失敗，將重建")

    # 建立新訊息（錢包狀態）→ 置頂 → 回寫
    welcome_msg = telegram_ws.global_send_message(
        chat_id, status_text, parse_mode="HTML",
        reply_markup=reply_markup
    )
    if welcome_msg and welcome_msg.get("ok"):
        new_msg_id = welcome_msg["result"]["message_id"]
        telegram_ws.global_pin_message(chat_id, new_msg_id, disable_notification=True)

        # 回寫 users.json
        users_data = load_users()
        users_data["users"][uid]["pin_message_id"] = new_msg_id
        save_users(users_data)
        logger.info(f"[Pin] uid={uid} 建立並置頂新訊息 ID={new_msg_id}")
        return new_msg_id

    logger.error(f"[Pin] uid={uid} 建立置頂訊息失敗")
    return None


def _get_pinned_status_text(chat_id, user):
    """
    取得置頂訊息的錢包狀態文字

    供 ensure_pinned_welcome 和 bitfinex_monitor.on_wallet 共用

    參數：
        chat_id: 聊天 ID
        user: 用戶資料字典

    回傳：
        HTML 訊息文字
    """
    try:
        exchange = _safe_api_call(create_exchange, user)
        if not exchange:
            return f"❌ 無法連線 Bitfinex"

        balance_f = 0.0
        avail_f = 0.0
        try:
            bal = exchange.fetch_balance()
            if bal and bal.get("free", {}).get("USD") is not None:
                avail_f = float(bal["free"]["USD"])
            if bal and bal.get("total", {}).get("USD") is not None:
                balance_f = float(bal["total"]["USD"])
        except Exception as e:
            logger.warning(f"[_get_pinned_status_text] 余額取得失敗: {e}")

        return render_funding_status(avail_f, balance_f, include_full_dashboard=False)
    except Exception as e:
        logger.error(f"[_get_pinned_status_text] uid={chat_id}: {e}")
        return f"❌ 讀取失敗: {e}"


# =============================================================================
# /help 指令
# =============================================================================

def cmd_help(chat_id):
    """說明指令：顯示所有可用指令列表"""
    msg = (
        "🤖 <b>Funding Bot</b>\n\n"
        "💰 /funding - Funding 狀態\n"
        "📈 /frr - 市場利率（FRR）\n"
        "📊 /status - 帳戶狀態\n"
        "📝 /register - 註冊 API Key\n"
        "⚙️ /auto - 自動掛單開關\n"
        "💰 /min - 設定最低金額\n"
        "📊 /report - 利息收入報表\n"
        "/help - 說明"
    )
    send_message(chat_id, msg, reply_markup=build_inline_keyboard())


# =============================================================================
# /funding 指令：Funding 狀態詳細資訊
# =============================================================================

def cmd_funding(chat_id):
    """
    Funding 狀態指令 /funding

    顯示：
    - 帳戶總額、出借中、等待成交
    - 市場利率（日率、年化）
    - 掛牌策略建議（利率、天期）
    - 所有出借中（credits）和掛單中（offers）的明細
    - 日收益、月收益預估
    - 過去 30 天利息收入統計
    """
    msg_id = send_status_message(chat_id, "⏳ <b>夥計分析中...</b>\n🔄 讀取帳戶資料中...")

    try:
        user = get_user(chat_id)
        if not user:
            final = "❌ 尚未設定，請先使用 /register 註冊 API Key"
            if msg_id:
                update_status_message(chat_id, msg_id, final)
            else:
                send_message(chat_id, final, reply_markup=build_inline_keyboard())
            return

        exchange = _safe_api_call(create_exchange, user)
        if not exchange:
            final = "FAIL: cannot connect"
            if msg_id:
                update_status_message(chat_id, msg_id, final)
            else:
                send_message(chat_id, final, reply_markup=build_inline_keyboard())
            return

        if msg_id:
            update_status_message(chat_id, msg_id, "⏳ <b>夥計分析中...</b>\n✅ 帳戶讀取完成\n🔄 查詢 Funding 資料中...")

        # 取得市場利率
        frr_daily, frr_annual, ref_rate = get_market_funding_rate(exchange)

        # 動態計算掛牌利率與天期
        from funding_logic import get_initial_rate, get_period, MIN_RATE_ANNUAL

        offer_rate_daily = get_initial_rate(frr_annual / 100)    # 小數形式
        offer_rate_annual = offer_rate_daily * 365                # 年化（百分比）
        period_days = get_period(offer_rate_annual / 100)        # 天期（需除 100 轉小數）

        # 計算實際使用的利率
        if ref_rate:
            actual_offer_rate_pct = ref_rate * 1.2 * 100
        else:
            actual_offer_rate_pct = offer_rate_daily * 100

        rate_label = "FRR（低利率環境）" if (frr_annual and frr_annual < 1.0) else "FRR x 1.2"

        with _api_lock:
            credits = exchange.private_post_auth_r_funding_credits({'symbol': 'fUSD'})
            offers = exchange.private_post_auth_r_funding_offers({'symbol': 'fUSD'})
            bal = exchange.fetch_balance({"type": "funding"})

        # 計算總額（使用錢包真實總額，而非 credits+offers 的組合）
        total_lent = sum(float(c[5]) for c in credits if c[5])
        total_offer = sum(float(o[5]) for o in offers if o[5])
        wallet_info = bal.get('total', {}) if isinstance(bal, dict) else {}
        total_funding = float(wallet_info.get('USD', 0)) if wallet_info else (total_lent + total_offer)
        # 未借出 = 錢包總額 - 已借出 - 等待成交（錢包裡還沒掛出去的現貨）
        unfilled = max(0.0, total_funding - total_lent - total_offer)

        # 利率轉換（顯示給用戶時乘 100）
        frr_daily_pct = (frr_daily * 100) if frr_daily else 0
        frr_annual_pct = frr_annual if frr_annual else 0  # fetch_frr 已回傳百分比
        daily_net = sum(float(c[5]) * float(c[11]) * 0.85 for c in credits if c[5] and c[11])

        msg = (
            "📊 <b>Funding 狀態</b>\n\n"
            "💰 <b>帳戶</b>\n"
            f"  總額: ${total_funding:.6f} USD\n"
            f"  出借中: ${total_lent:.6f} USD\n"
            f"  等待成交: ${total_offer:.6f} USD\n"
            f"  未借出: ${unfilled:.6f} USD\n\n"
            "📈 <b>市場利率</b>\n"
            f"  日利率: {frr_daily_pct:.4f}%/天\n"
            f"  年化 (APR): {frr_annual_pct:.2f}%\n\n"
            "⚙️ <b>掛牌策略</b>\n"
            f"  利率: {offer_rate_annual:.4f}% APR ({offer_rate_daily:.4f}%/天)\n"
            f"  天期: {period_days} 天\n"
            f"  ── Debug ──\n"
            f"  FRR年化: {frr_annual_pct:.4f}%\n"
            f"  最低利率: {MIN_RATE_ANNUAL*100:.2f}% APR\n"
            f"  初始利率: {offer_rate_daily:.6f} (日利率)\n"
            f"  天期判斷: 年化 {offer_rate_annual:.2f}% {'>14.5%→120天' if period_days == 120 else '>12%→7天' if period_days == 7 else '≤12%→2天'}\n"
        )

        # 顯示出借中（已匹配）的掛單明細
        if credits:
            msg += "🔴 <b>出借中（已匹配）</b>\n"
            for c in credits:
                amount = float(c[5])
                rate_pct = float(c[11]) * 100     # 轉換為百分比顯示
                period = int(c[12]) if c[12] else 0
                # [3] = created (ms)，到期 = [3] + period * 86400000
                ts_maturity_ms = int(c[3]) + period * 86400000 if c[3] else 0
                dt_maturity = datetime.fromtimestamp(ts_maturity_ms / 1000, tz=timezone.utc).astimezone(None)
                msg += (f"  <code>{c[0]}</code> | {amount:.2f} USD | "
                        f"{rate_pct:.4f}%/天({rate_pct*365:.2f}%/年) | {period}天 | "
                        f"到期 {dt_maturity.strftime('%m/%d %H:%M')}\n")
            msg += f"  ➤ <b>出借中合計: ${total_lent:.6f} USD</b>\n\n"
        else:
            msg += "🔴 <b>出借中</b>: 無\n\n"

        # 顯示等待成交的掛單明細
        if offers:
            msg += "🟡 <b>等待被借（掛單中）</b>\n"
            for o in offers:
                amount = float(o[5])
                rate_pct = float(o[14]) * 100 if o[14] else 0  # 轉換為百分比顯示
                period = int(o[15]) if o[15] else 0
                msg += (f"  <code>{o[0]}</code> | {amount:.2f} USD | "
                        f"{rate_pct:.4f}%/天({rate_pct*365:.2f}%/年) | {period}天\n")
            msg += f"  ➤ <b>等待合計: ${total_offer:.6f} USD</b>\n\n"
        else:
            msg += "🟡 <b>等待被借</b>: 無\n\n"

        # 日/月收益預估
        if daily_net > 0:
            msg += f"📌 <b>日收益</b>: ~${daily_net:.6f} USD\n"
            msg += f"📌 <b>月收益</b>: ~${daily_net*30:.6f} USD\n"

        # 過去 30 天利息統計
        try:
            since_ms = int((time.time() - 30 * 86400) * 1000)
            with _api_lock:
                ledgers = exchange.private_post_auth_r_ledgers_hist({
                    'currency': 'USD',
                    'start': since_ms,
                    'limit': 500,
                })
            funding_payments = [l for l in (ledgers or []) if 'Margin Funding Payment' in str(l)]
            total_30d_net = sum(float(l[5]) for l in funding_payments)

            if funding_payments:
                timestamps = []
                for l in funding_payments:
                    try:
                        ts_raw = int(l[3])
                        # 判斷時間戳單位（ms 或 s）
                        if ts_raw > 1e12:  # 大於 1 兆視為毫秒
                            ts_sec = ts_raw / 1000
                        else:
                            ts_sec = ts_raw
                        timestamps.append(ts_sec)
                    except:
                        pass
                earliest_ts = min(timestamps)
                earliest_dt = datetime.fromtimestamp(earliest_ts, tz=timezone.utc).astimezone(None)
                date_range = earliest_dt.strftime('%m/%d')
            else:
                date_range = "無資料"

            count = len(funding_payments)
            msg += f"\n📅 <b>過去30天利息收益</b>\n"
            msg += f"從 {date_range} 計算, 共 {count} 筆, 總數為 ${total_30d_net:.2f} USD\n"
        except Exception as e:
            logger.warning(f"[Funding] 30d income fetch failed: {e}")

        if msg_id:
            update_status_message(chat_id, msg_id, "⏳ <b>夥計分析中...</b>\n✅ 帳戶讀取完成\n✅ Funding 資料讀取完成\n🔄 整理輸出中...")

        logger.info(f"[Funding] 開始整理輸出, credits={len(credits)}, offers={len(offers)}")
        if msg_id:
            update_status_message(chat_id, msg_id, "⏳ <b>夥計分析中...</b>\n✅ 帳戶讀取完成\n✅ Funding 資料讀取完成\n✅ 整理完成，發送中...")

        logger.info(f"[Funding] 準備發送最終訊息, msg_id={msg_id}, msg_len={len(msg)}")
        try:
            result = update_status_message(chat_id, msg_id, msg)
            logger.info(f"[Funding] 發送結果: {result}")
        except Exception as e:
            logger.error(f"[Funding] 發送失敗，改用send_message: {e}")
            send_message(chat_id, msg, reply_markup=build_inline_keyboard())

    except Exception as e:
        logger.error(f"cmd_funding error: {e}")
        if msg_id:
            update_status_message(chat_id, msg_id, f"❌ 發生錯誤:\n{str(e)[:100]}")
        else:
            send_message(chat_id, f"FAIL: {str(e)[:100]}", reply_markup=build_inline_keyboard())


# =============================================================================
# /status 指令：帳戶狀態摘要
# =============================================================================

def cmd_status(chat_id):
    """顯示帳戶 Funding 餘額（精簡版）"""
    try:
        user = get_user(chat_id)
        if not user:
            send_message(chat_id, "❌ 尚未設定，請先使用 /register 註冊", reply_markup=build_inline_keyboard())
            return

        balance = get_balance(user)
        msg = (
            f"📊 <b>帳戶狀態</b>\n\n"
            f"💰 閒置 USD: ${balance['USDT']:.6f}\n"
            f"💰 閒置 BTC: {balance['BTC']:.8f}\n\n"
            f"按「💰 Funding」查看完整狀態"
        )
        send_message(chat_id, msg, reply_markup=build_inline_keyboard())
    except Exception as e:
        logger.error(f"cmd_status error: {e}")
        send_message(chat_id, f"FAIL: {str(e)[:100]}", reply_markup=build_inline_keyboard())


# =============================================================================
# /auto 指令：自動掛單開關
# =============================================================================

def cmd_auto(chat_id, args=None):
    """
    自動掛單開關 /auto

    用法：
    /auto           → 切換目前狀態
    /auto on        → 開啟
    /auto off       → 關閉
    /auto 開啟/關閉 → 中文支援
    """
    users_data = load_users()
    uid = str(chat_id)
    user = users_data["users"].get(uid)

    if not user:
        send_message(chat_id, "❌ 尚未設定，請先 /register", reply_markup=build_inline_keyboard())
        return

    current = user.get("auto_place", False)

    if args and args[0].lower() in ("on", "1", "true", "開", "開啟"):
        new_state = True
        action = "開啟"
    elif args and args[0].lower() in ("off", "0", "false", "關", "關閉"):
        new_state = False
        action = "關閉"
    else:
        new_state = not current
        action = "開啟" if new_state else "關閉"

    user["auto_place"] = new_state
    save_users(users_data)

    status = "✅ 已" if new_state else "❌ 已"
    send_message(
        chat_id,
        f"{status}{action}自動掛單\n\n"
        f"最低金額: {user.get('min_amount', 150)} USD",
        reply_markup=build_inline_keyboard(user)
    )


# =============================================================================
# /min 指令：設定最低金額
# =============================================================================

def cmd_min_amount(chat_id, args=None):
    """
    設定最低掛單金額 /min

    用法：
    /min            → 查看目前設定
    /min 200        → 設定為 200 USD
    """
    users_data = load_users()
    uid = str(chat_id)
    user = users_data["users"].get(uid)

    if not user:
        send_message(chat_id, "❌ 尚未設定，請先 /register", reply_markup=build_inline_keyboard())
        return

    if not args:
        current = user.get("min_amount", 150)
        send_message(
            chat_id,
            f"📊 目前的最低金額設定為 <b>{current}</b> USD\n\n"
            f"請輸入：<code>/min 金額</code>\n"
            f"例如：<code>/min 200</code>",
            reply_markup=build_inline_keyboard(user)
        )
        return

    try:
        amount = float(args[0])
        if amount < 0:
            raise ValueError("金額不能為負數")
        user["min_amount"] = amount
        save_users(users_data)
        send_message(
            chat_id,
            f"✅ 最低金額已更新為 <b>{amount}</b> USD",
            reply_markup=build_inline_keyboard(user)
        )
    except ValueError as e:
        send_message(
            chat_id,
            f"❌ 金額格式錯誤：{e}\n\n請輸入正確的數字，例如：<code>/min 200</code>",
            reply_markup=build_inline_keyboard(user)
        )


# =============================================================================
# /register 指令：註冊 API Key
# =============================================================================

def cmd_register(chat_id, args=None):
    """
    註冊 API Key /register

    用法：
    /register YOUR_API_KEY YOUR_API_SECRET

    流程：
    1. 檢查參數格式
    2. 用 API Key/Secret 建立交易所並驗證（fetch_balance）
    3. 若驗證成功，寫入 users.json
    4. 建立置頂訊息
    """
    if not args or len(args) < 2:
        msg = (
            "📝 <b>註冊 API Key</b>\n\n"
            "請依照以下格式傳送：\n"
            "<code>/register YOUR_API_KEY YOUR_API_SECRET</code>\n\n"
            "例如：\n"
            "<code>/register abc123 def456</code>\n\n"
            "⚠️ 請至 Bitfinex 建立 Funding 專用的 API Key（僅需「讀取」權限）"
        )
        send_message(chat_id, msg, reply_markup=build_inline_keyboard())
        return

    api_key = args[0].strip()
    api_secret = args[1].strip()

    if len(api_key) < 10 or len(api_secret) < 10:
        send_message(chat_id, "❌ API Key 或 Secret 長度不足，請確認後重新輸入", reply_markup=build_inline_keyboard())
        return

    try:
        # 驗證 API Key/Secret 是否有效
        test_exchange = ccxt.bitfinex({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
        })
        test_exchange.fetch_balance({"type": "funding"})
        logger.info(f"[Register] api_key validated for chat_id={chat_id}")
    except Exception as e:
        logger.error(f"[Register] API validation failed: {e}")
        send_message(
            chat_id,
            f"❌ API Key 驗證失敗：{str(e)[:100]}\n\n"
            "請確認 API Key 和 Secret 是否正確",
            reply_markup=build_inline_keyboard()
        )
        return

    # 驗證成功，寫入 users.json
    users_data = load_users()
    users_data["users"][str(chat_id)] = {
        "chat_id": str(chat_id),
        "api_key": api_key,
        "api_secret": api_secret,
        "exchange": "bitfinex",
        "enabled": True,
        "created_at": datetime.now().strftime("%Y-%m-%d"),
        "pin_message_id": None,
    }
    save_users(users_data)

    send_message(
        chat_id,
        "✅ <b>註冊成功！</b>\n\n"
        f"Telegram ID: <code>{chat_id}</code>\n"
        "已設定 Bitfinex API Key\n\n"
        "現在可以使用 /funding 查看 Funding 狀態",
        reply_markup=build_inline_keyboard()
    )

    # 註冊完成後建立/更新置頂訊息
    user = get_user(chat_id)
    if user:
        ensure_pinned_welcome(chat_id, user)


# =============================================================================
# /market 指令：市場行情
# =============================================================================

def get_market_monitor():
    """取得 MarketMonitor singleton 實例"""
    from funding_monitor import _market_monitor
    return _market_monitor


def cmd_market(chat_id):
    """
    顯示市場行情（BTC / FRR / 訂單簿牆）

    包含：比特幣價格、波動率、FRR、訂單簿 top wall
    """
    try:
        mon = get_market_monitor()
        if not mon:
            send_message(chat_id, "❌ MarketMonitor 尚未初始化", reply_markup=build_inline_keyboard())
            return

        snap = mon.get_snapshot()
        frr_daily = snap.frr_rate
        frr_annual = snap.frr_annual
        btc = snap.btc_price
        vol = snap.volatility * 100           # 轉換為百分比
        top_wall = snap.top_wall_rate
        top_amt = snap.top_wall_amount
        side = snap.top_wall_side

        frr_daily_pct = frr_daily * 100 if frr_daily else 0
        frr_annual_pct = frr_annual if frr_annual else 0
        top_wall_pct = top_wall * 100 if top_wall else 0

        side_emoji = "📈" if side == "bid" else "📉"

        msg = (
            f"📊 <b>市場行情</b>\n\n"
            f"<b>比特幣 BTC</b>\n"
            f"  價格: ${btc:,.0f}\n"
            f"  波動率: {vol:.1f}%\n\n"
            f"<b>Funding FRR</b>\n"
            f"  日利率: {frr_daily_pct:.4f}%\n"
            f"  年化 APR: {frr_annual_pct:.2f}%\n\n"
            f"<b>訂單簿牆</b>\n"
            f"  {side_emoji} Top Wall: {top_wall_pct:.4f}% ({top_amt:.0f} USD)\n\n"
            f"<code>FRR 來源: Bitfinex v2/ticker/fUSD [0]</code>"
        )
        send_message(chat_id, msg, reply_markup=build_inline_keyboard())
    except Exception as e:
        logger.error(f"cmd_market error: {e}")
        send_message(chat_id, f"FAIL: {str(e)[:100]}", reply_markup=build_inline_keyboard())


# =============================================================================
# /frr 指令：市場利率
# =============================================================================

def cmd_frr(chat_id):
    """
    顯示市場利率 FRR 並給出建議掛牌策略

    策略邏輯：
    - 年化 >= 20% → 掛 30 天長單
    - 年化 <= 1% → 直接掛 FRR 不加成（低利率環境）
    - 其他 → 掛 FRR x 1.2
    """
    try:
        user = get_user(chat_id)
        if not user:
            send_message(chat_id, "❌ 尚未設定", reply_markup=build_inline_keyboard())
            return

        frr_daily, frr_annual, ref_rate = get_market_funding_rate(None)

        if frr_daily is None:
            send_message(chat_id, "❌ 無法取得市場利率", reply_markup=build_inline_keyboard())
            return

        # 顯示時乘 100 變百分比
        frr_daily_pct = frr_daily * 100
        frr_annual_pct = frr_annual  # fetch_frr 已回傳百分比

        if frr_annual >= 20:
            period = 30
            strategy = "高利率 > 20% → 掛 30 天長單"
        elif frr_annual <= 1:
            period = 2
            strategy = "低利率 < 1% → 直接掛 FRR 不加成"
        else:
            period = 2
            strategy = "適中利率 → 掛 FRR x 1.2"

        msg = (
            f"📈 <b>市場利率 FRR</b>\n\n"
            f"  日利率: {frr_daily_pct:.4f}%/天\n"
            f"  年化 APR: {frr_annual_pct:.2f}%\n\n"
            f"⚙️ <b>建議掛牌策略</b>\n"
            f"  天期: {period} 天\n"
            f"  {strategy}\n\n"
            f"按「💰 Funding」查看帳戶狀態"
        )
        send_message(chat_id, msg, reply_markup=build_inline_keyboard())
    except Exception as e:
        logger.error(f"cmd_frr error: {e}")
        send_message(chat_id, f"FAIL: {str(e)[:100]}", reply_markup=build_inline_keyboard())


# =============================================================================
# 利息收入報表輔助函式
# =============================================================================

def _fetch_ledger_income(exchange, since_ms, limit=500):
    """
    批次取得 ledger 記錄（支援分頁）

    Bitfinex ledgers API 有分頁限制，一次最多取 limit 筆，
    需要自己處理分頁抓取

    參數：
        exchange: 交易所實例
        since_ms: 起始時間（毫秒）
        limit: 每批數量上限

    回傳：
        所有 ledger entries 的列表
    """
    end_ms = int(time.time() * 1000)
    all_entries = []
    start = since_ms
    while True:
        batch = exchange.private_post_auth_r_ledgers_hist({
            'currency': 'USD',
            'start': start,
            'limit': limit,
            'end': end_ms,
        })
        if not batch:
            break
        all_entries.extend(batch)
        if len(batch) < limit:
            break
        # 取下一批：從最後一筆記錄的時間戳繼續
        start = int(batch[-1][3]) + 1
    return all_entries


def _filter_funding_income(entries):
    """
    從 ledger entries 中過濾出 Margin Funding Payment（利息收入）

    過濾條件：
    - description 包含 'Margin Funding Payment'
    - 金額 > 0（表示你收到的利息，不是支出）
    - wallet 為 'funding'（資金帳戶）

    Ledger entry 格式：
        [3] = timestamp (ms)
        [5] = amount（正數 = 收入，負數 = 支出）
        [7] = balance（結餘）
        [8] = description

    參數：
        entries: ledger entries 列表

    回傳：
        利息收入條目列表：[{'ts': ..., 'amount': ..., 'balance': ..., 'desc': ...}]
    """
    income_entries = []
    for e in entries:
        try:
            desc = str(e[8]) if e[8] else ""
            amt = float(e[5]) if e[5] is not None else 0
            if 'Margin Funding Payment' in desc and amt > 0:
                ts = int(e[3]) / 1000  # 毫秒 → 秒
                income_entries.append({
                    'ts': ts,
                    'amount': amt,
                    'balance': float(e[7]) if e[7] is not None else 0,
                    'desc': desc,
                })
        except Exception:
            continue
    return income_entries


# =============================================================================
# /report 指令：利息收入報表
# =============================================================================

# 報表鎖（防止並發執行）
_report_lock = threading.Lock()


def cmd_report(chat_id):
    """
    利息收入報表 /report 或 /profit

    顯示：
    - 過去 24 小時利息筆數和總收入
    - 過去 7 天利息筆數和總收入
    - 利息描述範例（Raw Data）
    """
    # 防止並發：上一次還在跑就忽略
    if not _report_lock.acquire(blocking=False):
        logger.warning("[cmd_report] 上一次執行尚未完成，跳過")
        return

    msg_id = send_status_message(chat_id, "⏳ <b>產出利息報表中...</b>\n🔄 取得 Ledger 資料...")

    try:
        user = get_user(chat_id)
        if not user:
            final = "❌ 尚未設定，請先使用 /register"
            if msg_id:
                update_status_message(chat_id, msg_id, final)
            else:
                send_message(chat_id, final, reply_markup=build_inline_keyboard())
            return

        exchange = _safe_api_call(create_exchange, user)
        if not exchange:
            final = "FAIL: cannot connect exchange"
            if msg_id:
                update_status_message(chat_id, msg_id, final)
            else:
                send_message(chat_id, final)
            return

        now_ts = time.time()
        ts_24h = now_ts - 86400
        ts_7d = now_ts - 7 * 86400

        if msg_id:
            update_status_message(chat_id, msg_id, "⏳ <b>產出利息報表中...</b>\n✅ Ledger 取得成功\n🔄 過濾利息資料...")

        # 一次拉回 7 天資料
        all_entries = _fetch_ledger_income(exchange, since_ms=int(ts_7d * 1000))

        if msg_id:
            update_status_message(chat_id, msg_id, "⏳ <b>產出利息報表中...</b>\n✅ Ledger 取得成功\n✅ 利息資料過濾中...\n🔄 整理輸出...")

        income_entries = _filter_funding_income(all_entries)

        # 分組：24h vs 7d
        entries_24h = [e for e in income_entries if e['ts'] >= ts_24h]
        entries_7d = income_entries  # 全部就是 7d

        total_24h = sum(e['amount'] for e in entries_24h)
        total_7d = sum(e['amount'] for e in entries_7d)

        # Raw data sample（第一筆利息紀錄）
        raw_sample = "無利息紀錄"
        if income_entries:
            e = income_entries[0]
            dt = datetime.fromtimestamp(e['ts'], tz=timezone.utc).astimezone(None).strftime('%Y-%m-%d %H:%M')
            raw_sample = f"ts={dt} | amount={e['amount']:.6f} | desc={repr(e['desc'][:60])}"

        if msg_id:
            update_status_message(chat_id, msg_id, "✅ 整理完成，傳送報表...")

        msg = (
            "📊 <b>Funding 利息收入報表</b>\n\n"
            f"📅 <b>過去 24 小時</b>\n"
            f"  利息筆數: {len(entries_24h)}\n"
            f"  利息總收入: <b>${total_24h:.6f}</b> USD\n\n"
            f"📅 <b>過去 7 天</b>\n"
            f"  利息筆數: {len(entries_7d)}\n"
            f"  利息總收入: <b>${total_7d:.6f}</b> USD\n\n"
            "──────────────────────────\n"
            "🔎 <b>利息描述範例（Raw Data）</b>\n"
            f"<code>{raw_sample}</code>\n\n"
            "📌 備註：含 3 分鐘快速還款產生的利息"
        )

        if msg_id:
            update_status_message(chat_id, msg_id, msg)
        else:
            send_message(chat_id, msg, reply_markup=build_inline_keyboard())

    except Exception as e:
        logger.error(f"cmd_report error: {e}")
        if msg_id:
            update_status_message(chat_id, msg_id, f"❌ 發生錯誤:\n{str(e)[:100]}")
        else:
            send_message(chat_id, f"FAIL: {str(e)[:100]}")
    finally:
        _report_lock.release()


# =============================================================================
# 訊息路由
# =============================================================================

def handle_callback(chat_id, message_id, callback_id, data):
    """
    處理 inline keyboard callback

    支援的 callback_data：
    - funding, frr, status, toggle_auto, register, refresh
    - cancel_offer_<id>: 取消特定掛單
    """
    users_data = load_users()
    uid = str(chat_id)
    user = users_data["users"].get(uid)

    if data == "funding":
        answer_callback(callback_id, text="💰 查詢 Funding 狀態中...")
        if user:
            cmd_funding(chat_id)

    elif data == "frr":
        answer_callback(callback_id, text="📈 查詢 FRR 中...")
        cmd_frr(chat_id)

    elif data == "status":
        answer_callback(callback_id, text="先不實作")

    elif data == "toggle_auto":
        if user:
            current = user.get("auto_place", False)
            user["auto_place"] = not current
            save_users(users_data)
            new_text = _build_dashboard_msg(user)
            if _cache_dirty_check(chat_id, new_text):
                keyboard = build_inline_keyboard(user)
                telegram_ws.global_edit_message(chat_id, message_id, new_text, reply_markup=keyboard)
            else:
                telegram_ws.global_edit_message(chat_id, message_id, new_text, reply_markup=build_inline_keyboard(user))

    elif data == "register":
        answer_callback(callback_id, text="📝 請使用 /start 註冊")

    elif data == "refresh":
        if user:
            new_text = _build_dashboard_msg(user)
            if _cache_dirty_check(chat_id, new_text):
                keyboard = build_inline_keyboard(user)
                telegram_ws.global_edit_message(chat_id, message_id, new_text, reply_markup=keyboard)
            else:
                telegram_ws.global_edit_message(chat_id, message_id, new_text, reply_markup=build_inline_keyboard(user))

    elif data.startswith("cancel_offer_"):
        offer_id = data.replace("cancel_offer_", "")
        answer_callback(callback_id, text=f"⏳ 正在取消 offer {offer_id}...")
        try:
            ws = bitfinex_monitor.get_ws_client(uid)
            if ws:
                ws.send_offer_cancel(offer_id)
                telegram_ws.global_edit_message(
                    chat_id, message_id,
                    f"❌ <b>取消請求已發送</b>\n\nOffer ID: <code>{offer_id}</code>\n狀態: 等待 Bitfinex 確認...",
                    reply_markup=build_inline_keyboard(user)
                )
            else:
                answer_callback(callback_id, text="❌ 找不到 WS 連線，請稍後再試")
        except Exception as e:
            answer_callback(callback_id, text=f"❌ 取消失敗: {str(e)[:100]}")


def handle_message(chat_id, text):
    """
    處理一般文字訊息（非指令）

    目前只用於按鈕點擊發送的文字（如「💰 Funding」）
    """
    logger.info(f"[handle_message] chat_id={chat_id} text={repr(text)}")
    if text == "💰 Funding":
        logger.info(f"[handle_message] matching 💰 Funding -> call cmd_funding")
        cmd_funding(chat_id)
    elif text == "📝 註冊":
        cmd_register(chat_id)
    elif text == "📊 狀態":
        cmd_status(chat_id)
    elif text == "📈 利率":
        cmd_frr(chat_id)
    elif text.lower() in ["/start", "/help"]:
        cmd_help(chat_id)


def handle_command(chat_id, command, args=None):
    """
    指令路由分發

    支援的指令：
    /start, /help, /funding, /status, /frr, /market, /menu,
    /register, /report, /auto, /min, /自動掛單, /最低金額
    """
    command = command.lower()
    if command in ["/start", "/help"]:
        cmd_help(chat_id)
    elif command == "/funding":
        cmd_funding(chat_id)
    elif command == "/status":
        cmd_status(chat_id)
    elif command == "/frr":
        cmd_frr(chat_id)
    elif command == "/market":
        cmd_market(chat_id)
    elif command == "/menu":
        cmd_menu(chat_id)
    elif command == "/register":
        cmd_register(chat_id, args)
    elif command == "/report" or command == "/profit":
        cmd_report(chat_id)
    elif command == "/auto":
        cmd_auto(chat_id, args)
    elif command == "/min":
        cmd_min_amount(chat_id, args)
    elif command == "/自動掛單":
        cmd_auto(chat_id, args or "on")
    elif command == "/最低金額":
        if args:
            cmd_min_amount(chat_id, args)


# =============================================================================
# Telegram Update 處理
# =============================================================================

class _BoundedCallbackSet:
    """
    執行緒安全的「有界集合」（Bounded Set）

    用途：追蹤已處理過的 callback_id，防止 Telegram 重複送同一個 callback

    特性：
    - 超過 maxsize 時自動淘汰最舊的 entry
    - 內部使用 OrderedDict 維護插入順序
    """
    def __init__(self, maxsize=10000):
        self._maxsize = maxsize
        self._data = {}
        self._lock = threading.Lock()

    def add(self, item):
        with self._lock:
            # 移到最末（最新）
            self._data[item] = None
            # 淘汰最舊的（OrderedDict 的第一個）
            while len(self._data) > self._maxsize:
                self._data.pop(next(iter(self._data)))

    def __contains__(self, item):
        with self._lock:
            return item in self._data

    def __len__(self):
        with self._lock:
            return len(self._data)


# 已處理過的 callback_id（防止 Telegram 重複送）
PROCESSED_CALLBACKS = _BoundedCallbackSet(maxsize=10000)


def on_telegram_update(update):
    """
    當 TelegramWebSocket 收到新訊息/回調時觸發

    負責解析 update 並分發到對應的處理函式

    處理的 update 類型：
    - callback_query: inline keyboard 按鈕點擊
    - message: 一般文字訊息
    """
    update_id = update.get("update_id", 0)
    logger.info(f"收到 update: {update_id} type={'callback' if 'callback_query' in update else 'message'}")

    if "callback_query" in update:
        cq = update["callback_query"]
        callback_id = cq["id"]
        data = cq.get("data", "")

        # 防重複處理
        if callback_id in PROCESSED_CALLBACKS:
            logger.warning(f"[CALLBACK] 重複忽略 callback_id={callback_id}")
            return
        PROCESSED_CALLBACKS.add(callback_id)

        if "message" in cq:
            cb_chat_id = cq["message"]["chat"]["id"]
            message_id = cq["message"]["message_id"]
        else:
            cb_chat_id = cq["from"]["id"]
            message_id = 0

        send_chat_action(cb_chat_id, "typing")
        handle_callback(cb_chat_id, message_id, callback_id, data)

    elif "message" in update:
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        text = msg.get("text", "")
        logger.info(f"[MESSAGE] msg_id={msg.get('message_id')} chat={chat_id} text={repr(text[:30]) if text else 'empty'}")

        send_chat_action(chat_id, "typing")

        logger.info(f"[WS] routing text={repr(text[:20])}")
        if text.startswith("/"):
            parts = text.split()
            logger.info(f"[WS] handle_command {parts[0]}")
            handle_command(chat_id, parts[0], parts[1:])
        else:
            logger.info(f"[WS] handle_message text={repr(text)}")
            handle_message(chat_id, text)
        logger.info(f"[WS] routing done")


def on_telegram_error(error):
    """TelegramWebSocket 發生錯誤時觸發"""
    logger.error(f"[TelegramWS] Error: {error}")


# =============================================================================
# 主程式入口
# =============================================================================

def main():
    """
    主程式：初始化所有模組並啟動

    啟動順序：
    1. Telegram WebSocket
    2. Funding 監控線程
    3. Market 監控線程
    4. 策略引擎
    5. Bitfinex WS 即時監控（每用戶一條連線）
    6. 確保每個已註冊使用者都有置頂訊息
    """
    logger.info("=" * 50)
    logger.info("Telegram Funding Bot v4.0 啟動")
    logger.info("=" * 50)

    # 初始化 Telegram WebSocket
    cfg = load_bot_config()
    bot_token = cfg["telegram"]["bot_token"]

    ws = telegram_ws.TelegramWebSocket(
        bot_token=bot_token,
        on_update=on_telegram_update,
        on_error=on_telegram_error
    )

    # 初始化全域 helper（讓 telegram_ws.py 可以呼叫）
    telegram_ws.init(bot_token)
    telegram_ws.global_set_my_commands()

    # -------------------------------------------------------------------------
    # 啟動 Funding 監控線程
    # -------------------------------------------------------------------------
    funding_thread = threading.Thread(
        target=funding_monitor.start_funding_monitor,
        kwargs={
            'MIN_IDLE_USD': MIN_IDLE_USD,
            'DEFAULT_PERIOD_DAYS': DEFAULT_PERIOD_DAYS,
            'HIGH_RATE_THRESHOLD': HIGH_RATE_THRESHOLD,
            'LOW_RATE_THRESHOLD': LOW_RATE_THRESHOLD,
            'AUTO_CHECK_INTERVAL': AUTO_CHECK_INTERVAL,
            'SETTLE_INTERVAL': SETTLE_INTERVAL,
            'load_users': load_users,
            'create_exchange': create_exchange,
            '_safe_api_call': _safe_api_call,
            '_api_lock': _api_lock,
            'send_message': send_message,
            'get_active_funding_offers': get_active_funding_offers,
            'funding_offer_placed': funding_offer_placed,
        },
        daemon=True
    )
    funding_thread.start()
    logger.info("Funding 監控線程已啟動")

    # -------------------------------------------------------------------------
    # 啟動 Market 監控線程
    # -------------------------------------------------------------------------
    market_mon = market_monitor.MarketMonitor()
    market_mon.start()
    logger.info("Market 監控線程已啟動")

    # 掛載到 funding_monitor 供策略使用
    funding_monitor.set_market_monitor(market_mon)

    # -------------------------------------------------------------------------
    # 啟動策略引擎
    # -------------------------------------------------------------------------
    strategy_conf = strategy_engine.StrategyConfig()
    strategy_conf.MAX_OFFER_USD = 150  # 安全限制：最多 150 USD
    strategy_eng = strategy_engine.StrategyEngine(market_mon, strategy_conf)

    # 設定撤單回調
    def _strategy_cancel():
        """策略引擎回調：WS 撤單所有 offer"""
        import bitfinex_monitor
        users = load_users()
        for uid, u in users.get("users", {}).items():
            ws = bitfinex_monitor.get_ws_client(uid)
            if not ws:
                continue
            try:
                ex = create_exchange(u)
                offers = get_active_funding_offers(ex)
                for o in offers:
                    oid = o.get("id")
                    if oid:
                        ws.send_offer_cancel(str(oid))
                        logger.info(f"[Strategy] WS 撤單 #{oid}")
            except Exception as e:
                logger.warning(f"[Strategy] 撤單失敗 uid={uid}: {e}")

    strategy_eng.set_cancel_callback(_strategy_cancel)
    strategy_eng.start()
    logger.info("策略引擎已啟動")

    # 掛載到 funding_monitor
    funding_monitor.set_strategy_engine(strategy_eng)

    # -------------------------------------------------------------------------
    # 確保每個已註冊使用者都有置頂訊息
    # -------------------------------------------------------------------------
    users_data = load_users()
    for uid, user in users_data.get("users", {}).items():
        chat_id = user.get("chat_id")
        if chat_id:
            try:
                ensure_pinned_welcome(int(chat_id), user)
            except Exception as e:
                logger.error(f"[Pin] 初始化置頂失敗 uid={uid}: {e}")

    # -------------------------------------------------------------------------
    # 啟動 Bitfinex WS 即時監控（每用戶一條連線）
    # -------------------------------------------------------------------------
    try:
        bitfinex_monitor.start_all_user_ws(
            load_users=load_users,
            create_exchange=create_exchange,
            send_message=send_message,
            update_pinned=update_status_message,
            pin_message=pin_message,
            get_pinned_message=get_pinned_message
        )
        logger.info("Bitfinex WS 監控線程已啟動")
    except Exception as e:
        logger.error(f"[BitfinexWS] 啟動失敗: {e}")

    # -------------------------------------------------------------------------
    # 啟動 WebSocket（主執行緒保持運行）
    # -------------------------------------------------------------------------
    ws.start()

    # 主執行緒保持運行
    funding_thread.join()


if __name__ == "__main__":
    import os

    # PID file：防止重複啟動
    pid_file = "/tmp/trading_bot.pid"
    if os.path.exists(pid_file):
        old_pid = open(pid_file).read().strip()
        if old_pid and os.path.exists(f"/proc/{old_pid}"):
            print(f"⚠️ 程式已在執行中 (PID {old_pid})，不重複啟動。")
            exit(1)
    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))
    print(f"✅ PID {os.getpid()} 已寫入 {pid_file}")

    main()

    # 正常結束時刪除 PID file
    os.unlink(pid_file)
