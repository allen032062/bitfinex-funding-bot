# funding_monitor.py
# =============================================================================
# Funding 自動監控與掛單模組（WebSocket 版）
# =============================================================================
# 功能說明：
#   - 背景執行緒，持續監控市場利率並自動掛單
#   - 掛單（on）/ 撤單（oc）全部走 WebSocket Auth 通道
#   - 不再使用 REST API 進行掛單操作
#
# 利率單位說明：
#   - 內部計算：使用小數形式（如 0.00031205 = 0.031205%）
#   - 顯示給用戶：乘以 100 變成百分比（如 0.031205%）
#   - 年化利率 = 日率 × 365
# =============================================================================

import time
import logging
from enum import Enum
from dataclasses import dataclass

import bitfinex_monitor
from funding_logic import check_offer_expired

# 取得本模組的 logger 實例
logger = logging.getLogger(__name__)


# =============================================================================
# 市場利率查詢（統一由 MarketMonitor 提供快照資料）
# =============================================================================

def get_market_funding_rate(exchange):
    """
    取得目前市場 Funding 利率（FRR, Flash Return Rate）
    統一使用 MarketMonitor 的快照資料，確保所有來源一致。

    參數：
        exchange: Bitfinex 交易所實例（本參數目前未使用，保留向後相容）

    回傳：
        (日率_小數, 年率_百分比, 原始日率) 或 (None, None, None)
        注意：年率已乘 100 轉換為百分比形式
    """
    snap = get_market_snapshot()
    if snap and snap.frr_rate:
        # frr_rate: 日率（小數），frr_annual: 年率（百分比）
        return snap.frr_rate, snap.frr_annual, snap.frr_rate
    return None, None, None


# =============================================================================
# 市場監控整合（MarketMonitor 快照）
# =============================================================================

# 全域市場監控器引用（由外部注入）
_market_monitor = None


def set_market_monitor(mon):
    """
    注入市場監控器實例（通常在啟動時由主程式呼叫）

    參數：
        mon: MarketMonitor 實例，需提供 get_snapshot() 方法
    """
    global _market_monitor
    _market_monitor = mon
    logger.info("[funding_monitor] Market Monitor 已掛載")


def get_market_snapshot():
    """
    取得市場快照資料

    回傳：
        MarketSnapshot 物件或 None（若監控器未設定）
    """
    if _market_monitor:
        return _market_monitor.get_snapshot()
    return None


# =============================================================================
# 策略狀態機（Strategy State Machine）
# =============================================================================

class StrategyState(Enum):
    """
    策略狀態列舉

    - FRR_BOTTOM (A): 保底策略，掛在 FRR（市場最低利率）
    - LADDER (B): 階梯策略，分散掛單到多個利率層級
    - GRAB_WALL (C): 搶牆策略，積極搶奪牆單
    - WATCH (D): 觀望，不主動掛單
    """
    FRR_BOTTOM = "A"
    LADDER = "B"
    GRAB_WALL = "C"
    WATCH = "D"


@dataclass
class StrategyConfig:
    """
    策略配置參數（dataclass 方便統一管理）
    """
    state: StrategyState = StrategyState.FRR_BOTTOM  # 目前狀態
    low_vol_threshold: float = 0.10   # 低波動門檻（低於此值認為是低波動）
    high_vol_threshold: float = 0.30   # 高波動門檻（高於此值認為是高波動）
    frr_offset_bps: int = 10          # FRR 偏移，單位 basis points（1 bps = 0.01%）
    ladder_levels: int = 5             # 階梯策略的層級數量
    ladder_spacing_bps: int = 5        # 階梯相鄰層級的間隔（bps）
    wall_distance_bps: int = 3        # 牆單距離（bps）
    min_wall_amount: float = 1000      # 牆單最小金額（USD）


# 全域策略配置實例
_strategy_config = StrategyConfig()


def get_strategy_state() -> StrategyState:
    """取得目前的策略狀態"""
    return _strategy_config.state


def evaluate_state(snapshot) -> StrategyState:
    """
    根據市場快照評估應該進入哪種策略狀態

    參數：
        snapshot: MarketSnapshot 物件，包含波動率等市場資訊

    回傳：
        建議的 StrategyState
    """
    if snapshot is None:
        return StrategyState.WATCH

    # 根據波動率（volatility）判斷市場狀態
    vol = snapshot.volatility
    if vol < _strategy_config.low_vol_threshold:
        # 低波動 → 保底策略，避免風險
        return StrategyState.FRR_BOTTOM
    elif vol > _strategy_config.high_vol_threshold:
        # 高波動 → 階梯策略，追求更高收益
        return StrategyState.LADDER
    else:
        # 中等波動 → 觀望
        return StrategyState.WATCH


def switch_state(new_state, exchange, user, chat_id,
                 _safe_api_call, send_message, get_active_funding_offers):
    """
    切換策略狀態（使用 WS 撤單）

    流程：
    1. 若從非 WATCH 狀態切出，先撤除所有舊掛單
    2. 更新狀態
    3. 通知用戶

    參數：
        new_state: 新的策略狀態
        exchange: Bitfinex 交易所實例
        user: 用戶資料字典
        chat_id: Telegram 聊天 ID
        _safe_api_call: 安全 API 呼叫函式
        send_message: 發送 Telegram 訊息的函式
        get_active_funding_offers: 取得活躍掛單的函式
    """
    old_state = _strategy_config.state
    if new_state == old_state:
        return False  # 狀態沒變，不做任何事

    logger.info(f"[Strategy] 狀態切換: {old_state.value} → {new_state.value}")

    # 從非觀望狀態切出時，需要撤除所有掛單
    if old_state != StrategyState.WATCH:
        try:
            offers = get_active_funding_offers(exchange)
            import bitfinex_monitor
            uid = str(user.get("chat_id"))
            ws = bitfinex_monitor.get_ws_client(uid)
            for offer in offers:
                offer_id = offer.get("id")
                if offer_id and ws:
                    logger.info(f"[Strategy] WS 撤單 #{offer_id}")
                    ws.send_offer_cancel(str(offer_id))
        except Exception as e:
            logger.warning(f"[Strategy] 撤單失敗: {e}")

    # 更新全域狀態
    _strategy_config.state = new_state

    # 狀態名稱對照表（給用戶看的中文名稱）
    state_names = {
        StrategyState.FRR_BOTTOM: "FRR保底",
        StrategyState.LADDER: "階梯單",
        StrategyState.GRAB_WALL: "搶牆",
        StrategyState.WATCH: "觀望"
    }
    msg = f"📊 策略切換: {state_names[old_state]} → {state_names[new_state]}"
    send_message(chat_id, msg)
    return True


# =============================================================================
# 策略引擎整合（Strategy Engine Integration）
# =============================================================================

_strategy_engine = None


def set_strategy_engine(engine):
    """
    注入策略引擎實例

    參數：
        engine: 策略引擎物件，需提供 get_plan() 和 get_state() 方法
    """
    global _strategy_engine
    _strategy_engine = engine
    logger.info("[funding_monitor] Strategy Engine 已掛載")


def get_strategy_engine():
    """取得已註冊的策略引擎"""
    return _strategy_engine


def get_strategy_plan():
    """
    取得策略引擎的執行計畫

    回傳：
        策略計畫字典或 None
    """
    if _strategy_engine:
        return _strategy_engine.get_plan()
    return None


def get_strategy_state_ws() -> StrategyState:
    """
    WebSocket 版本的策略狀態查詢

    回傳：
        目前策略狀態（優先取自引擎，否則取本地配置）
    """
    if _strategy_engine:
        return _strategy_engine.get_state()
    return _strategy_config.state


# =============================================================================
# 主監控線程啟動函式
# =============================================================================

def start_funding_monitor(*, MIN_IDLE_USD, DEFAULT_PERIOD_DAYS, HIGH_RATE_THRESHOLD,
                          LOW_RATE_THRESHOLD, AUTO_CHECK_INTERVAL,
                          SETTLE_INTERVAL, load_users, create_exchange,
                          _safe_api_call, _api_lock, send_message,
                          get_active_funding_offers, funding_offer_placed=None,
                          funding_offer_update=None, get_balance=None,
                          get_user=None, balance=None):
    """
    背景線程：自動掛單 + 狀態監控

    主要職責：
    1. 定期檢查是否需要自動掛單（AUTO_CHECK_INTERVAL）
    2. 定期嘗試結算利息收入（SETTLE_INTERVAL）
    3. 檢查並自動取消過期掛單（超過 2 小時未成交）

    掛單/撤單走 WS Auth 通道（由 bitfinex_monitor 提供）

    參數：
        MIN_IDLE_USD: 最小閒置 USD 金額（低於此值不掛單）
        DEFAULT_PERIOD_DAYS: 預設借款天期
        HIGH_RATE_THRESHOLD: 高利率門檻（觸發警告）
        LOW_RATE_THRESHOLD: 低利率門檻（觸發警告）
        AUTO_CHECK_INTERVAL: 自動檢查間隔（秒）
        SETTLE_INTERVAL: 結算檢查間隔（秒）
        load_users: 載入用戶資料的回調函式
        create_exchange: 建立交易所實例的回調函式
        _safe_api_call: 安全 API 呼叫包裝函式
        _api_lock: API 呼叫鎖
        send_message: 發送 Telegram 訊息函式
        get_active_funding_offers: 取得活躍掛單函式
        funding_offer_placed: 掛單成功回調（已廢棄，保留向後相容）
        funding_offer_update: 掛單更新回調（已廢棄）
        get_balance: 取得餘額函式（已廢棄）
        get_user: 取得用戶函式（已廢棄）
        balance: 餘額資料（已廢棄）
    """
    logger.info("Funding 監控線程啟動（WS 版）")

    # 追蹤上次檢查時間，避免重複執行
    last_unfilled_warn = {}
    last_auto_check = 0
    last_settle_check = 0

    # =========================================================================
    # WebSocket 指令確認回調函式
    # =========================================================================

    def _on_offer_new_conf(uid, offer_id, data, ok):
        """
        WS 新掛單（on）指令確認 callback

        當 WebSocket 收到掛單確認訊息時會觸發此函式

        參數：
            uid: 用戶 ID
            offer_id: Bitfinex 分配的掛單 ID（可能為 None，需等 fou 事件補足）
            data: 原始回傳資料
            ok: 是否成功
        """
        users_data = load_users()
        user = users_data["users"].get(uid)
        if not user:
            logger.warning(f"[WS-ON] uid={uid} user not found in users_data")
            return
        chat_id = user.get("chat_id")
        logger.info(f"[WS-ON] uid={uid} chat_id={chat_id} offer_id={offer_id} ok={ok}")
        if not chat_id:
            logger.warning(f"[WS-ON] uid={uid} chat_id is None/0")
            return

        if ok and offer_id:
            # offer_id 已確認（Bitfinex 已分配 ID），等待 fou 事件發送完整狀態
            logger.info(f"[WS-ON] uid={uid} offer_id={offer_id} 掛單成功，停止重複通知")
        elif ok:
            # offer_id 為 None 但仍成功（Bitfinex 尚未分配 ID）
            snap = get_market_snapshot()
            frr_annual = snap.frr_annual if snap else 0
            logger.info(f"[WS-ON] uid={uid} 掛單成功（待確認 ID）")
            msg = (f"📝 <b>掛單成功（待確認 ID）</b>\n\n"
                   f"Offer ID: <code>分配中</code>\n"
                   f"FRR: <code>{frr_annual:.4f}% APR</code>\n"
                   f"狀態: ✅ 已送達Bitfinex")
            send_message(int(chat_id), msg)
        else:
            # 掛單失敗
            logger.warning(f"[WS-ON] uid={uid} 掛單失敗 data={data}")
            send_message(int(chat_id), "❌ <b>掛單失敗</b>\n" + str(data))

    def _on_offer_cancel_conf(uid, offer_ids, ok):
        """
        WS 撤單（oc）指令確認 callback

        參數：
            uid: 用戶 ID
            offer_ids: 被取消的 offer ID 列表
            ok: 是否成功
        """
        if ok:
            logger.info(f"[WS-OC] uid={uid} 撤單成功: {offer_ids}")
        else:
            logger.warning(f"[WS-OC] uid={uid} 撤單失敗: {offer_ids}")

    # =========================================================================
    # 為每個用戶註冊 WS 回調（各自獨立等待，不互相拖累）
    # =========================================================================
    users_data = load_users()
    for uid, user in users_data["users"].items():
        for reg_attempt in range(10):
            ws = bitfinex_monitor.get_ws_client(uid)
            if ws and hasattr(ws, "_register_offer_callbacks"):
                ws._register_offer_callbacks(_on_offer_new_conf, _on_offer_cancel_conf)
                logger.info(f"[Funding] uid={uid} WS callbacks 已註冊")
                break
            logger.warning(f"[Funding] uid={uid} ws not ready (attempt {reg_attempt+1}/10), waiting...")
            time.sleep(1)

    # =========================================================================
    # 內部輔助函式：透過 WS 發送新掛單
    # =========================================================================

    def _ws_place_offer(uid, user, chat_id, amount, offer_rate, rate_pct, period_days, frr_annual):
        """
        透過 WebSocket 發送新掛單（on）指令

        注意：不直接回傳 offer_id，由 conf callback 補上

        參數：
            uid: 用戶 ID
            user: 用戶資料字典
            chat_id: Telegram 聊天 ID
            amount: 掛單金額（USD）
            offer_rate: 日率（小數形式，如 0.0001 = 0.01%/天）
            rate_pct: 年率（百分比形式，如 3.65 表示 3.65%）
            period_days: 借款天期
            frr_annual: 當前 FRR 年率（百分比）

        回傳：
            client order ID（cid）或 None
        """
        ws = bitfinex_monitor.get_ws_client(uid)
        if not ws:
            logger.warning(f"[WS-ON] uid={uid} WS 未就緒")
            return None

        # 墊派資料，等 conf 回來時取用
        cid = ws.send_offer_new("fUSD", amount, offer_rate, period_days)
        logger.info(f"[WS-ON] uid={uid} cid={cid} amount={amount} rate={offer_rate} period={period_days}")
        return cid

    # =========================================================================
    # 主監控迴圈
    # =========================================================================
    while True:
        try:
            now = time.time()
            users_data = load_users()

            for uid, user in users_data["users"].items():
                chat_id = user.get("chat_id")
                if not chat_id:
                    continue

                ws = bitfinex_monitor.get_ws_client(uid)
                if not ws:
                    continue

                try:
                    # ── 每 AUTO_CHECK_INTERVAL 秒檢查是否要自動掛單 ──
                    if now - last_auto_check >= AUTO_CHECK_INTERVAL:
                        placed = _ws_auto_place(
                            ws, uid, user, chat_id,
                            MIN_IDLE_USD, DEFAULT_PERIOD_DAYS, HIGH_RATE_THRESHOLD,
                            LOW_RATE_THRESHOLD,
                            _ws_place_offer,
                            _safe_api_call, _api_lock, send_message,
                            get_active_funding_offers, funding_offer_placed
                        )
                        last_auto_check = now

                    # ── 每 SETTLE_INTERVAL 秒嘗試結算 ──
                    if now - last_settle_check >= SETTLE_INTERVAL:
                        _try_settle(
                            users_data, user, chat_id,
                            _safe_api_call, _api_lock, load_users, create_exchange
                        )
                        last_settle_check = now

                    # ── 檢查未成交掛單是否過期（> 2小時）→ 取消重掛 ──
                    try:
                        user_exchange = _safe_api_call(create_exchange, user)
                        offers = get_active_funding_offers(user_exchange)
                    except Exception as exp_err:
                        logger.warning(f"[Funding] 取得掛單失敗 uid={uid}: {exp_err}")
                        offers = []

                    expired_offer_ids = []
                    logger.info(f"[Funding] uid={uid} 檢查過期掛單，目前有 {len(offers)} 張掛單")

                    # Bitfinex API 回傳掛單格式：[id, cid, mts_created, mts_updated, amount, ...]
                    # 索引意義：
                    #   [0] = id, [2] = mts_created (ms), [10] = status, [14] = rate, [15] = period
                    for offer in offers:
                        offer_id = offer[0] if len(offer) > 0 else None
                        ts_created_str = offer[2] if len(offer) > 2 else None
                        ts_created = int(ts_created_str) if ts_created_str else None

                        if offer_id and ts_created:
                            # 計算掛單年齡（小時）
                            age_h = (int(time.time() * 1000) - ts_created) / 3600000
                            is_exp = check_offer_expired(ts_created)
                            logger.info(f"[Funding] uid={uid} offer #{offer_id} age={age_h:.2f}h expired={is_exp}")

                            if is_exp:
                                expired_offer_ids.append(str(offer_id))

                    # 若有過期掛單，批量取消並通知
                    if expired_offer_ids:
                        logger.info(f"[Funding] uid={uid} 有 {len(expired_offer_ids)} 張掛單超過2小時未成交，取消: {expired_offer_ids}")
                        for oid in expired_offer_ids:
                            ws.send_offer_cancel(oid)
                        send_message(
                            int(chat_id),
                            f"⏰ <b>掛單未成交自動取消</b>\n\n"
                            f"{len(expired_offer_ids)} 張掛單超過2小時未借出，已取消重掛"
                        )

                except Exception as e:
                    logger.error(f"[Funding] 監控失敗 uid={uid}: {e}")

            time.sleep(60)  # 主迴圈休眠 60 秒

        except Exception as e:
            logger.error(f"[Funding] 監控線程例外: {e}")
            time.sleep(30)


# =============================================================================
# 自動掛單邏輯（WS 版本）
# =============================================================================

def _ws_auto_place(ws, uid, user, chat_id,
                   MIN_IDLE_USD, DEFAULT_PERIOD_DAYS, HIGH_RATE_THRESHOLD,
                   LOW_RATE_THRESHOLD,
                   ws_place_offer,
                   _safe_api_call, _api_lock,
                   send_message, get_active_funding_offers, funding_offer_placed):
    """
    透過 WebSocket 自動掛單

    核心邏輯：
    1. 檢查用戶是否啟用自動掛單（auto_place）
    2. 檢查 funding USD 餘額是否足夠（需 > min_amount）
    3. 根據市場利率計算分批掛單金額
    4. 若市場成交利率 > 7% 且等待 > 15 分鐘，第一筆下修到市場成交利率
    5. 其餘掛單使用 FRR

    利率說明：
    - 市場利率（FRR）：Flash Return Rate，市場平均利率
    - 市場成交利率（Trade Rate）：實際成交的利率

    參數：
        ws: WebSocket 客戶端
        uid: 用戶 ID
        user: 用戶資料字典
        chat_id: Telegram 聊天 ID
        MIN_IDLE_USD: 最小閒置金額
        DEFAULT_PERIOD_DAYS: 預設天期
        HIGH_RATE_THRESHOLD: 高利率門檻
        LOW_RATE_THRESHOLD: 低利率門檻
        ws_place_offer: WS 掛單函式
        _safe_api_call: 安全 API 呼叫
        _api_lock: API 鎖
        send_message: 發送訊息函式
        get_active_funding_offers: 取得活躍掛單
        funding_offer_placed: 掛單成功回調

    回傳：
        成功發送的 cid 列表，或 None
    """

    # 檢查用戶是否啟用自動掛單
    if not user.get("auto_place", False):
        return None

    try:
        from funding_logic import (
            calculate_ladder_amounts,
            get_initial_rate,
            get_period,
            MIN_RATE_ANNUAL,
        )

        # 用戶自訂最小掛單金額（預設 150 USD）
        min_amount = user.get("min_amount", 150)

        # 取得 funding 錢包 USD 餘額（使用 REST API，低頻且需準確）
        exchange = _safe_api_call(
            lambda: __import__("ccxt").bitfinex({
                "apiKey": user.get("api_key"),
                "secret": user.get("api_secret"),
            })
        )
        with _api_lock:
            bal = exchange.fetch_balance({"type": "funding"})
        funding_usd_free = float(bal.get("free", {}).get("USD", 0) or 0)

        # 餘額不足，不掛單
        if funding_usd_free < min_amount:
            logger.info(f"[AutoCheck] 目前可用: {funding_usd_free:.2f} USD, 門檻: {min_amount} USD, 狀態: 餘額不足")
            return None

        logger.info(f"[AutoCheck] 目前可用: {funding_usd_free:.2f} USD, 門檻: {min_amount} USD, 狀態: 執行掛單")

        # 計算分批掛單金額（階梯式）
        amounts = calculate_ladder_amounts(funding_usd_free)
        if not amounts:
            return None

        # 取得市場利率（從 MarketMonitor 全域快照）
        snap = get_market_snapshot()
        if snap and snap.frr_annual:
            frr_annual = snap.frr_annual        # 年率（百分比形式，如 3.65）
            frr_daily = snap.frr_rate           # 日率（小數形式，如 0.0001）
        else:
            frr_annual = 0
            frr_daily = MIN_RATE_ANNUAL / 365   # fallback: 使用最小利率

        # =====================================================================
        # Trade Rate 策略（市場成交利率策略）
        # =====================================================================
        # 邏輯：若最近市場成交利率 > 7% 年化，且超過 15 分鐘未成交
        #       → 第一筆下修到市場成交利率（增加成交機會）
        TRADE_RATE_THRESHOLD = 7.0   # 市場成交利率門檻（%）
        TRADE_WAIT_MINUTES = 15      # 等待時間門檻（分鐘）

        trade_rate_pct, trade_ts = ws.get_last_trade_rate()
        now_ms = int(time.time() * 1000)
        trade_wait_min = (now_ms - trade_ts) / 60000 if trade_ts else 999999

        # 判斷是否要套用市場成交利率策略
        apply_trade_rate = (
            trade_rate_pct >= TRADE_RATE_THRESHOLD
            and trade_wait_min >= TRADE_WAIT_MINUTES
        )

        if apply_trade_rate:
            # 第一筆使用市場成交利率
            effective_rate_pct = trade_rate_pct                          # 百分比（如 7.5）
            effective_rate_daily = effective_rate_pct / 100 / 365        # 轉小數
            logger.info(f"[AutoCheck] uid={uid} 市場成交利率={trade_rate_pct:.2f}% "
                        f"(等待{trade_wait_min:.0f}min) → 第一筆下修到此利率")
        else:
            # 使用 FRR
            effective_rate_pct = frr_annual
            effective_rate_daily = get_initial_rate(frr_annual / 100)

        offer_rate = effective_rate_daily
        rate_annual = effective_rate_pct / 100   # 百分比 → 小數（內部計算用）
        period_days = get_period(rate_annual)

        # =====================================================================
        # 分批掛單
        # =====================================================================
        # 第一筆（index=0）：apply_trade_rate ? trade_rate : frr
        # 其餘維持 FRR
        placed_cids = []
        for i, amount in enumerate(amounts):
            if apply_trade_rate and i == 0:
                # 第一筆：使用市場成交利率
                rate_for_this = effective_rate_daily
                rate_annual_for_this = effective_rate_pct
            else:
                # 其餘：使用 FRR
                rate_for_this = get_initial_rate(frr_annual / 100)
                rate_annual_for_this = frr_annual

            cid = ws_place_offer(
                uid, user, chat_id,
                amount, rate_for_this, rate_annual_for_this, period_days, frr_annual
            )
            if cid:
                placed_cids.append(cid)

        # 若觸發市場成交利率策略，通知用戶
        if apply_trade_rate and len(amounts) > 1:
            send_message(
                int(chat_id),
                f"📉 <b>市場成交利率策略觸發</b>\n\n"
                f"市場成交利率: <code>{trade_rate_pct:.2f}%</code>/年\n"
                f"等待時間: <code>{trade_wait_min:.0f}</code> 分鐘\n\n"
                f"掛 {len(amounts)} 筆：第1筆 {amounts[0]:.2f} USD "
                f"@{trade_rate_pct:.2f}%，其餘 {amounts[1]:.2f} USD @{frr_annual:.2f}%"
            )

        return placed_cids

    except Exception as e:
        logger.error(f"[Funding] 自動掛單失敗: {e}")
        return None


# =============================================================================
# 錢包事件監控（已停用）
# =============================================================================

def _check_wallet_events(exchange, user, chat_id, _safe_api_call, _api_lock, send_message):
    """
    檢測錢包餘額、Offer、Credit 的變化並主動通知

    注意：此函式已停用，WS 版本的錢包通知由 bitfinex_monitor.on_wallet 處理
    """
    pass  # WS 版本的錢包通知由 bitfinex_monitor.on_wallet 處理


# =============================================================================
# 利息結算邏輯
# =============================================================================

def _try_settle(users_data, user, chat_id,
                _safe_api_call, _api_lock, load_users, create_exchange):
    """
    每 SETTLE_INTERVAL 秒嘗試結算一次利息收入

    追蹤 per-user 的 last_settle_swap 總量，有增加就主動通知用戶

    Bitfinex 的 swap 機制：借款人還款時，會自動將利息轉入 funding 錢包
    透過監控 swap 總量的變化，可以偵測到新的利息收入

    參數：
        users_data: 所有用戶資料
        user: 目前要檢查的用戶
        chat_id: Telegram 聊天 ID
        _safe_api_call: 安全 API 呼叫
        _api_lock: API 鎖
        load_users: 載入用戶資料
        create_exchange: 建立交易所實例
    """
    try:
        exchange = _safe_api_call(create_exchange, user)
        with _api_lock:
            bal = exchange.fetch_balance({"type": "funding"})
        info_list = bal.get("info", [])

        for item in info_list:
            # item 格式: [currency, symbol, amount, old_amount, ...]
            # symbol = "USD" 表示 USD funding 錢包
            if isinstance(item, list) and len(item) > 4 and item[1] == "USD":
                # item[4] = swaps（該幣種的已結算利息總量）
                swap_interest = float(item[4]) if item[4] else 0.0

                uid = str(user.get("chat_id", ""))
                key = f"_last_swap_{uid}"

                # 讀取上次的 swap 量（存在函式屬性中）
                last_swap = getattr(_try_settle, key, None)

                if last_swap is not None:
                    # 非首次：計算利息增量
                    delta = swap_interest - last_swap
                    if delta > 0.001:  # 有實際利息收入（> 0.001 USD）
                        msg = (
                            f"💰 <b>利息結算通知</b>\n\n"
                            f"本次結算: <code>+{delta:.6f} USD</code>\n"
                            f"累計利息: <code>{swap_interest:.6f} USD</code>\n"
                            f"（此為已還款利息，資金已回到帳戶）"
                        )
                        send_message(int(chat_id), msg)
                        logger.info(f"[_try_settle] uid={chat_id} 利息結算 +{delta:.6f} USD, 累計 {swap_interest:.6f} USD")
                    # 更新追蹤值（不管有沒有通知都更新）
                    setattr(_try_settle, key, swap_interest)
                else:
                    # 首次：只設定初始值，不通知
                    setattr(_try_settle, key, swap_interest)
                    logger.info(f"[_try_settle] uid={chat_id} 初始化 swap 追蹤={swap_interest:.6f} USD")
                break  # 只處理第一筆 USD
    except Exception as e:
        logger.error(f"[_try_settle] uid={chat_id}: {e}")
