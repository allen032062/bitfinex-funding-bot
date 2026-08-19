# funding_logic.py
# =============================================================================
# Funding 階梯放貸策略模組
# =============================================================================
#
# 【策略說明】
#   根據市場 FRR（Flash Return Rate）自動計算掛單利率與分批金額
#   - 最低利率保護：10% 年化（防止低利率時過度讓價）
#   - 智慧分批：每筆 500 USD，超過門檻才分多筆
#   - 自動取消重掛：掛單超過 2 小時未成交自動取消
#   - 天期動態調整：根據年化利率自動選擇 2/7/30/120 天
#
# 【利率單位說明】
#   - 內部計算：使用小數形式（如 0.00031205 = 0.031205%）
#   - 顯示給用戶：乘以 100 變成百分比（如 0.031205%）
#   - 年化利率 = 日率 × 365
# =============================================================================

import logging
import time

logger = logging.getLogger(__name__)

# =============================================================================
# 全域常數設定
# =============================================================================

# 最低利率保護（年化 10%）
# 當市場 FRR 低於 10% 時，仍以 10% 掛單，避免過度讓價
MIN_RATE_ANNUAL = 0.10   # 10% 年化
MIN_RATE_DAILY = MIN_RATE_ANNUAL / 365  # 日利率 ≈ 0.00027397（小數形式）

# 分批掛單設定
OFFER_MIN_AMOUNT = 150   # 每筆最低金額（USD，低於此不掛單）
OFFER_SIZE = 500         # 每筆標準大小（USD）

# 掛單過期時間（小時）
# 超過此時間未成交，會被自動取消並重新掛單
OFFER_EXPIRE_HOURS = 2

# 天期設定（根據年化利率自動選擇）
PERIOD_HIGH_RATE = 120    # 年利率 > 14.5% 時 → 掛 120 天長單
PERIOD_MID_RATE = 7       # 年利率 > 12% 時 → 掛 7 天中期
PERIOD_LOW_RATE = 2       # 年利率 <= 12% 時 → 掛 2 天短單

# 天期判斷門檻（內部用小數表示，如 0.145 = 14.5%）
PERIOD_THRESHOLD_HIGH = 0.145   # 14.5%（觸發長天期）
PERIOD_THRESHOLD_MID = 0.12    # 12%（觸發中期天期）


# =============================================================================
# 利率計算函式
# =============================================================================

def get_initial_rate(frr_rate):
    """
    根據市場 FRR 計算初始掛單利率（每日利率，小數形式）

    邏輯：
    - 取 max(最低利率, FRR - 微量偏移)
    - 避免掛得比市場 FRR 還低，保留微幅競爭優勢

    參數：
        frr_rate: FRR 年化利率（小數形式，如 0.15 表示 15%）

    回傳：
        日利率（小數形式，如 0.00031205）
    """
    daily_frr = frr_rate / 365 if frr_rate else 0
    # 取最低利率與 FRR - 0.001% 的較大值
    initial = max(MIN_RATE_DAILY, daily_frr - 0.00001)
    logger.info(f"[Ladder] FRR={frr_rate*100:.2f}% -> initial_rate={initial*100:.4f}%/天")
    return initial


def get_period(rate_annual):
    """
    根據年化利率自動選擇借款天期

    策略邏輯：
    - 年化 > 14.5%：借款方願意借長天期（120 天），鎖定高收益
    - 年化 > 12%：中期（7 天）
    - 年化 <= 12%：短期（2 天），保持靈活性

    參數：
        rate_annual: 年化利率（小數形式，如 0.15 表示 15%）

    回傳：
        天期（120、7 或 2）
    """
    if rate_annual > PERIOD_THRESHOLD_HIGH:
        period = PERIOD_HIGH_RATE
    elif rate_annual > PERIOD_THRESHOLD_MID:
        period = PERIOD_MID_RATE
    else:
        period = PERIOD_LOW_RATE
    logger.info(f"[Ladder] rate={rate_annual*100:.2f}% -> period={period}天")
    return period


def calculate_adjusted_rate(current_rate, adjustment_pct=0.0001):
    """
    計算下修後的利率（用於市場成交利率策略）

    當市場成交利率高但遲遲未成交時，逐漸小幅下調利率以增加成交機會

    參數：
        current_rate: 目前日利率（小數形式）
        adjustment_pct: 每次調降幅度（預設 0.0001 = 0.01%）

    回傳：
        新的日利率（小數形式），若低於最低利率則回傳 None（不再下修）
    """
    new_rate = current_rate - adjustment_pct

    if new_rate < MIN_RATE_DAILY:
        logger.warning(f"[Ladder] rate={new_rate*100:.4f}% < MIN={MIN_RATE_DAILY*100:.4f}%，停止下修")
        return None

    logger.info(f"[Ladder] rate調整: {current_rate*100:.4f}% -> {new_rate*100:.4f}%/天")
    return new_rate


# =============================================================================
# 分批金額計算
# =============================================================================

def calculate_ladder_amounts(avail_usd):
    """
    計算階梯分批金額（將總可用金額分成多筆掛單）

    分批策略：
    - 餘額 < 150 USD：不掛單（金額太低沒意義）
    - 150 ~ 499.99 USD：一筆（全部放進去）
    - >= 500 USD：每筆 500 USD，最後一筆若 < 500 但 >= 150 保留
    - 若最後一筆 < 150：併入倒數第二筆

    目的：分散訂單，增加部分成交的機會

    參數：
        avail_usd: 可用 USD 金額

    回傳：
        amount 列表（如 [500, 500, 350]），空列表表示不掛單
    """
    if avail_usd < OFFER_MIN_AMOUNT:
        logger.info(f"[Ladder] avail={avail_usd:.2f} < {OFFER_MIN_AMOUNT}，不掛單")
        return []

    # 計算可以分成幾筆 500
    num_offers = int(avail_usd // OFFER_SIZE)
    remainder = avail_usd % OFFER_SIZE

    amounts = []

    if num_offers == 0:
        # 150 ~ 499.99 USD：一筆（允許低於 500 但不能低於 150）
        amounts = [avail_usd]
    elif remainder == 0:
        # 剛好整除，每筆都是 OFFER_SIZE
        amounts = [OFFER_SIZE] * num_offers
    else:
        # 有餘數需要處理
        if remainder < OFFER_MIN_AMOUNT:
            # 最後一筆 < 150，併入倒數第二筆
            amounts = [OFFER_SIZE] * (num_offers - 1)
            amounts.append(OFFER_SIZE + remainder)
        else:
            # 最後一筆 >= 150（即使 < 500 也保留）
            amounts = [OFFER_SIZE] * num_offers
            amounts.append(remainder)

    logger.info(f"[Ladder] avail={avail_usd:.2f} -> {len(amounts)} orders: {amounts}")
    return amounts


# =============================================================================
# 掛單狀態檢查
# =============================================================================

def check_offer_age(ts_created, now_ms=None):
    """
    檢查掛單是否超過 20 分鐘（已停用）

    歷史原因：早期版本使用 20 分鐘過期制
    現已改為 2 小時，請使用 check_offer_expired()
    """
    return False


def check_offer_expired(ts_created, now_ms=None):
    """
    檢查掛單是否已過期（超過 2 小時未成交）

    用於自動取消重掛邏輯

    參數：
        ts_created: 掛單建立時間（毫秒 timestamp，Unix epoch in milliseconds）
        now_ms: 現在時間（毫秒 timestamp，預設 None 表示使用 current_time）

    回傳：
        True = 已過期（>= OFFER_EXPIRE_HOURS 小時），False = 未過期
    """
    if not ts_created:
        return False

    now = now_ms or int(time.time() * 1000)
    age_hours = (now - ts_created) / 3600000

    return age_hours >= OFFER_EXPIRE_HOURS


# =============================================================================
# 訊息格式化
# =============================================================================

def format_strategy_msg(rate_daily, period_days=120):
    """
    格式化策略推播訊息（已停用，保留向後相容）

    參數：
        rate_daily: 日利率（小數形式）
        period_days: 天期（預設 120）

    回傳：
        格式化訊息文字
    """
    rate_annual = rate_daily * 365 * 100
    return f"[策略] FRR 下修掛單\n利率: {rate_annual:.2f}%/年 ({rate_daily*100:.4f}%/天)\n天期: {period_days}天"


# =============================================================================
# 單元測試
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Test 1: 計算金額
    print("\n=== Test 計算金額 ===")
    calculate_ladder_amounts(100)   # < 150 → 不掛單
    calculate_ladder_amounts(200)   # 150~299 → 一筆
    calculate_ladder_amounts(350)   # 有餘數
    calculate_ladder_amounts(300)   # 剛好 500
    calculate_ladder_amounts(450)   # 需合併最後一筆

    # Test 2: 計算利率
    print("\n=== Test 計算利率 ===")
    get_initial_rate(0.15)   # FRR 15% → 高於最低利率
    get_initial_rate(0.08)   # FRR 8% → 低於最低利率，使用 MIN

    # Test 3: 下修利率
    print("\n=== Test 下修 ===")
    r = 0.0003  # 0.03%/天
    for _ in range(5):
        r = calculate_adjusted_rate(r)
        if r is None:
            break
