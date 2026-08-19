# strategy_engine.py
# =============================================================================
# 策略狀態機引擎（Phase 2-3）
# =============================================================================
#
# 【功能說明】
#   根據市場狀態（波動率、FRR、牆單）自動切換策略狀態
#   並計算預計掛單參數（利率、金額、天期）
#
# 【策略狀態】
#   FRR_BOTTOM (A): 低波動 → FRR 保底（保守策略）
#   LADDER (B): 高波動 → 階梯單（積極策略）
#   GRAB_WALL (C): 中波動 +有大牆 → 搶牆（套利策略）
#   WATCH (D): 無明確訊號 → 觀望（不做掛單）
#
# 【安全防護網】
#   1. Hard Floor：利率不能低於 FRR 或 年化 10%
#   2. 撤單連動：狀態切換時呼叫 cancel_active_offers
#   3. 金額限制：每次最多 MAX_OFFER_USD（預設 300 USD）
#
# 【利率單位說明】
#   - 內部計算：使用小數形式（如 0.00031205）
#   - 顯示給用戶：乘以 365 × 100 變成百分比
# =============================================================================

import time
import logging
import threading
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Callable

logger = logging.getLogger(__name__)


# =============================================================================
# 策略狀態列舉
# =============================================================================

class StrategyState(Enum):
    """
    策略狀態列舉

    - FRR_BOTTOM (A): 低波動 → FRR 保底（掛在 FRR 或略高）
    - LADDER (B): 高波動 → 階梯單（分散掛單到多個利率層級）
    - GRAB_WALL (C): 中波動且有大牆 → 搶牆（掛在牆單利率的 98%）
    - WATCH (D): 觀望，不主動掛單
    """
    FRR_BOTTOM = "A"
    LADDER = "B"
    GRAB_WALL = "C"
    WATCH = "D"


# =============================================================================
# 策略配置
# =============================================================================

@dataclass
class StrategyConfig:
    """
    策略配置參數

    屬性：
        LOW_VOL_THRESHOLD: 低波動門檻（< 15% 認為低波動）
        HIGH_VOL_THRESHOLD: 高波動門檻（> 40% 認為高波動）
        FRR_PREMIUM: FRR 加成倍率（1.10 = FRR × 1.10）
        LADDER_LEVELS: 階梯策略的層級數量
        MIN_RATE_ANNUAL: 最低年化利率（10%，安全地板）
        MIN_OFFER_USD: 最小掛單金額（150 USD）
        MAX_OFFER_USD: 最大掛單金額（300 USD，每筆）
    """
    # 波動率閾值
    LOW_VOL_THRESHOLD = 0.15    # < 15% = 低波動
    HIGH_VOL_THRESHOLD = 0.40  # > 40% = 高波動

    # FRR 保底參數
    FRR_PREMIUM = 1.10         # FRR × 1.10

    # 階梯單參數
    LADDER_LEVELS = 5

    # 安全防護網
    MIN_RATE_ANNUAL = 0.10     # 最低年化 10%
    MIN_OFFER_USD = 150         # 最小掛單金額（USD）
    MAX_OFFER_USD = 300        # 最大掛單金額（USD，每筆）


# =============================================================================
# 預計掛單 dataclass
# =============================================================================

@dataclass
class PlannedOffer:
    """
    預計掛單：策略引擎計算出的掛單建議參數

    屬性：
        rate: 日利率（小數形式，如 0.00031205）
        amount: 掛單金額（USD）
        period_days: 借款天期
        state: 目前策略狀態
        is_secure: 是否通過安全檢查（False = 被 Hard Floor 保護提升過）
    """
    rate: float              # 日率（小數）
    amount: float           # USD 金額
    period_days: int        # 天數
    state: StrategyState   # 策略狀態
    is_secure: bool = True  # 是否通過安全檢查


# =============================================================================
# 策略引擎
# =============================================================================

class StrategyEngine:
    """
    策略狀態機引擎

    功能：
    1. 每 10 秒評估一次市場狀態
    2. 根據波動率決定策略狀態
    3. 狀態切換時觸發撤單回調
    4. 計算預計掛單參數（帶安全檢查）
    5. 對外提供目前狀態和計畫查詢

    使用方式：
        engine = StrategyEngine(market_monitor)
        engine.set_cancel_callback(my_cancel_fn)
        engine.start()
        state = engine.get_state()
        plan = engine.get_plan()
    """

    def __init__(self, market_monitor, config: StrategyConfig = None):
        """
        初始化策略引擎

        參數：
            market_monitor: MarketMonitor 實例（用於取得市場快照）
            config: 策略配置（可選，預設使用 StrategyConfig()）
        """
        self._mon = market_monitor
        self._config = config or StrategyConfig()
        self._state = StrategyState.WATCH      # 目前狀態
        self._prev_state = StrategyState.WATCH  # 上次狀態
        self._running = False
        self._latest_plan: Optional[PlannedOffer] = None

        # 撤單/掛單回調（由 funding_monitor 注入）
        self._cancel_callback: Optional[Callable] = None
        self._place_offer_callback: Optional[Callable] = None

        # Rate Limit：撤單冷卻時間
        self._last_cancel_time = 0
        self._cancel_cooldown = 30  # 秒

    def set_cancel_callback(self, callback: Callable):
        """設定撤單回調（狀態切換時呼叫）"""
        self._cancel_callback = callback

    def set_place_offer_callback(self, callback: Callable):
        """設定掛單回調（目前未使用，保留）"""
        self._place_offer_callback = callback

    def start(self):
        """啟動策略引擎（在獨立執行緒中運行）"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("[StrategyEngine] 啟動")

    def stop(self):
        """停止策略引擎"""
        self._running = False
        logger.info("[StrategyEngine] 已停止")

    def get_state(self) -> StrategyState:
        """取得目前策略狀態"""
        return self._state

    def get_plan(self) -> Optional[PlannedOffer]:
        """取得最新預計掛單"""
        return self._latest_plan

    # =========================================================================
    # 主迴圈
    # =========================================================================

    def _run_loop(self):
        """策略引擎主迴圈（每 10 秒評估一次）"""
        while self._running:
            try:
                self._evaluate()
            except Exception as e:
                logger.error(f"[StrategyEngine] error: {e}")
            time.sleep(10)

    def _evaluate(self):
        """
        評估市場並更新策略

        流程：
        1. 讀取市場快照
        2. 計算安全地板利率（Hard Floor）
        3. 判斷波動率狀態
        4. 決定策略狀態（可能觸發撤單）
        5. 計算預計掛單參數
        """
        snap = self._mon.get_snapshot()
        if snap is None:
            return

        # 讀取市場數據
        vol = snap.volatility
        frr = snap.frr_rate
        wall_rate = snap.top_wall_rate
        wall_amount = snap.top_wall_amount

        # 計算安全地板（Hard Floor）
        # 不能低於 FRR 或 最低年化利率（取兩者較大值）
        min_daily_rate = self._config.MIN_RATE_ANNUAL / 365
        floor_rate = max(frr, min_daily_rate)

        logger.info("=" * 50)
        logger.info(f"[市場] BTC=${snap.btc_price:.0f} vol={vol*100:.1f}%")
        logger.info(f"[市場] FRR={frr:.8f} ({frr*365*100:.2f}%/年) TopWall={wall_rate:.6f}")
        logger.info(f"[安全] HardFloor={floor_rate:.8f} ({floor_rate*365*100:.2f}%/年)")

        # 判斷波動率狀態
        if vol < self._config.LOW_VOL_THRESHOLD:
            vol_state = "低波動"
        elif vol > self._config.HIGH_VOL_THRESHOLD:
            vol_state = "高波動"
        else:
            vol_state = "中波動"

        logger.info(f"[狀態] 波動率={vol_state}")

        # 決定策略狀態
        new_state = self._decide_state(vol, frr, wall_rate, wall_amount)

        # 檢查狀態切換 → 觸發撤單
        if new_state != self._state:
            self._on_state_change(new_state)

        self._state = new_state

        # 計算預計掛單（帶安全檢查）
        plan = self._calculate_plan(frr, wall_rate, wall_amount, floor_rate)
        self._latest_plan = plan

        # 印出結果
        period_name = {2: "2天", 7: "7天", 15: "15天"}
        logger.info(f"[預計] 利率={plan.rate:.8f} ({plan.rate*365*100:.2f}%/年)")
        logger.info(f"[預計] 金額=${plan.amount:.0f} 天數={period_name.get(plan.period_days, plan.period_days)}")

        if not plan.is_secure:
            logger.warning(f"[安全] ⚠️ 利率已被安全floor保護提升！")

        logger.info(f"[策略] 狀態={self._state.value} ({self._state.name})")

    # =========================================================================
    # 狀態決策
    # =========================================================================

    def _decide_state(self, vol: float, frr: float, wall_rate: float, wall_amount: float) -> StrategyState:
        """
        根據市場數據決定策略狀態

        邏輯：
        - vol < LOW_VOL_THRESHOLD（15%）→ FRR_BOTTOM（低波動保守）
        - vol > HIGH_VOL_THRESHOLD（40%）→ LADDER（高波動積極）
        - vol 在中間：
          - wall_amount >= 1000 且 wall_rate > frr → GRAB_WALL（搶牆套利）
          - 否則 → WATCH（觀望）

        參數：
            vol: 波動率（小數）
            frr: FRR 日利率（小數）
            wall_rate: 牆單利率（小數）
            wall_amount: 牆單金額

        回傳：
            建議的策略狀態
        """
        cfg = self._config

        if vol < cfg.LOW_VOL_THRESHOLD:
            return StrategyState.FRR_BOTTOM
        elif vol > cfg.HIGH_VOL_THRESHOLD:
            return StrategyState.LADDER
        else:
            # 中波動：看牆單
            if wall_amount >= 1000 and wall_rate > frr:
                return StrategyState.GRAB_WALL
            else:
                return StrategyState.WATCH

    # =========================================================================
    # 狀態切換處理
    # =========================================================================

    def _on_state_change(self, new_state: StrategyState):
        """
        狀態改變時觸發撤單

        實作 Rate Limit：避免短時間多次撤單（冷卻 30 秒）

        參數：
            new_state: 新的策略狀態
        """
        now = time.time()

        # Rate Limit：避免重複撤單
        if now - self._last_cancel_time < self._cancel_cooldown:
            logger.info("[策略] 撤單冷卻中，跳過")
            return

        old = self._prev_state
        self._prev_state = self._state

        logger.warning("=" * 50)
        logger.warning(f"[Action] 狀態切換！ {old.value} → {new_state.value}")

        if self._cancel_callback:
            try:
                logger.warning(f"[Action] 執行撤單...")
                self._cancel_callback()
                logger.warning(f"[Action] 撤單完成！")
                self._last_cancel_time = time.time()
            except Exception as e:
                logger.error(f"[Action] 撤單失敗: {e}")
        else:
            logger.warning(f"[Action] 撤銷所有未成交掛單！（無回調）")

        logger.warning("=" * 50)

    # =========================================================================
    # 掛單計算（帶安全檢查）
    # =========================================================================

    def _calculate_plan(self, frr: float, wall_rate: float, wall_amount: float, floor_rate: float) -> PlannedOffer:
        """
        計算預計掛單參數，帶安全檢查

        各狀態的掛單策略：
        - FRR_BOTTOM: FRR × 1.10，每筆 300 USD，2 天
        - LADDER: 取 FRR 和 Wall 的平均值，每筆 300 USD，7 天
        - GRAB_WALL: Wall × 0.98，取 Wall 的 30%（最少 150 USD），2 天
        - WATCH: 不掛單

        安全檢查：
        1. Hard Floor：利率不能低於 floor_rate
        2. 金額上限：不能超過 MAX_OFFER_USD

        參數：
            frr: FRR 日利率（小數）
            wall_rate: 牆單利率（小數）
            wall_amount: 牆單金額
            floor_rate: 安全地板利率（小數）

        回傳：
            PlannedOffer：預計掛單參數
        """
        cfg = self._config
        is_secure = True

        if self._state == StrategyState.FRR_BOTTOM:
            # FRR × 1.10，每筆最大金額，2 天
            rate = frr * cfg.FRR_PREMIUM
            amount = cfg.MAX_OFFER_USD
            period = 2

        elif self._state == StrategyState.LADDER:
            # 取 FRR 和 Wall 的平均值（如果 Wall 高於 FRR）
            if wall_rate > frr:
                rate = (frr + wall_rate) / 2
            else:
                rate = frr * 1.1
            amount = cfg.MAX_OFFER_USD
            period = 7

        elif self._state == StrategyState.GRAB_WALL:
            # Wall × 0.98，取 Wall 的 30%（最少 150 USD）
            rate = wall_rate * 0.98
            amount = min(wall_amount * 0.3, cfg.MAX_OFFER_USD)
            amount = max(amount, cfg.MIN_OFFER_USD)
            period = 2

        else:  # WATCH
            rate = frr
            amount = 0
            period = 2

        # [安全檢查 1] Hard Floor
        if rate < floor_rate:
            rate = floor_rate
            is_secure = False
            logger.warning(f"[安全] HardFloor觸發：{rate:.8f}")

        # [安全檢查 2] 金額限制
        amount = min(amount, cfg.MAX_OFFER_USD)

        return PlannedOffer(
            rate=rate,
            amount=amount,
            period_days=period,
            state=self._state,
            is_secure=is_secure
        )


# =============================================================================
# 測試主程式
# =============================================================================

def main():
    import logging
    import market_monitor

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    mon = market_monitor.MarketMonitor()
    mon.start()

    engine = StrategyEngine(mon)
    engine.start()

    try:
        while True:
            time.sleep(15)
            state = engine.get_state()
            plan = engine.get_plan()
            print(f"\n{'='*50}")
            print(f"當前策略: {state.value} ({state.name})")
            if plan:
                print(f"預計掛單: ${plan.amount:.0f} @ {plan.rate:.8f} = {plan.rate*365*100:.2f}%/年")
                print(f"安全: {'✅' if plan.is_secure else '⚠️'}")
    except KeyboardInterrupt:
        engine.stop()
        mon.stop()


if __name__ == "__main__":
    main()
