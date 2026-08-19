# Bitfinex Funding Bot

> Bitfinex 自動放貸機器人 — 透過 Telegram 控制，階梯掛單、FRR 策略、利息監控

## 功能

- **自動放貸**：根據利率條件自動在 Bitfinex 掛出階梯式借貸訂單
- **FRR 跟單**：追蹤市場成交利率，自動調整第一筆借貸到最新成交價
- **Telegram 控制**：隨時查看錢包、利率、活躍訂單、下線/上線
- **利息監控**：每 15 分鐘結算並通知累計利息收入
- **多帳戶支援**：可同時管理多個 Bitfinex 子帳戶

## 快速開始

### 1. 安裝依賴

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. 設定

在 `config/settings.yaml` 填入你的 Bitfinex API Key：

```yaml
api:
  key: "你的_API_KEY"
  secret: "你的_API_SECRET"
  testnet: false

telegram:
  bot_token: "你的_Telegram_Bot_Token"
  chat_id: "你的_Telegram_Chat_ID"
```

### 3. 啟動

```bash
source venv/bin/activate
python telegram_bot.py
```

## 利率策略說明

| 年化利率 | 借貸天期 |
|----------|----------|
| > 14.5%  | 120 天   |
| > 12%    | 7 天     |
| ≤ 12%    | 2 天     |

利率為小數形式（如 `0.00031205`），顯示給用戶時自動乘 100 轉換為百分比。

## 架構

```
funding_bot/
├── telegram_bot.py      # 主程式，Telegram 命令處理
├── funding_monitor.py   # 自動放貸核心邏輯
├── funding_logic.py     # 階梯掛單策略
├── bitfinex_monitor.py # 多帳戶錢包/offer 監控
├── bitfinex_ws.py      # Bitfinex WebSocket（認證 + 推送）
├── telegram_ws.py      # Telegram Long Polling 網路層
├── market_monitor.py   # 市場利率牆監控
├── market_db.py        # SQLite 本地資料庫
├── strategy_engine.py  # 策略狀態機
├── backtester.py       # 回測框架
└── config/
    └── settings.yaml   # 設定（API Key，請勿 commit）
```

## 注意事項

- **API Key 請勿 commit** 到公開倉庫
- 請先在 Bitfinex testnet 驗證策略邏輯
- 借貸有風險，請自行評估
