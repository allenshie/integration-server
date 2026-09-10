# App 與 MCMOT 的匹配摘要日誌

這份文件說明 App 與 MCMOT 在匹配摘要上的分工，以及如何在 App 啟動時
看到 MCMOT 提供的詳細表格。

## 責任分工

App 負責 pipeline 層級的運行統計，例如：

- MCMOT 是否成功完成，以及 watermark 是否前進；
- 目前 active global object 數量；
- matching attempt、cadence skip 與失敗次數；
- pipeline 的延遲與吞吐量。

MCMOT 負責匹配細節。每次真正的 typed matching attempt 由 MCMOT 建立
`MatchingAttemptReport`，並可輸出該次任務、各 camera、各 local object 的
處理結果。App 只把 `attempt_report` 當成 opaque result 傳遞，不解讀
matching decision、candidate ID 或 global assignment，也不在 App 重新建立
一套匹配 renderer。

## 啟用 MCMOT 詳細摘要

在 App 根目錄的 `.env` 設定：

```text
MCMOT_MATCHING_SUMMARY=1
```

也可以使用 `true`、`yes` 或 `on`；未設定時預設關閉。使用 App 正式入口
`./scripts/start.sh` 啟動時，App 會先載入根目錄 `.env`，再建立 integration
與 MCMOT，因此上述設定會傳到實際執行 process。

若直接在 `integration_core` 目錄執行 `uv run` 的 Python 指令或測試，uv 不會
因為看到 App 根目錄的 `.env` 就自動載入它。這種情況請先執行：

```bash
export MCMOT_MATCHING_SUMMARY=1
```

或在同一個 shell 載入正確的 `.env`，再啟動指令。只修改檔案但沒有讓它進入
process 環境，MCMOT 不會輸出摘要。

## 什麼時候會看到表格

MCMOT 只在真正建立並執行 typed matching attempt 後輸出一份摘要，會等該次
任務收集完 camera 結果後才輸出。一次 attempt 會先有整體統計，再為每個
camera 輸出一張物件表；表格會列出正式匹配、快速匹配、candidate、filtered、
rejected、unmatched 與 error 等結果，並顯示 `match_cost` 與 `reason`（若有）。

以下情況維持安靜，不會產生匹配摘要：

- cadence 尚未到期；
- 沒有 pending snapshot；
- 該輪只有 maintenance；
- 使用 legacy `process_events()` 路徑；
- matching attempt 建立前的 schema 錯誤。

因此 `pipeline_summary` 只看到 `mc_mot` 的窗口統計，並不表示 MCMOT 的詳細
摘要一定會出現；要同時滿足環境變數已進入 process，且該輪確實建立 typed
matching attempt。

完整欄位與 decision 說明請見 MCMOT 專案的
[匹配摘要日誌文件](../../../MCMOT/docs/MATCHING_SUMMARY.md)。
