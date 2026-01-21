# Integration Daemon

整合端為常駐工作流程服務：接收 edge 事件、依據工作階段切換 pipeline，並將狀態回報至 monitoring。

## 文件

- [Core Workflow（預設流程）](docs/CORE_WORKFLOW.md)
- [環境變數說明](docs/ENV.md)
- [MC-MOT 模組說明](src/integration/mcmot/README.md)

## 快速開始

```bash
cd integration
cp .env.example .env
uv venv --python /usr/bin/python3.10
source .venv/bin/activate
uv pip install -r requirements.txt
python main.py
```

<<<<<<< HEAD
## 目錄概覽
=======
### 使用 pip 安裝

若無法使用 uv，也可改用傳統 `pip`：

```bash
cd integration
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python main.py
```

會自動啟動 `/edge/events` HTTP 伺服器（預設 0.0.0.0:9000），並根據 `APP_TIMEZONE` 及 `working_windows` 控制 pipeline 切換。環境變數可直接在 shell 設定或撰寫 `.env` 後 `export`。
`integration/main.py` 在啟動時會自動讀取同目錄的 `.env`（如存在），若某變數已由系統環境設定則以系統值為準。

常見參數：

| 變數 | 預設 | 用途 |
| --- | --- | --- |
| `APP_TIMEZONE` | `Asia/Taipei` | Pipeline 排程時區。|
| `LOOP_INTERVAL_SECONDS` | `5` | 工作階段 workflow 間隔。|
| `NON_WORKING_IDLE_SECONDS` | `30` | 供傳統非工作流程使用的預設 idle 秒數，可由 selector/pipeline 自行決定是否採用。|
| `EDGE_EVENT_PORT` | `9000` | HTTP 伺服器對外 Port。|
| `MONITOR_ENDPOINT` | *(未設定)* | Monitoring 服務 base URL。|
| `INTEGRATION_MONITOR_SERVICE_NAME` | `integration-daemon` | 回報監控時使用的 service name。|
| `LOG_LEVEL` | `INFO` | Python logging 輸出等級（`DEBUG`/`INFO`/`WARNING`...）。|
| `MCMOT_ENABLED` | `1` | 是否啟用 MC-MOT 追蹤整合。|
| `MCMOT_CONFIG_PATH` | `data/config/mcmot.config.yaml` | MC-MOT config 檔案路徑（可使用相對於 repo root 的路徑）。|
| `SW_CORE_ROOT` | *(自動設定)* | integration core 根目錄，啟動時預設為 `integration/` 目錄；若 core 以 submodule 使用，可覆寫此路徑以確保相對路徑正確。|
| `INGESTION_HANDLER_CLASS` | *(未設定)* | 指定 Ingestion handler 類別，需繼承 `BaseIngestionHandler`；未設定使用預設正規化邏輯。|
| `TRACKING_ENGINE_CLASS` | *(未設定)* | 指定追蹤 handler 類別，需繼承 `BaseTrackingHandler`；未設定使用內建 `MCMOT` 處理流程。|
| `FORMAT_TASK_ENABLED` | `1` | 是否插入格式轉換節點；設為 `0` 時 rules 直接讀取 MC-MOT 原始資源。|
| `FORMAT_STRATEGY_CLASS` | *(未設定)* | 指定格式轉換引擎類別（`package.module:Class`），未設定使用內建摘要邏輯。|
| `RULES_ENGINE_CLASS` | *(未設定)* | 指定規則引擎類別（`package.module:Class`），需繼承 `BaseRuleEngine`；未設定則使用內建 `DefaultRuleEngine`。|
| `RULES_DETAIL` | *(未設定)* | 為規則節點附加的描述字串，僅用於 log。|
| `EVENT_DISPATCH_ENGINE_CLASS` | *(未設定)* | 指定事件派送引擎（`package.module:Class`），負責將規則/系統事件輸出到外部 API 或資料庫；未設定使用只寫 log 的預設實作。|
| `PIPELINE_SELECTOR_CLASS` | *(未設定)* | 指定 `BasePipelineSelector` 子類，決定每輪迴圈要執行哪個 pipeline；未設定則使用內建 `WorkingHoursSelector`（永遠執行 core 的 working pipeline）。|
| `PIPELINE_TASK_CLASSES` | *(未設定)* | 以 `name=package.module:Class` 形式註冊額外 pipeline，多個條目以逗號分隔；可用 selector 返回對應名稱以指派自訂 pipeline。|
| `PIPELINE_SLEEP_SECONDS` | *(未設定)* | 以 `name=seconds` 形式指定 pipeline 預設 loop interval，例如 `working=0.1,off_hours=15`；當 pipeline/selector 沒有回傳 `sleep` 時就會使用對應值。|
| `GLOBAL_MAP_VIS_ENABLED` | `0` | 啟用全局地圖可視化（僅在 MC-MOT 有 map 設定時生效）。|
| `GLOBAL_MAP_VIS_MODE` | `write` | `write` 只輸出檔案、`show` 只顯示視窗、`both` 同時進行。|
| `GLOBAL_MAP_VIS_OUTPUT` | `output/global_map` | 當 mode 包含 `write` 時輸出的目錄。|
| `GLOBAL_MAP_VIS_WINDOW` | `global-map` | `show` 模式下的視窗名稱。|
| `GLOBAL_MAP_VIS_CAMERAS` | *(未設定)* | 限制要顯示 local overlay 的 edge/camera ID，逗號分隔，預設顯示全部。|
| `GLOBAL_MAP_VIS_SHOW_LEGEND` | `1` | 是否在地圖上顯示相機圖例及顏色對照。|
| `GLOBAL_MAP_VIS_GLOBAL_COLOR` | *(未設定)* | 指定全域物件顏色（`#RRGGBB`），若未設定則依 class palette。|
| `GLOBAL_MAP_VIS_CLASS_COLORS` | *(未設定)* | 以 `class:#FF0000,...` 指定 class 對應顏色。|
| `GLOBAL_MAP_VIS_GLOBAL_RADIUS_RATIO` | `0.008` | 全域點大小佔地圖最小邊比例，會自動依地圖尺寸調整。|
| `GLOBAL_MAP_VIS_LOCAL_RADIUS_RATIO` | `0.004` | Local 點大小佔比（會自動小於全域點）。|

> `GLOBAL_MAP_VIS_CLASS_COLORS` 以 `class:#RRGGBB` 形式設定多組值，例如 `person:#00FF00,forklift:#FF00FF`。若同時指定 `GLOBAL_MAP_VIS_GLOBAL_COLOR` 則作為預設顏色。

### MC-MOT 設定檔（`data/config/mcmot.config.yaml`）

整合端的核心資料（相機位置、TPS/Homography 模型、忽略區域、全局地圖）皆在此 YAML 檔配置。請根據實際部署逐項調整：

> 啟動時會自動設定 `SW_CORE_ROOT` 為 integration 目錄，`MCMOT_CONFIG_PATH` 及 YAML 內的檔案欄位（`coordinate_matrix_ckpt`、`ignore_polygons`、`map.image_path` 等）若使用相對路徑，會以該 config 檔所在目錄為基準解析，無須受執行目錄影響。

- `system`：座標轉換模式、時間設定等全域參數。
- `cameras[]`：每個攝影機的 `camera_id`/`edge_id`、TPS/Homography 權重路徑、ignore polygons 等。
- `tracking`：可追蹤類別、matching threshold、軌跡遺失限制、距離閾值（若配合 `map` 會自動換算公尺）。
- `map`/`global_map_visualization`：全局地圖影像路徑、像素與公尺比例、顏色設定。

若實際場域資料不便公開，可將此檔案改為示例版本並提供欄位說明；部署時再以私有設定覆寫 `MCMOT_CONFIG_PATH`。

## 目錄結構
>>>>>>> af0d02a71a2e4d67914435c97c22deb90ef8dc66

```
integration/
├── main.py
├── .env(.example)
├── src/integration/
├── data/
└── README.md
```

<<<<<<< HEAD
## 子專案使用
=======
> 注意：為避免洩露實際場域資訊，`data/coordinate_models/` 與 log/output 等檔案不會隨 repo 發佈。請依據實際部署填入對應的 TPS 模型、地圖與忽略區域；可參考 `data/coordinate_models/README`（若自訂）或 `mcmot.config.yaml` 說明欄位需求。

程式碼與資料分離後，只要把 `src/` 加入 `PYTHONPATH`（`main.py` 已自動處理），即可維持 `from integration.xxx import` 的匯入方式。部署到容器或其他環境時，也只需同步 `src/` 與 `data/` 即可。

## Pipeline 與插件

Working pipeline 的節點順序、PipelineRegistry/selector、事件派送流程等細節已整理在 [`src/integration/pipeline/README.md`](src/integration/pipeline/README.md)，請參考該文件取得更完整的架構說明。

重點摘要如下：

- Working pipeline 順序固定為 **Ingestion → MCMOT → (Format) → RuleEvaluation → EventDispatch**，所有自訂行為皆透過 handler/engine 插槽（`INGESTION_HANDLER_CLASS`、`TRACKING_ENGINE_CLASS`、`FORMAT_STRATEGY_CLASS`、`RULES_ENGINE_CLASS`、`EVENT_DISPATCH_ENGINE_CLASS`）注入。
- 自訂 pipeline 可藉由 `PIPELINE_TASK_CLASSES` 註冊，selector（`PIPELINE_SELECTOR_CLASS`）只需回傳已註冊的名稱即可切換；預設 `WorkingHoursSelector` 永遠回傳 `working`。
- 透過 `PIPELINE_SLEEP_SECONDS` 或 selector metadata 的 `sleep` 欄位可細緻控制各 pipeline 的 loop interval。若 selector/pipeline 未設定 `sleep`，Registry 中的預設值會交由 `PhaseTask` 套用。
- selector 或 RuleEngine 若要輸出事件，可使用 `integration.pipeline.events.enqueue_event()` 推入 queue；`EventDispatchTask` 會統一呼叫對應的 dispatch engine 對外送出。

MC-MOT 設定檔定義座標映射與匹配參數：

- `system.coordinate_transform_mode`：決定使用 TPS 或 Homography 進行座標轉換。
- `tracking`：除了 `trackable_classes` 外，新增 `match_threshold`（成本閾值）、`max_traj_loss`（軌跡損失正規化上限）以及 `distance_threshold_m`（可選，用於限制 local/global 物件需在某公尺範圍內才算匹配）。
- `map`：描述全局地圖影像尺寸（像素）與實際公尺的對應，若設定 `distance_threshold_m` 會自動使用此比例換算距離。
- `global_map_visualization`（由上述環境變數控制）：依據 map 配置載入場域平面圖並將 MC-MOT 的 global objects 繪製其上，可選擇輸出檔案或於開發機顯示視窗，方便驗證映射與配對結果。
- `cameras[]`：為每個攝影機設定下列欄位：
  - `camera_id`：整合端使用的統一名稱，用於全局地圖、追蹤紀錄。
  - `edge_id`：Edge 端上報結果時的識別（即 `EDGE_CAMERA_ID`），integration 以此區分來源。
  - `enabled`：是否啟用該攝影機（可暫時停用某些攝影機而不刪除設定）。
  - `coordinate_matrix_ckpt` / `homography_ckpt`：座標轉換模型檔案路徑。
  - `ignore_polygons`：可選的忽略區域設定，格式以 `.npy` 檔儲存多邊形座標。

這些欄位會在啟動時載入後快取，提供座標轉換、忽略區域與匹配邏輯所需的參數。

## Docker Compose

```bash
cd integration
cp .env.example .env   # 視需求調整

docker compose up --build
```

映像同樣只包含 `common/` 與 `integration/` 目錄並於 `/svc/integration` 執行 `python main.py`。若 monitoring/edge 於不同網段，請調整 `.env` 內的 URL（例如 `http://host.docker.internal:9400`）。

> 共用網路：compose 會將服務連到外部 `smartware_net` network，請先啟動 streaming/docker-compose 或手動 `docker network create smartware_net`。

> MC-MOT 程式碼已整合至 `src/integration/mcmot`，預設設定讀取 `data/config/mcmot.config.yaml`。若需自訂，可掛載 volume 或調整 `MCMOT_CONFIG_PATH` 指向新的 YAML。

## 作為子專案核心（Submodule）使用

若想在其他專案沿用此整合核心（例如只替換規則/格式轉換邏輯），可參考以下流程；假設子專案名為 `warehouse-alerts`：

1. **導入 core**：在 `warehouse-alerts` repo 根目錄執行 `git submodule add <core_repo> integration`，或直接把 `integration/` 目錄複製進來，並確保 `integration/src/` 在 `PYTHONPATH`。
2. **安裝依賴**：進到 `integration/` 執行 `uv pip install -r requirements.txt`（或傳統 `pip install -r ...`），讓 `smart_workflow`、MC-MOT 依賴就緒。
3. **準備 `.env`**：在 `integration/` 底下 `cp .env.example .env` 作為基底，再在 `warehouse-alerts/.env` 中只列出需要覆寫的變數（例如 `RULES_ENGINE_CLASS=warehouse_alerts.rules.engine:AlertRuleEngine`）。啟動時可依序載入兩份 `.env`。
4. **實作插件類別**：在 `warehouse_alerts/rules/engine.py` 等檔案中實作 `BaseRuleEngine`/`BaseFormatEngine` 等子類，並在 `warehouse_alerts/.env` 設定對應的 class path；若需要自訂 ingestion 或 tracking，也可提供 `BaseIngestionHandler`/`BaseTrackingHandler` 子類。
5. **撰寫啟動腳本**：在 `warehouse_alerts/main.py` 內載入 core `.env` 與子專案 `.env`（可參考 `examples/rule_plugin_demo/main.py`），最後呼叫 `integration.main.main()`。此腳本同時可以設定子專案自有資源（DB/通知服務等）。
>>>>>>> af0d02a71a2e4d67914435c97c22deb90ef8dc66

若作為子模組導入，請確保 `integration/src` 在 `PYTHONPATH`，並於 `.env` 設定
`PIPELINE_SCHEDULE_PATH` 及需要的 plugin class path。
