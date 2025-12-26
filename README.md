# Integration Daemon

整合端為常駐工作流程服務：接收 edge 事件、依據工作時段切換 pipeline，並將狀態回報至 monitoring。現在可以在 `integration/` 目錄內直接啟動（`python main.py`），或透過 Docker 容器部署。

## 快速開始

```bash
cd integration
cp .env.example .env          # 複製後依部署調整 monitoring/MCMOT 等參數
uv venv --python /usr/bin/python3.12
source .venv/bin/activate
uv pip install -r requirements.txt
python main.py                # 僅需 LOG_LEVEL/monitoring/MCMOT 設定即可啟動
```

若無法使用 `uv`，可將上述步驟改為 `python -m venv .venv` 與 `pip install -r requirements.txt`。所有環境變數皆由 `.env` 讀取，可依 `APP_TIMEZONE`、`EDGE_EVENT_PORT` 等欄位調整對應服務。

## 本機啟動

### 使用 uv 安裝

```bash
cd integration
uv venv --python /usr/bin/python3.12  # 或 python -m venv .venv
source .venv/bin/activate
uv pip install -r requirements.txt
python main.py
```

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
| `NON_WORKING_IDLE_SECONDS` | `30` | 非工作階段 idle 間隔。|
| `EDGE_EVENT_PORT` | `9000` | HTTP 伺服器對外 Port。|
| `MONITOR_ENDPOINT` | *(未設定)* | Monitoring 服務 base URL。|
| `INTEGRATION_MONITOR_SERVICE_NAME` | `integration-daemon` | 回報監控時使用的 service name。|
| `LOG_LEVEL` | `INFO` | Python logging 輸出等級（`DEBUG`/`INFO`/`WARNING`...）。|
| `MCMOT_ENABLED` | `1` | 是否啟用 MC-MOT 追蹤整合。|
| `MCMOT_CONFIG_PATH` | `data/config/mcmot.config.yaml` | MC-MOT config 檔案路徑（可使用相對於 repo root 的路徑）。|
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

- `system`：座標轉換模式、時間設定等全域參數。
- `cameras[]`：每個攝影機的 `camera_id`/`edge_id`、TPS/Homography 權重路徑、ignore polygons 等。
- `tracking`：可追蹤類別、matching threshold、軌跡遺失限制、距離閾值（若配合 `map` 會自動換算公尺）。
- `map`/`global_map_visualization`：全局地圖影像路徑、像素與公尺比例、顏色設定。

若實際場域資料不便公開，可將此檔案改為示例版本並提供欄位說明；部署時再以私有設定覆寫 `MCMOT_CONFIG_PATH`。

## 目錄結構

```
integration/
├── main.py                  # 入口
├── Dockerfile / docker-compose.yml / requirements.txt / README.md
├── src/integration/         # 所有 Python 模組 (api, config, pipeline, mcmot, ...)
├── data/
│   ├── coordinate_models/   # TPS 模型、global_map.png、忽略區域
│   └── config/mcmot.config.yaml
├── output/global_map/       # GLOBAL_MAP_VIS_* 產出的快照
├── logs/                    # Loguru / 服務 log
└── .env(.example)
```

> 注意：為避免洩露實際場域資訊，`data/coordinate_models/` 與 log/output 等檔案不會隨 repo 發佈。請依據實際部署填入對應的 TPS 模型、地圖與忽略區域；可參考 `data/coordinate_models/README`（若自訂）或 `mcmot.config.yaml` 說明欄位需求。

程式碼與資料分離後，只要把 `src/` 加入 `PYTHONPATH`（`main.py` 已自動處理），即可維持 `from integration.xxx import` 的匯入方式。部署到容器或其他環境時，也只需同步 `src/` 與 `data/` 即可。

## Pipeline 流程

工作時段內的 pipeline 由三個節點組成（非工作時段則僅執行 `NonWorkingUpdateTask`）：

1. **IngestionTask**：從內建 HTTP server 取得 Edge 推理事件，正規化時間戳、過濾過期資料，並對每個攝影機只保留最新一筆事件。
2. **MCMOTTask**：將事件交給 `MCMOTEngine` 進行座標轉換、跨攝影機追蹤與 global ID 維護。啟用 `GLOBAL_MAP_VIS_*` 後，由 `GlobalMapRenderer` 根據地圖尺寸自動決定標記/字體大小、顏色與圖例（包含「Global」區塊與所有 camera 顏色），並可透過 `GLOBAL_MAP_VIS_CAMERAS` 聚焦特定 edge。詳細配置方式請參閱 [MC-MOT 模組 README](src/integration/mcmot/README.md)。
3. **RuleEvaluationTask**：預留給違規檢查/作業判定等邏輯。預設僅輸出 log，可在 `src/integration/pipeline/tasks/working/rules/` 中擴充自訂規則。

以上節點由 `InitPipelineTask` 在啟動時建立並快取於 `TaskContext`，`PhaseTask` 則依工作時段切換執行哪個 pipeline。若需要 24/7 運作，只要在 `.env` 中把工作時段設為全天（例如 00:00~24:00）或維持預設的單一時段，即可讓系統始終執行 working pipeline。

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
