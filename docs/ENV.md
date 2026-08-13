# 環境變數說明

此文件以表格列出環境變數的預設值與用途。`.env.example` 僅保留核心必填項，其餘變數請按需求加入 `.env`。

## 啟動與執行

| 變數 | 預設 | 說明 |
| --- | --- | --- |
| `APP_TIMEZONE` | `Asia/Taipei` | 應用時區。 |
| `LOG_LEVEL` | `INFO` | 日誌等級。 |
| `CONFIG_SUMMARY` | `0` | 啟動時是否輸出配置摘要。 |
| `LOOP_INTERVAL_SECONDS` | `5` | 主循環間隔秒數。 |
| `PIPELINE_SUMMARY_INTERVAL_SECONDS` | `60` | pipeline 摘要輸出間隔秒數。 |
| `NON_WORKING_IDLE_SECONDS` | `30` | 非工作時段的 idle 秒數。 |
| `RETRY_BACKOFF_SECONDS` | `10` | 錯誤重試退避時間，單位秒。 |
| `PIPELINE_SCHEDULE_PATH` | `-` | pipeline schedule 設定檔路徑；通常由主專案 `.env` 或啟動流程注入。 |
| `CONFIG_ROOT` | `integration root` | 相對路徑解析基準；未設定時回退到 integration root。 |
| `MONITOR_ENDPOINT` | `-` | Monitoring service endpoint。 |
| `INTEGRATION_MONITOR_SERVICE_NAME` | `integration-daemon` | 上報 monitoring 時使用的 service 名稱。 |

## 排程與工作階段

| 變數 | 預設 | 說明 |
| --- | --- | --- |
| `PHASE_ENGINE_CLASS` | `-` | PhaseEngine 類別路徑（`module:Class`）；未設定時使用 `TimeBasedPhaseEngine`。 |
| `SCHEDULER_ENGINE_CLASS` | `-` | SchedulerEngine 類別路徑（`module:Class`）；未設定時使用 `SinglePhaseSchedulerEngine`。 |
| `PHASE_STABLE_SECONDS` | `180` | `DebouncedPhaseEngine` 的穩定時間窗，單位秒。 |
| `EDGE_EVENT_STALE_SECONDS` | `0` | 超過此秒數未收到新 edge 事件視為 stale；`0` 表示關閉。 |
| `EDGE_EVENT_STALE_MODE` | `freeze` | stale 時的處理模式：`freeze` 保留穩定 phase，`unknown` 則回傳未知 phase。 |
| `EDGE_EVENT_UNKNOWN_PHASE` | `unknown` | `EDGE_EVENT_STALE_MODE=unknown` 時使用的 phase 名稱。 |

## Pipeline 插件

| 變數 | 預設 | 說明 |
| --- | --- | --- |
| `INGESTION_ENGINE_CLASS` | `-` | Ingestion engine 類別路徑（`module:Class`）。 |
| `INGESTION_HANDLER_CLASS` | `-` | 舊鍵名；相容用途，建議改用 `INGESTION_ENGINE_CLASS`。 |
| `FORMAT_STRATEGY_CLASS` | `-` | Format engine 類別路徑（`module:Class`）。 |
| `RULES_ENGINE_CLASS` | `-` | Rule engine 類別路徑（`module:Class`）。 |
| `EVENT_DISPATCH_ENGINE_CLASS` | `-` | 事件派送 engine 類別路徑（`module:Class`）。 |
| `PHASE_CHANGE_ENGINE_CLASS` | `-` | Phase 變更處理 engine 類別路徑（`module:Class`）。 |
| `RULES_DETAIL` | `-` | 規則節點額外描述，僅用於 log。 |

## MC-MOT

- `MCMOT_ENABLED`：`0/1` 或 `true/false`。是否啟用 MC-MOT。預設 `1`。
- `MCMOT_TRACKING_CONFIG_PATH`：字串路徑。MCMOT tracking 設定檔位置。
- `MCMOT_CAMERA_CONFIG_PATH`：字串路徑。MCMOT camera / scene 設定檔位置。
- 啟用 MC-MOT 時兩個路徑都必須明確設定；不再支援舊的 `MCMOT_CONFIG_PATH` 單一配置或 `road_config.yaml` fallback。
- 路徑若為相對路徑，會以 `CONFIG_ROOT`（未設定時為 integration_core root）為基準解析。
- 兩個實際設定檔由部署環境提供，不納入 `integration_core` repo；MCMOT 內部 asset 路徑仍由 MCMOT 自己依其設定規則處理。
- MC-MOT 的執行實體固定為 `MCMOTEngine`，不再提供 tracking handler 類別覆寫。

## 視覺化（可選）

全域地圖視覺化已從 `MCMOT` 子模組分離，`integration_core` 只負責自己的視覺化設定。
啟用後，`load_config()` 會先讀取 `GLOBAL_MAP_VIS_CONFIG_PATH` 指向的 YAML，再建出 `GlobalMapVisualizationConfig` 並掛到 `AppConfig` 上。
範例檔可參考 `data/config/global_map_vis.example.yaml`。

| 變數 | 預設 | 說明 |
| --- | --- | --- |
| `GLOBAL_MAP_VIS_ENABLED` | `0` | 是否啟用全域地圖視覺化。 |
| `GLOBAL_MAP_VIS_CONFIG_PATH` | `-` | 視覺化 YAML 路徑，相對路徑以 integration root 為基準；啟用視覺化時必填。 |

視覺化 YAML 範例：

```yaml
map:
  image_path: data/assets/global_map.png
  width_meters: 120.0
  height_meters: 60.0

render:
  mode: write
  output_dir: output/global_map
  window_name: global-map
  marker_radius: 6
  label_font_scale: 0.5
  label_thickness: 1
  show_global_id: true
  show_class_name: false
  show_legend: true
  global_radius_ratio: 0.008
  local_radius_ratio: 0.004

cameras:
  - camera_id: cam01
    display_name: Camera 1
    aliases: [cam01, edge_cam01]
```

- `map.image_path`：底圖路徑，renderer 會依此載入全域地圖。
- `map.width_meters / map.height_meters`：地圖對應的實際尺寸。
- `render.*`：由 `GlobalMapRenderer` 自己消費的顯示參數，顏色由 renderer 內建規則自動分配，不需要手動提供色碼。
- `cameras`：至少提供 `camera_id`；`display_name` 與 `aliases` 為選填，用於圖例與別名匹配。

## 通訊用途參數

| 變數 | 預設 | 說明 |
| --- | --- | --- |
| `EDGE_EVENT_BACKEND` | `http` | edge 事件接收協議：`http` 或 `mqtt`。 |
| `EDGE_EVENT_HOST` | `0.0.0.0` | edge event HTTP server host。 |
| `EDGE_EVENT_PORT` | `9000` | edge event HTTP server port。 |
| `EDGE_EVENTS_TOPIC` | `edge/events` | edge 事件通道名稱。 |
| `EDGE_EVENT_MAX_AGE` | `5` | 超過此秒數的 edge 事件會被丟棄。 |
| `PHASE_BROADCAST_ENABLED` | `1` | 是否啟用 phase 廣播；關閉後仍會計算 phase 與執行 pipeline，但不會送出 phase 訊息。 |
| `PHASE_PUBLISH_BACKEND` | 跟隨 `EDGE_EVENT_BACKEND` | phase 廣播協議：`mqtt` 或 `http`；未設定時會沿用 `EDGE_EVENT_BACKEND`。 |
| `PHASE_TOPIC` | `integration/phase` | phase 廣播通道名稱。 |
| `PHASE_HEARTBEAT_SECONDS` | `600` | phase 心跳重送間隔，單位秒。 |
| `MATCHING_BROADCAST_ENABLED` | `0` | 是否啟用 matching 結果廣播；關閉後仍會執行 MC-MOT 與後續流程，但不會對外廣播 matching snapshot。 |
| `MATCHING_BROADCAST_BACKEND` | 跟隨 `PHASE_PUBLISH_BACKEND` | matching 結果廣播協議：`mqtt` 或 `http`；未設定時會沿用 `PHASE_PUBLISH_BACKEND`。 |
| `MATCHING_BROADCAST_TOPIC` | `integration/matching` | matching 結果廣播通道名稱；`MATCHING_BROADCAST_CHANNEL` 為相容別名。 |
| `MATCHING_BROADCAST_RECORDING_ENABLED` | `0` | 是否同時將 matching observation 寫入 JSONL sidecar。 |
| `MATCHING_BROADCAST_RECORDING_PATH` | `output/matching/matching_observations.jsonl` | matching observation JSONL 輸出路徑；`MATCHING_BROADCAST_RECORD_PATH` 與 `MATCHING_BROADCAST_OBSERVATION_PATH` 為相容別名。 |

## MQTT 協議參數

| 變數 | 預設 | 說明 |
| --- | --- | --- |
| `MQTT_ENABLED` | `0` | 是否啟用 MQTT 能力。 |
| `MQTT_HOST` | `localhost` | MQTT broker host。 |
| `MQTT_PORT` | `1883` | MQTT broker port。 |
| `MQTT_QOS` | `1` | MQTT QoS 層級。 |
| `MQTT_RETAIN` | `1` | 是否保留最後狀態。 |
| `MQTT_CLIENT_ID` | `-` | MQTT client id，可選。 |
| `MQTT_AUTH_ENABLED` | `0` | 是否啟用 MQTT 帳密驗證。 |
| `MQTT_USERNAME` | `-` | MQTT 使用者名稱；`MQTT_AUTH_ENABLED=1` 時必填。 |
| `MQTT_PASSWORD` | `-` | MQTT 密碼；建議透過 Secret 或 env 注入。 |

## HTTP 協議參數

| 變數 | 預設 | 說明 |
| --- | --- | --- |
| `PHASE_HTTP_BASE_URL` | `-` | HTTP base URL，例如 `http://localhost:9001`。 |
| `PHASE_HTTP_TIMEOUT_SECONDS` | `5` | HTTP timeout 秒數。 |

## 協議設定範例

### phase 發佈（MQTT）

```bash
MQTT_ENABLED=1
MQTT_HOST=localhost
MQTT_PORT=1883
PHASE_TOPIC=integration/phase
MQTT_QOS=1
MQTT_RETAIN=1
MQTT_AUTH_ENABLED=1
MQTT_USERNAME=allen
MQTT_PASSWORD=allen
PHASE_HEARTBEAT_SECONDS=600
PHASE_PUBLISH_BACKEND=mqtt
```

### matching 結果廣播（HTTP / MQTT）

```bash
# HTTP 模式
MATCHING_BROADCAST_ENABLED=1
MATCHING_BROADCAST_BACKEND=http
MATCHING_BROADCAST_TOPIC=integration/matching

# MQTT 模式
MATCHING_BROADCAST_ENABLED=1
MATCHING_BROADCAST_BACKEND=mqtt
MATCHING_BROADCAST_TOPIC=integration/matching

# matching observation sidecar
MATCHING_BROADCAST_RECORDING_ENABLED=1
MATCHING_BROADCAST_RECORDING_PATH=output/matching/matching_observations.jsonl
```

### edge events 接收（HTTP / MQTT）

```bash
# HTTP 模式
EDGE_EVENT_BACKEND=http
EDGE_EVENT_HOST=0.0.0.0
EDGE_EVENT_PORT=9000
PHASE_PUBLISH_BACKEND=http
PHASE_HTTP_BASE_URL=http://localhost:9001

# MQTT 模式
EDGE_EVENT_BACKEND=mqtt
EDGE_EVENTS_TOPIC=edge/events
MQTT_HOST=localhost
MQTT_PORT=1883
MQTT_AUTH_ENABLED=1
MQTT_USERNAME=allen
MQTT_PASSWORD=allen
PHASE_PUBLISH_BACKEND=mqtt
PHASE_TOPIC=integration/phase
```

## 協議設定建議

| 項目 | 建議 | 說明 |
| --- | --- | --- |
| Edge / phase 協議 | 同步 | `EDGE_EVENT_BACKEND` 與 `PHASE_PUBLISH_BACKEND` 最好使用相同協議，維持 edge 與 integration core 的通訊一致性。 |
| 協議分離 | 例外處理 | 只有在特殊整合需求下才分離兩者協議。 |
| 設定組裝 | 自動 | integration core 會根據用途參數與協議參數組裝對應的 messaging 設定。 |
| 新增協議 | 參考文件 | 若需新增協議，請參考 `smart_messaging_core` 的協議文件與 route 設計。 |

## 執行節奏

| 變數 | 預設 | 說明 |
| --- | --- | --- |
| `LOOP_INTERVAL_SECONDS` | `5` | 主循環間隔秒數。 |
| `RETRY_BACKOFF_SECONDS` | `10` | 錯誤重試退避時間，單位秒。 |
| `CONFIG_SUMMARY` | `0` | 啟動時是否輸出配置摘要（engine / pipeline / phase broadcast）。 |

## 健康檢查（K8s Probe）

| 變數 | 預設 | 說明 |
| --- | --- | --- |
| `INTEGRATION_HEALTH_SERVER_ENABLED` | `0` | 是否啟用健康檢查 HTTP server。 |
| `INTEGRATION_HEALTH_SERVER_HOST` | `0.0.0.0` | 健康檢查服務 host。 |
| `INTEGRATION_HEALTH_SERVER_PORT` | `8081` | 健康檢查服務 port。 |
| `INTEGRATION_HEALTH_LIVENESS_TIMEOUT_SECONDS` | `30` | `/healthz` loop 心跳逾時秒數。 |
| `INTEGRATION_HEALTH_READINESS_TIMEOUT_SECONDS` | `30` | `/readyz` 最近進度逾時秒數。 |
| `INTEGRATION_HEALTH_STARTUP_GRACE_SECONDS` | `10` | startup 完成後首次 loop / progress 寬限秒數。 |

啟用後提供：

| Endpoint |
| --- |
| `GET /startupz` |
| `GET /healthz` |
| `GET /readyz` |

## Iron Gate Scheduler（專案插件）

僅在使用 `IronGateSchedulerEngine` 時需要。

| 變數 | 預設 | 說明 |
| --- | --- | --- |
| `EDGE_GATE_CLASS` | `iron_gate` | 鐵門類別名稱。 |
| `EDGE_WORKING_PHASE` | `working` | 對應開門時的 phase 名稱。 |
| `EDGE_NON_WORKING_PHASE` | `non_working` | 對應關門時的 phase 名稱。 |
