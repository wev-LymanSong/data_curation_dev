
we_mart.wv_dm_subscr_daily
==========================

# BASIC INFO

|**About**| |
| :--- | :--- |
|**Database**|**we_mart**|
|**Table Type**|MART PRIMARY|
|**Partitioned by**|`part_date`|
|**Created/ Last Updated At**|2024-06-24 / 2024-08-08|
|**Created By**|박상민|
|**Last Updated By**|박상민|
|**Collaborators**|박상민[7]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2024-06-24|박상민|[PR](https://github.com/benxcorp/databricks/commit/8e7af3fc1aa0dea15fe963098f5c4f90343b031a)|
|2024-06-25|박상민|[PR](https://github.com/benxcorp/databricks/commit/ee5e192c8efb409f2d9b018209c5bae047f04a5f)|
|2024-07-01|박상민|[PR](https://github.com/benxcorp/databricks/commit/c9618af3d45d7985d6bb9e228340748052804809)|
|2024-07-10|박상민|[PR](https://github.com/benxcorp/databricks/commit/e7cf07873cb44d01fa2e1e9fde3d80d158128899)|
|2024-07-11|박상민|[PR](https://github.com/benxcorp/databricks/commit/71e50494003345a047f78196589061de974a393b)|
|2024-07-25|박상민|[PR](https://github.com/benxcorp/databricks/commit/3a2337fb52f3c241cd3d3c789fd996c68c027f0a)|
|2024-08-08|박상민|[PR](https://github.com/benxcorp/databricks/commit/9406d1a5eda3f73616d2ed55eb0190ff21689c7d)|
  
  
# TABLE NOTICE
  
   
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|key_date|date|기준일자|
|1|subscribe_id|bigint|subscribe 쪽 구독 id|
|2|we_art_id|int|아티스트id|
|3|we_art_name|string|아티스트명|
|4|dm_id|bigint|dm_id|
|5|dm_name|string|DM명(EN)|
|6|account_id|string|account_id|
|7|subscr_type_dtl|string|구독 상세 유형|
|8|subscr_term_key|string|구독 주기 key |
|9|is_last_term|int|마지막 주기 여부|
|10|start_type|string|시작 유형|
|11|end_type|string|종료 유형|
|12|reserve_type|string|예약 유형|
|13|subscr_days|int|구독 일수|
|14|new_dt|timestamp|구독 최초 시작 일시(KST)|
|15|start_dt|timestamp|구독 주기 시작 일시(KST)|
|16|original_start_dt|timestamp|subscribe 상 발생한 시작일시(KST)|
|17|original_exp_dt|timestamp|구독 주기 최초 만료일시(KST)|
|18|original_end_dt|timestamp|구독 주기 최초 종료일시(KST)|
|19|real_end_dt|timestamp|실제 종료일시(KST)|
|20|final_renew_target_dt|timestamp|갱신 대상 일시(KST)|
|21|last_reserve_status_dt|timestamp|예약 상태 변경일(KST)|
|22|change_from|string|변경 시작 이전 구독권(from)|
|23|change_to|string|변경 종료 이후 구독권(to)|
|24|subscribe_type|string|구독권 유형|
|25|market_type|string|마켓 유형|
|26|subscription_id|bigint|wdm 쪽 구독id|
|27|ctry_code|string|국가 코드|
|28|is_comm_user|int|커뮤니티 가입여부|
|29|is_fc|int|멤버십 가입여부|
|30|product_id|string|상품id(concat'd')|
|31|store_item_id|string|store_item_id|
|32|part_date|string|파티션일자(로그발생일 기준)|
|33|run_timestamp|timestamp|배치일시(UTC)|
  
    
---
# HOW TO USE
  
   
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_we_mart_priority_daily`

### Update Interval: DAILY

### Update Type: APPEND

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/mart/we_mart/wv_dm_subscr_daily.py)

### Airflow: [DAG](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_we_mart_priority_daily.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|subscribe.reservation_history|we_mart.stats_wv_d_dm_subscr|
|subscribe.subscribe_history| |
|we_mart.we_artist| |
|we_mart.we_user| |
|we_mart.ws_fc_user_history| |
|we_mart.wv_comm_user| |
|we_mart.wv_user_ctry_history| |
|we_meta.wv_dm_subscr_exc| |
|weverse2.wdm_common_dm| |
|weverse2.wdm_subscription| |

## 🐤 Downstream Tables Info
  
   
---  
---