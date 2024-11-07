
we_mart.wv_live_play
====================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_mart**|
|**Table Type**|MART PRIMARY|
|**Partitioned by**|`part_date`|
|**Created/ Last Updated At**|2023-03-29 / 2024-06-04|
|**Created By**|이현지|
|**Last Updated By**|이현지|
|**Collaborators**|이현지[9], 이현지[1]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2023-03-29|이현지|[PR](https://github.com/benxcorp/databricks/commit/ee263cd44d13e7281e9d79ca2c86e6381c6e028a)|
|2023-05-30|이현지|[PR](https://github.com/benxcorp/databricks/commit/728cd018c72dca77458e34a4804a561fb97c7e37)|
|2023-06-08|이현지|[PR](https://github.com/benxcorp/databricks/commit/70701d6f29eb658e91533ce05ec11b57ae7fd218)|
|2023-07-07|이현지|[PR](https://github.com/benxcorp/databricks/commit/8e0566510727b967e9d0b4ded0be96d4d8ca1dd8)|
|2024-03-05|이현지|[PR](https://github.com/benxcorp/databricks/commit/fd60cbccf2c1bf37c78b8913a93f18ffb251a6cc)|
|2024-03-05|이현지|[PR](https://github.com/benxcorp/databricks/commit/62077c40ec537fdce04ae6af1298a9553d9622ec)|
|2024-03-19|이현지|[PR](https://github.com/benxcorp/databricks/commit/2bf4c67f4cbb6f64c7f841908e60914cc3b9271a)|
|2024-03-19|이현지|[PR](https://github.com/benxcorp/databricks/commit/fab382bc7472d0b8786c85dd479305ae1ca0d030)|
|2024-04-24|이현지|[PR](https://github.com/benxcorp/databricks/commit/b63e92c266264619eddfd2e04a11f1b0a058f90b)|
|2024-06-04|이현지|[PR](https://github.com/benxcorp/databricks/commit/1271453a2867e797d99df43cc3c77d9f53236687)|
  
  
# TABLE NOTICE
  
### 테이블 개요

* **테이블 목적**: 위버스 라이브 방송 시청 데이터를 담은 테이블
* **데이터 레벨**: AGGREGATED DATA(STATISTICS)
* **파티션 키**: `part_date`
* **주요 키**: `post_id`, `wv_user_id`, `device_id`, `start_dt`

### 테이블 특징

* 라이브 방송 시작 시간 기준으로 1시간 단위로 집계된 데이터
* `is_join`, `is_leave`, `join_cnt`, `leave_cnt` 컬럼을 통해 시청 시작 및 종료 여부, 총 시청 시작 및 종료 횟수 파악 가능
* `u_sum_play_time`, `d_sum_play_time` 컬럼을 통해 유저 및 기기별 시청 시간 합계 파악 가능
* `is_first_wv_join`, `is_first_comm_join`, `is_comm_join`, `is_comm_user`, `is_fc` 컬럼을 통해 라이브 시청 중 위버스, 커뮤니티 가입 여부, 팬클럽 가입 여부 확인 가능

### 데이터 추출 및 생성 과정

1. **주요 데이터 소스**:
    * `we_mart.wv_server_log_base`: 위버스 서버 로그 데이터
    * `we_mart.wv_live`: 위버스 라이브 목록 데이터
    * `we_mart.ws_fc_user_history`: 위버스샵 FC 유저 히스토리 데이터
    * `weverse2.user_user`: 위버스 유저 정보 데이터
    * `wev_prod.we_mart.wv_live_joint_history`: 위버스 라이브 합동 방송 히스토리 데이터
    * `we_mart.wv_comm_user_update`: 위버스 커뮤니티 유저 업데이트 데이터
2. **데이터 전처리**:
    * `wv_server_log_base` 테이블에서 `url` 컬럼이 `/video/v1.0/join` 또는 `/video/v1.0/leave`인 데이터만 추출
    * `wv_live` 테이블에서 `is_from_vlive`가 0이고 `status`가 `CANCELED`가 아닌 데이터만 추출
    * `ws_fc_user_history` 테이블에서 `ord_status`가 `PAYMENT_FAILED`가 아니고 `is_cx_by_restore`가 0인 데이터만 추출
    * `wv_server_log_base` 테이블과 `wv_live` 테이블을 `post_id` 컬럼으로 조인
    * `wv_server_log_base` 테이블과 `user_user` 테이블을 `user_id_fill` 컬럼으로 조인
    * `wv_server_log_base` 테이블과 `wv_live_joint_history` 테이블을 `post_id` 컬럼으로 조인
    * `wv_server_log_base` 테이블과 `wv_comm_user_update` 테이블을 `wv_user_id`, `we_art_id` 컬럼으로 조인
    * `wv_server_log_base` 테이블과 `ws_fc_user_history` 테이블을 `we_member_id`, `we_art_id`, `start_dt`, `end_dt` 컬럼으로 조인
3. **데이터 통합**:
    * 유저별, 기기별 라이브 시청 시작 및 종료 시간, 횟수, 시청 시간 합계 등을 집계
    * 라이브 시청 중 위버스, 커뮤니티 가입 여부, 팬클럽 가입 여부 등을 확인
4. **최종 테이블 생성**:
    * 위에서 집계 및 확인된 데이터를 `we_mart.wv_live_play` 테이블에 저장
    * `part_date` 컬럼을 파티션 키로 활용

### 테이블 활용 가이드

* **주요 활용**:
    * 위버스 라이브 시청 데이터 분석
    * 라이브 방송 시청 시간, 횟수, 유저 특징(가입 여부, 팬클럽 여부 등) 분석
    * 특정 라이브 방송 시청 데이터 분석
* **조인 시 유의사항**:
    * `we_mart.wv_live` 테이블과 `post_id` 컬럼으로 조인하여 라이브 정보 확인 가능
    * `weverse2.user_user` 테이블과 `wv_user_id` 컬럼으로 조인하여 유저 정보 확인 가능
    * `we_mart.ws_fc_user_history` 테이블과 `we_member_id`, `we_art_id` 컬럼으로 조인하여 FC 유저 정보 확인 가능

### 추가 정보

* `tmp_end_dt` 컬럼은 라이브 종료 시간을 나타내는 `end_dt` 컬럼의 임시 값
* `is_late` 컬럼은 라이브 종료 후 시청 시작 여부를 나타내는 값
* `guest_joint_seq` 컬럼은 합동 라이브 방송 시 게스트 참여 순서를 나타내는 값
* `u_join_first`, `u_leave_last`, `d_join_first`, `d_leave_last` 컬럼은 유저, 기기별 라이브 시청 시작 및 종료 시간을 나타내는 값  
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|run_timestamp|timestamp| |
|1|part_date|string|LIVE시작일|
|2|date_id|string|s_log.date_id(KST)|
|3|hour|string|hour(KST)|
|4|minute|int|minute(KST)|
|5|datetime|string| |
|6|we_member_id|bigint| |
|7|wv_user_id|string| |
|8|device_id|string| |
|9|platform|string|접속 플랫폼|
|10|os|string|접속 OS|
|11|ctry_code|string|s_log.gcc|
|12|post_id|string| |
|13|video_id|string| |
|14|start_dt|timestamp|LIVE시작일시|
|15|end_dt|timestamp|LIVE종료일시|
|16|tmp_end_dt|timestamp|LIVE종료일시(임시)|
|17|we_art_id|int|아티스트id|
|18|comm_id|int|커뮤니티id|
|19|is_fc_only|int|커뮤니티only LIVE여부|
|20|is_joint_live|int|합동라이브 여부|
|21|is_join|int|재생 여부|
|22|is_leave|int|이탈 여부|
|23|u_join_first|int|유저,LIVE별 첫 재생|
|24|u_leave_last|int|유저,LIVE별 마지막 이탈|
|25|d_join_first|int|기기,LIVE별 첫 재생|
|26|d_leave_last|int|기기,LIVE별 마지막 이탈|
|27|is_late|int|LIVE종료 후 여부|
|28|join_cnt|bigint|재생수|
|29|leave_cnt|bigint|이탈수|
|30|u_sum_play_time|bigint|유저별 LIVE 시청시간 합(join/leave별)|
|31|d_sum_play_time|bigint|기기별 LIVE 시청시간 합(join/leave별)|
|32|is_first_wv_join|int|LIVE진행 중 위버스 최초가입 여부|
|33|is_first_comm_join|int|LIVE진행 중 커뮤니티 최초가입 여부|
|34|is_comm_join|int|LIVE진행 중 커뮤니티 가입 여부|
|35|is_comm_user|int|커뮤니티 가입자 여부|
|36|is_fc|int|FC유저 여부|
|37|guest_joint_seq|int|합동라이브 게스트 조인 순서|
  
    
---
# HOW TO USE
  
No content.  
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_wv_mart_daily`

### Update Interval: DAILY

### Update Type: OVERWRITE

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/mart/we_mart/wv_live_play.py)

### Airflow: [DAG](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_wv_mart_daily.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|we_mart.ws_fc_user_history|we_mart.stats_we_t_kpi|
|we_mart.wv_comm_user_update|we_mart.stats_wv_d_live_play|
|we_mart.wv_live|we_mart.stats_wv_d_live_play_min|
|we_mart.wv_live_joint_history|we_mart.stats_wv_m_video_copyright|
|we_mart.wv_server_log_base| |
|weverse2.user_user| |

## 🐤 Downstream Tables Info
  
No content.  
---