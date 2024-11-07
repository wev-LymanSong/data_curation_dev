
we_mart.wv_vod_play
===================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_mart**|
|**Table Type**|MART PRIMARY|
|**Partitioned by**|`part_date`|
|**Created/ Last Updated At**|2023-03-29 / 2024-07-02|
|**Created By**|이현지|
|**Last Updated By**|송재영|
|**Collaborators**|송재영[5], 이현지[5], 이현지[1]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2023-03-29|이현지|[PR](https://github.com/benxcorp/databricks/commit/e6bab64f9282850b32445eafa2457809bda61490)|
|2023-03-29|이현지|[PR](https://github.com/benxcorp/databricks/commit/db9c55857e369cc5642df86826bf9f0674ceb939)|
|2023-03-29|이현지|[PR](https://github.com/benxcorp/databricks/commit/ee263cd44d13e7281e9d79ca2c86e6381c6e028a)|
|2023-03-30|이현지|[PR](https://github.com/benxcorp/databricks/commit/f5f16e496e9a02ba5431ee6bfe78ae31514134d4)|
|2023-03-30|이현지|[PR](https://github.com/benxcorp/databricks/commit/3888991847c2d4c1d0445b4a5b8f7a7ee13d1518)|
|2023-05-16|송재영|[PR](https://github.com/benxcorp/databricks/commit/63927e5e6305b263a9c26ce8aca2ec364f35b171)|
|2023-05-16|송재영|[PR](https://github.com/benxcorp/databricks/commit/0d348cbe45b3e7fb1d356fe2c05ca18e5be5bd1a)|
|2023-05-17|송재영|[PR](https://github.com/benxcorp/databricks/commit/3c825e6a563d7972ff9b6f07c79ef5b8d4ffc53f)|
|2023-06-12|송재영|[PR](https://github.com/benxcorp/databricks/commit/92d52fded71d7111678382b27e880f317933ae55)|
|2023-10-18|이현지|[PR](https://github.com/benxcorp/databricks/commit/fa67ca0c38f7859c126cefc301ece06e14679133)|
|2024-07-02|송재영|[PR](https://github.com/benxcorp/databricks/commit/21da1795061451c1397d4b388a87e68712a3483e)|
  
  
# TABLE NOTICE
  
### 테이블 개요

* **테이블 목적**: Weverse 플랫폼 VOD 재생 로그 데이터를 분석하기 위한 통합 테이블. VOD 시청 시간, 재생 횟수, 조회 횟수 등을 포함하여 사용자 행동 분석, 콘텐츠 성과 측정 등에 활용
* **데이터 레벨**: AGGREGATED DATA(STATISTICS)
* **파티션 키**: `part_date`
* **주요 키**: `wv_user_id`, `post_id`, `video_id`, `datetime`

### 테이블 특징

* `wv_user_id`, `post_id`, `video_id`, `datetime` 컬럼을 사용하여 VOD 재생 로그를 상세하게 추적
* 사용자, 콘텐츠, 시간별 VOD 시청 데이터를 집계하여 분석 가능
* `is_join`, `is_post_read`, `join_cnt`, `post_read_cnt` 등의 컬럼을 통해 VOD 재생 및 콘텐츠 조회 정보 제공
* `u_sum_play_time`, `d_sum_play_time` 컬럼은 각각 사용자별, 기기별 VOD 시청 시간을 나타내 사용자 행동 분석에 활용 가능

### 데이터 추출 및 생성 과정

1. **주요 데이터 소스**:
    * `we_mart.wv_server_log_base`: Weverse 플랫폼 서버 로그 데이터
    * `we_mart.wv_media`: Weverse 콘텐츠 정보
    * `we_mart.wv_live`: Weverse LIVE 정보
    * `weverse2.user_user`: Weverse 사용자 정보
    * `weverse2.community_content_common_product_media_relation`: Weverse 커뮤니티 콘텐츠 상품 정보
    * `auth`: VOD 구매 내역 정보 (쿼리에서 임시 뷰로 생성)
2. **데이터 전처리**:
    * `wv_server_log_base`에서 VOD 재생 및 콘텐츠 조회 관련 로그만 필터링
    * `wv_media` 테이블과 조인하여 콘텐츠 정보 추가
    * `wv_live` 테이블과 조인하여 LIVE 정보 추가
    * `user_user` 테이블과 조인하여 사용자 정보 추가
    * `community_content_common_product_media_relation` 테이블과 조인하여 콘텐츠 상품 정보 추가
    * `auth` 테이블과 조인하여 VOD 구매 여부 정보 추가
    * `lead` 함수를 사용하여 VOD 재생 시작 및 종료 시간 정보 계산
3. **데이터 통합**:
    * `part_date`, `wv_user_id`, `post_id`, `video_id`, `datetime` 등의 컬럼을 기준으로 데이터 집계
    * `is_join`, `is_post_read`, `join_cnt`, `post_read_cnt` 등의 컬럼을 계산하여 VOD 재생 및 콘텐츠 조회 정보 집계
    * `u_sum_play_time`, `d_sum_play_time` 컬럼을 계산하여 사용자별, 기기별 VOD 시청 시간 집계
4. **최종 테이블 생성**:
    * 집계된 데이터를 `we_mart.wv_vod_play` 테이블에 저장
    * `part_date` 컬럼을 파티션 키로 사용하여 데이터 분할 저장

### 테이블 활용 가이드

* **주요 활용**:
    * VOD 시청 시간, 재생 횟수, 조회 횟수 등을 분석하여 콘텐츠 성과 측정
    * 사용자별, 콘텐츠별 VOD 시청 패턴 분석
    * 특정 기간, 국가, 기기별 VOD 시청 트렌드 분석
* **조인 시 유의사항**:
    * `wv_user_id`, `post_id`, `video_id`, `datetime` 컬럼을 사용하여 다른 테이블과 조인 가능
    * `we_mart.we_user` 테이블과 조인하여 사용자 정보 추가 가능
    * `we_mart.wv_media` 테이블과 조인하여 콘텐츠 정보 추가 가능
    * `part_date` 컬럼을 사용하여 다른 테이블과 조인 시 동일한 기간의 데이터만 조인되도록 주의

### 추가 정보

* `auth` 테이블은 VOD 구매 내역을 나타내는 임시 뷰로, `we_mart.wv_vod_play` 테이블 생성 시 사용됨
* `u_sum_play_time`과 `d_sum_play_time` 컬럼은 각각 사용자별, 기기별 VOD 시청 시간을 나타내며, 사용자 행동 분석에 유용하게 활용 가능
* `is_join`과 `is_post_read` 컬럼은 각각 VOD 재생 여부와 콘텐츠 조회 여부를 나타내며, VOD 재생 및 콘텐츠 조회 정보 분석에 활용 가능  
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|run_timestamp|timestamp| |
|1|part_date|string| |
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
|14|we_art_id|int|아티스트id|
|15|comm_id|int|커뮤니티id|
|16|is_fc_only|int|커뮤니티only LIVE여부|
|17|is_pitem|int|유무료 여부|
|18|product_id|string| |
|19|section_type|string| |
|20|media_type|string| |
|21|is_pay|int|VOD 구매여부|
|22|is_join|int|재생 여부|
|23|is_post_read|int|조회 여부|
|24|join_cnt|bigint|재생수|
|25|post_read_cnt|bigint|조회수|
|26|u_sum_play_time|bigint|유저별 VOD시청 시간|
|27|d_sum_play_time|bigint|유저,기기별 VOD시청 시간|
  
    
---
# HOW TO USE
  
No content.  
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_we_mart_daily`

### Update Interval: DAILY

### Update Type: OVERWRITE

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/mart/we_mart/wv_vod_play.py)

### Airflow: [DAG](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_we_mart_daily.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|coupon.tb_cp_used|we_mart.stats_wv_d_media_play_bypost|
|we_mart.we_user|we_mart.stats_wv_m_video_copyright|
|we_mart.wv_live| |
|we_mart.wv_media| |
|we_mart.wv_order| |
|we_mart.wv_server_log_base| |
|we_meta.we_media_product| |
|weverse2.community_content_common_product_media_relation| |
|weverse2.user_user| |

## 🐤 Downstream Tables Info
  
No content.  
---