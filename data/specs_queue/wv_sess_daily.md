
we_mart.wv_sess_daily
=====================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_mart**|
|**Table Type**|MART PRIMARY|
|**Partitioned by**|`part_date`|
|**Created/ Last Updated At**|2022-09-16 / 2024-07-02|
|**Created By**|박상민|
|**Last Updated By**|송재영|
|**Collaborators**|박상민[5], 송재영[1], 구민서[1]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2022-09-16|박상민|[PR](https://github.com/benxcorp/databricks/commit/2d77f9ef72e5905a7ffcf8436869fa3c9f9d5a5d)|
|2022-09-27|박상민|[PR](https://github.com/benxcorp/databricks/commit/23429097b3cd0daa4d63abd0b51b74aff146207e)|
|2022-11-01|박상민|[PR](https://github.com/benxcorp/databricks/commit/4d1557f46e47fb858f7e4ee2e9288c42d9ad0ae7)|
|2022-11-02|박상민|[PR](https://github.com/benxcorp/databricks/commit/ae256d00ec1e7ebc842f802a6a88d66b2c56a617)|
|2022-12-09|구민서|[PR](https://github.com/benxcorp/databricks/commit/bd183779c84e616084e05627330a1c8b98ca44bd)|
|2023-10-18|박상민|[PR](https://github.com/benxcorp/databricks/commit/a3bf0465549bad83f8f434124826f40f0ff42b49)|
|2024-07-02|송재영|[PR](https://github.com/benxcorp/databricks/commit/21da1795061451c1397d4b388a87e68712a3483e)|
  
  
# TABLE NOTICE
  
### 테이블 개요

* **테이블 목적**: 위버스 플랫폼의 일별 세션 정보를 담고 있다.
* **데이터 레벨**: TRANSACTIONAL DATA
* **파티션 키**: `part_date`
* **주요 키**: `sess_id`, `key_date`, `we_member_id`, `wv_user_id`

### 테이블 특징

* `key_date` 컬럼을 통해 일별 세션 정보를 확인할 수 있다.
* `we_member_id`, `wv_user_id`, `device_id` 컬럼을 통해 사용자 정보를 식별할 수 있다.
* `sess_id` 컬럼은 세션 시작일시(yyyyMMddHHmmssSSS)와 `user_info_device_id`를 결합하여 생성된 고유한 세션 식별자이다.
* `sess_start_dt`, `sess_end_dt`, `sess_dur` 컬럼은 각 세션의 시작, 종료 시간과 체류 시간을 나타낸다.
* `user_sess_id` 컬럼은 `sess_id`와 `user_id`를 결합하여 사용자별 세션 정보를 구분한다.
* `user_sess_start_dt`, `user_sess_end_dt`, `user_sess_dur` 컬럼은 사용자별 세션의 시작, 종료 시간과 체류 시간을 나타낸다.
* `platform`, `os`, `app_ver` 컬럼은 사용자의 플랫폼, 운영 체제, 앱 버전 정보를 제공한다.
* `is_comm_user`, `is_fc` 컬럼은 사용자가 커뮤니티 가입자 또는 멤버십 가입자인지 여부를 나타낸다.
* `comm_id`, `we_art_id`, `we_art_name` 컬럼은 사용자가 방문한 커뮤니티, 해당 커뮤니티의 아티스트 정보를 제공한다.
* `art_indi_id`, `art_indi_type` 컬럼은 아티스트 멤버 정보를 제공한다.

### 데이터 추출 및 생성 과정

1. **주요 데이터 소스**:
    * `we_mart.wv_server_log_base` : 위버스 서버 로그 정보
    * `weverse2.community_content_post` : 커뮤니티 게시글 정보
    * `weverse2.community_content_common_notice` : 커뮤니티 공지 정보
    * `we_mart.we_user` : 위버스 회원 정보
    * `we_mart.wv_comm_user` : 커뮤니티 가입 정보
    * `we_mart.we_artist` : 아티스트 정보
    * `we_mart.ws_fc_user_history` : 멤버십 가입 정보
2. **데이터 전처리**:
    * `we_mart.wv_server_log_base` 테이블에서 `date_id` 컬럼 값을 기준으로 데이터를 추출한다.
    * `weverse2.community_content_post` 테이블에서 `post_id`와 `comm_id`를 추출한다.
    * `weverse2.community_content_common_notice` 테이블에서 `notice_id`와 `comm_id`를 추출한다.
    * `we_mart.we_user` 테이블에서 `wv_user_id`와 `account_id`를 추출한다.
    * `we_mart.wv_comm_user` 테이블에서 `comm_id`, `wv_user_id`, `art_indi_id`, `art_indi_type`을 추출한다.
    * `we_mart.we_artist` 테이블에서 `we_art_id`, `we_art_name`, `comm_id`를 추출한다.
    * `we_mart.ws_fc_user_history` 테이블에서 `we_art_id`, `we_member_id`를 추출한다.
3. **데이터 통합**:
    * `df_log` 테이블에 각 데이터 소스 테이블을 조인하여 `comm_id` 컬럼을 복구한다.
    * `sess_dur`, `user_sess_dur` 컬럼을 생성하여 세션 체류 시간을 계산한다.
    * `df_user`, `df_comm`, `df_art`, `df_fc` 테이블을 조인하여 사용자 정보, 커뮤니티 가입 정보, 아티스트 정보, 멤버십 가입 정보를 추가한다.
4. **최종 테이블 생성**:
    * 필요한 컬럼을 선택하고 `part_date` 컬럼을 파티션 키로 사용하여 `we_mart.wv_sess_daily` 테이블을 생성한다.

### 테이블 활용 가이드

* **주요 활용**:
    * 위버스 플랫폼의 일별 세션 정보 분석
    * 사용자 행동 패턴 분석
    * 커뮤니티 활동 분석
    * 멤버십 가입 정보 분석
* **조인 시 유의사항**:
    * `we_mart.wv_sess_daily` 테이블을 다른 테이블과 조인할 때는 `key_date`, `we_member_id`, `wv_user_id`, `device_id` 컬럼을 사용하여 조인하는 것이 효율적이다.
    * `sess_id` 컬럼은 세션 시작일시와 `user_info_device_id`를 결합하여 생성된 고유한 식별자이므로, 다른 테이블과 조인할 때는 세션 시작일시와 `user_info_device_id`를 함께 사용하여 조인해야 한다.

### 추가 정보

* `we_mart.wv_sess_daily` 테이블은 위버스 플랫폼의 세션 정보를 일별로 집계한 데이터이다.
* `run_timestamp` 컬럼은 데이터 처리 시간을 나타낸다.
* `part_date` 컬럼은 파티션 키로 사용되며, 데이터를 일별로 분할하여 관리한다.
* `we_mart.wv_sess_daily` 테이블은 위버스 플랫폼의 사용자 행동 패턴을 분석하는 데 유용하게 활용될 수 있다.  
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|key_date|date|방문일자|
|1|we_member_id|int|account_id|
|2|wv_user_id|int|위버스 회원 id|
|3|user_ctry|string|접속국가|
|4|device_id|string|기기id(브라우저id)|
|5|is_device_login|int|device 로그인여부|
|6|we_art_id|int|아티스트id|
|7|we_art_name|string|아티스트명|
|8|comm_id|string|커뮤니티id|
|9|is_comm_user|int|커뮤니티 가입자 여부|
|10|is_fc|int|멤버십 여부|
|11|art_indi_id|string|아티스트 멤버 id|
|12|art_indi_type|string|아티스트 타입|
|13|sess_id|string|로그시작일시(yyyyMMddHHmmssSSS)\|user_info_device_id|
|14|sess_ctry|string|session 접속국가(최종)|
|15|sess_start_dt|timestamp|세션id 최초일시(UTC)|
|16|sess_end_dt|timestamp|세션id 최종일시(UTC)|
|17|sess_dur|bigint|세션 체류 시간|
|18|user_sess_id|string|session_id + user_id|
|19|is_sess_login|int|sess 로그인 여부|
|20|user_sess_start_dt|timestamp|user_sess_id 시작일시(UTC)|
|21|user_sess_end_dt|timestamp|user_sess_id 종료일시(UTC)|
|22|user_sess_dur|bigint|user_session 체류시간|
|23|platform|string|플랫폼|
|24|os|string|OS|
|25|app_ver|string|app_ver|
|26|part_date|string|part_date|
|27|run_timestamp|timestamp|배치일시(UTC)|
  
    
---
# HOW TO USE
  
### Downstream Table/View
- `wv_sess_daily` 테이블을 사용하여 `we_mart.wv_user_daily` 테이블을 생성
    - ```sql
      create or replace table we_mart.wv_user_daily
      (
      key_date	date	comment "방문일자"
      , we_member_id	int	comment "account_id"
      , wv_user_id	int	comment "위버스 회원 id"
      , user_ctry	string	comment "접속국가"
      , device_id	string	comment "기기id(브라우저id)"
      , we_art_id	int	comment "아티스트id"
      , we_art_name	string	comment "아티스트명"
      , comm_id	string	comment "커뮤니티id"
      , is_comm_user	int	comment "커뮤니티 가입자 여부"
      , is_fc	int	comment "멤버십 여부"
      , art_indi_id	string	comment "아티스트 멤버 id"
      , art_indi_type	string	comment "아티스트 타입"
      , sess_id	string	comment "로그시작일시(yyyyMMddHHmmssSSS)|user_info_device_id"
      , sess_start_dt	timestamp	comment "세션id 최초일시(UTC)"
      , sess_end_dt	timestamp	comment "세션id 최종일시(UTC)"
      , sess_dur	bigint	comment "세션 체류 시간"
      , user_sess_id	string	comment "session_id + user_id"
      , user_sess_start_dt	timestamp	comment "user_sess_id 시작일시(UTC)"
      , user_sess_end_dt	timestamp	comment "user_sess_id 종료일시(UTC)"
      , user_sess_dur	bigint	comment "user_session 체류시간"
      , platform	string	comment "플랫폼"
      , os	string	comment "OS"
      , app_ver	string	comment "app_ver"
      , part_date	string	comment "part_date"
      , run_timestamp	timestamp	comment "배치일시(UTC)"
      ) 
      partitioned by (part_date)
      comment "WV 일간 세션 정보"
      ```
- `wv_sess_daily` 테이블을 사용하여 `we_mart.stats_wv_d_session` 테이블을 생성 - 일별 세션 수 집계
    - ```sql
      create or replace table we_mart.stats_wv_d_session
      as
      select
      date(key_date) as key_date
      , we_member_id
      , count(distinct sess_id) as sess_cnt
      , sum(sess_dur) as total_sess_dur
      from we_mart.wv_sess_daily
      where part_date = '2024-01-01'
      group by 1, 2
      ```
- `wv_sess_daily` 테이블을 사용하여 `we_mart.stats_wv_d_user_sess` 테이블을 생성 - 일별 유저 세션 수 집계
    - ```sql
      create or replace table we_mart.stats_wv_d_user_sess
      as
      select
      date(key_date) as key_date
      , we_member_id
      , count(distinct user_sess_id) as user_sess_cnt
      , sum(user_sess_dur) as total_user_sess_dur
      from we_mart.wv_sess_daily
      where part_date = '2024-01-01'
      group by 1, 2
      ```
- `wv_sess_daily` 테이블을 사용하여 `we_mart.stats_wv_d_platform` 테이블을 생성 - 일별 플랫폼별 세션 수 집계
    - ```sql
      create or replace table we_mart.stats_wv_d_platform
      as
      select
      date(key_date) as key_date
      , platform
      , count(distinct sess_id) as sess_cnt
      from we_mart.wv_sess_daily
      where part_date = '2024-01-01'
      group by 1, 2
      ```

### Data Extraction
- 일별, 아티스트별, 플랫폼별 세션 수 추출
    - ```sql
      select
      date(key_date) as key_date,
      we_art_id,
      we_art_name,
      platform,
      count(distinct sess_id) as sess_cnt
      from we_mart.wv_sess_daily
      where part_date = '2024-01-01'
      group by 1, 2, 3, 4
      ```
- 특정 아티스트(ARTIST)의 커뮤니티에 가입한 유저들의 일별 세션 수 추출
    - ```sql
      select
      date(key_date) as key_date,
      we_member_id,
      count(distinct sess_id) as sess_cnt
      from we_mart.wv_sess_daily
      where part_date = '2024-01-01'
      and we_art_id = (select we_art_id from we_mart.we_artist where we_art_name = 'ARTIST')
      group by 1, 2
      ```
- 특정 기간(2024-01-01 ~ 2024-01-07) 동안 특정 국가(KR)에서 위버스에 접속한 유저들의 세션 수 추출
    - ```sql
      select
      date(key_date) as key_date,
      we_member_id,
      count(distinct sess_id) as sess_cnt
      from we_mart.wv_sess_daily
      where part_date between '2024-01-01' and '2024-01-07'
      and user_ctry = 'KR'
      group by 1, 2
      ```
- 특정 아티스트(ARTIST)의 팬클럽에 가입한 유저들의 일별 세션 시간 추출
    - ```sql
      select
      date(key_date) as key_date,
      we_member_id,
      sum(sess_dur) as total_sess_dur
      from we_mart.wv_sess_daily
      where part_date = '2024-01-01'
      and we_art_id = (select we_art_id from we_mart.we_artist where we_art_name = 'ARTIST')
      and is_fc = 1
      group by 1, 2
      ```
- 특정 기간(2024-01-01 ~ 2024-01-07) 동안 특정 아티스트(ARTIST)의 커뮤니티에 가입한 유저들의 일별 세션 수 추출 ( `we_art.we_artist` 테이블과 조인)
    - ```sql
      select
      date(ws.key_date) as key_date,
      ws.we_member_id,
      count(distinct ws.sess_id) as sess_cnt
      from we_mart.wv_sess_daily ws
      inner join we_mart.we_artist art on ws.we_art_id = art.we_art_id
      where art.we_art_name = 'ARTIST'
      and ws.part_date between '2024-01-01' and '2024-01-07'
      group by 1, 2
      ```
- 특정 기간(2024-01-01 ~ 2024-01-07) 동안 특정 아티스트(ARTIST)의 커뮤니티에 가입한 유저들의 일별 세션 시간 추출 ( `we_art.we_artist` 테이블과 조인)
    - ```sql
      select
      date(ws.key_date) as key_date,
      ws.we_member_id,
      sum(ws.sess_dur) as total_sess_dur
      from we_mart.wv_sess_daily ws
      inner join we_mart.we_artist art on ws.we_art_id = art.we_art_id
      where art.we_art_name = 'ARTIST'
      and ws.part_date between '2024-01-01' and '2024-01-07'
      group by 1, 2
      ```
- 특정 기간(2024-01-01 ~ 2024-01-07) 동안 특정 아티스트(ARTIST)의 커뮤니티에 가입한 유저들의 일별 세션 수 및 세션 시간 추출 ( `we_art.we_artist` 테이블과 조인)
    - ```sql
      select
      date(ws.key_date) as key_date,
      ws.we_member_id,
      count(distinct ws.sess_id) as sess_cnt,
      sum(ws.sess_dur) as total_sess_dur
      from we_mart.wv_sess_daily ws
      inner join we_mart.we_artist art on ws.we_art_id = art.we_art_id
      where art.we_art_name = 'ARTIST'
      and ws.part_date between '2024-01-01' and '2024-01-07'
      group by 1, 2
      ```
- 특정 기간(2024-01-01 ~ 2024-01-07) 동안 특정 아티스트(ARTIST)의 커뮤니티에 가입한 유저들의 일별 세션 수 및 세션 시간 추출 ( `we_art.we_artist` 테이블과 조인)
    - ```python
      from pyspark.sql import SparkSession
      from pyspark.sql.functions import col, date, countDistinct, sum
      
      spark = SparkSession.builder.appName("WeverseSessionAnalysis").getOrCreate()
      
      # Target table: wv_sess_daily
      wv_sess_daily_df = spark.read.format("delta").load("we_mart.wv_sess_daily")
      
      # Join with we_artist table
      we_artist_df = spark.read.format("delta").load("we_mart.we_artist")
      joined_df = wv_sess_daily_df.join(we_artist_df, wv_sess_daily_df.we_art_id == we_artist_df.we_art_id, "inner")
      
      # Filter for specific artist and date range
      filtered_df = joined_df.filter(
          (col("we_art_name") == "ARTIST") & 
          (date(col("part_date")) >= "2024-01-01") &
          (date(col("part_date")) <= "2024-01-07")
      )
      
      # Aggregate by date and member ID
      result_df = filtered_df.groupBy(date(col("key_date")), col("we_member_id")).agg(
          countDistinct("sess_id").alias("sess_cnt"),
          sum("sess_dur").alias("total_sess_dur")
      )
      
      result_df.show()
      
      spark.stop()
      ```  
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_we_mart_priority_daily`, `analytics_we_mart_wv1_daily`

### Update Interval: DAILY

### Update Type: OVERWRITE

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/mart/we_mart/wv_sess_daily.py)

### Airflow DAGs

- [analytics_we_mart_priority_daily](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_we_mart_priority_daily.py)
- [analytics_we_mart_wv1_daily](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_we_mart_wv1_daily.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|we_mart.we_artist|we_mart.stats_we_d_integ_visit|
|we_mart.we_user|we_mart.stats_we_d_order_fc|
|we_mart.ws_fc_user_history|we_mart.stats_we_d_visit_new|
|we_mart.wv_comm_user|we_mart.stats_wv_d_art_comm_visit|
|we_mart.wv_server_log_base|we_mart.stats_wv_d_art_indi_comm_activity|
|weverse2.community_content_common_notice|we_mart.stats_wv_d_ctry_lang|
|weverse2.community_content_post|we_mart.stats_wv_d_live_agg|
| |we_mart.stats_wv_m_art_cohort_vst|
| |we_mart.stats_wv_m_art_comm_vst_day|
| |we_mart.stats_wv_m_retention_visit_by_type_comm|
| |we_mart.stats_wv_m_sess_duration|
| |we_mart.stats_wv_w_art_cohort_vst|
| |we_mart.stats_wv_w_art_comm_vst_day|
| |we_mart.stats_wv_w_art_indi_comm_activity|
| |we_mart.stats_wv_w_art_indi_comm_activity_art|
| |we_mart.stats_wv_w_retention_visit_by_type_comm|
| |we_mart.stats_wv_w_sess_duration|
| |we_mart.we_user_dic_engaged_by_project|
| |we_mart.we_user_visit|
| |we_mart.we_visit_monthly_base|
| |we_mart.we_visit_weekly_base|
| |we_mart.ws_platform_daily|
| |we_mart.wv_art_first_vst|
| |we_mart.wv_visit_daily_log|
| |we_mart.wv_visit_monthly|
| |we_mart.wv_visit_monthly_base|
| |we_mart.wv_visit_weekly|
| |we_mart.wv_visit_weekly_base|

## 🐤 Downstream Tables Info
  
### Downstream Tables
- **`we_mart.stats_wv_d_live_agg`**: 커뮤니티별 LIVE 재생 집계 스탯 
    - `we_mart.wv_sess_daily`  테이블에서 `key_date`, `we_member_id`, `we_art_id`, `we_art_name`, `user_ctry`, `is_fc` 컬럼을 사용하여 LIVE 시청자 수, 멤버십 가입자 수, 국가별 접속자 수 등을 집계.
    - `we_mart.wv_live`, `we_mart.wv_video_play`, `we_meta.we_country`, `we_mart.wv_media_reaction`, `we_mart.wv_comm_user`, `we_mart.we_artist` 테이블과 JOIN하여 LIVE 관련 정보 추가.
    - `key_date`, `we_art_id`, `we_art_name`, `user_type`, `media_type`, `live_type`, `platform`, `region_type` 컬럼을 파티션 키로 활용.
- **`we_mart.we_user_daily`**: 일간 통합 사용자 활동 이력
    - `we_mart.wv_sess_daily` 테이블에서 `key_date`, `we_member_id`, `we_art_id`, `user_ctry`, `is_fc`, `sess_id`, `sess_start_dt`, `sess_end_dt`, `sess_dur`, `user_sess_id`, `user_sess_start_dt`, `user_sess_end_dt`, `user_sess_dur`, `platform`, `os`, `app_ver` 컬럼을 사용하여 위버스 서비스 이용 관련 정보 추출.
    - `we_mart.we_user`, `we_mart.ws_order`, `we_mart.ws_user_buy`, `we_mart.ws_user_daily`, `we_mart.wv_comm_user`, `we_mart.wv_media_reaction`, `we_mart.wv_post_reaction`, `we_mart.wv_order`, `we_mart.we_user`, `we_mart.wv_video_play` 테이블과 JOIN하여 위버스, 위버스샵, 포닝 관련 정보 추가. 
    - `part_date` 컬럼을 파티션 키로 활용.
- **`we_mart.wv_visit_daily_log`**: 위버스 일별 방문 로그
    - `we_mart.wv_sess_daily` 테이블에서 `key_date`, `we_member_id`, `sess_ctry`, `user_sess_id`, `sess_dur` 컬럼을 사용하여 위버스 일별 방문 로그 생성.
    - `part_date` 컬럼을 파티션 키로 활용.

### Downstream View Tables
- **`we_mart.stats_we_d_visit_new`**: 방문이력 종합 통계
    - `we_mart.wv_sess_daily`, `we_mart.ws_user_daily`, `we_mart.ph_sess_daily`, `we_mart.wa_sess_daily`, `we_mart.we_artist`, `we_mart.view_we_country` 테이블과 JOIN하여 위버스, 위버스샵, 포닝, 앨범 서비스별 방문 통계 생성.
    - `part_month` 컬럼을 파티션 키로 활용.
- **`we_mart.stats_we_d_integ_v`**: 샵통합 방문자 통계
    - `wev_prod.we_mart.wv_dm_subscr` 테이블에서 `key_date`, `account_id`, `vst_type`, `is_ws_app`, `is_wv_app`, `is_ord` 컬럼을 사용하여 위버스, 위버스샵, 포닝 서비스 통합 방문 통계 생성.
    - `part_date` 컬럼을 파티션 키로 활용.  
---