
we_mart.wv_post_view
====================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_mart**|
|**Table Type**|MART PRIMARY|
|**Partitioned by**|`part_art_id`, `part_date`|
|**Created/ Last Updated At**|2023-05-22 / 2024-05-31|
|**Created By**|윤상혁|
|**Last Updated By**|윤상혁|
|**Collaborators**|윤상혁[15]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2023-05-22|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/036a7c6730e88fe153bbf3f27289b06841598722)|
|2023-05-22|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/dc988d5391c421e785b726cf2eafacabfc809cb4)|
|2023-05-23|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/dc200f8814bde71f6ea845bbf2e37ff7a12c0310)|
|2023-05-23|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/72b96fce347d5c53d2bfe76d7a21e9e81f9b846f)|
|2023-05-23|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/772628873f8c0269df3f66d6c1ef079bf8c87521)|
|2023-06-23|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/b1cfaf0411ab60d943fa6714887145c8d036d280)|
|2023-06-23|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/98b676639c3eca3709687efd51f33918aacca188)|
|2023-07-21|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/09c7be762cda2ac64810fe1cabf0098fa9d008ab)|
|2023-07-21|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/b5c813c03fc45fa8a97778625e7907b3e80f7493)|
|2023-07-26|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/09755a8b0cb7d75cedce71514f6e27bc257c68e1)|
|2023-08-04|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/d9fba45f2d10a5338f157ef54e2a8d76c932b47c)|
|2023-09-12|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/7494246a0c38c59d2a71a428318f6fc787453908)|
|2024-01-05|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/83c0b3a0c6f6cd6e976307b0b93e1bbfd636836c)|
|2024-04-01|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/9f66860b1a30c92e06e20c39305b82c0912cb284)|
|2024-05-31|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/6821da09f2b7a0dbe219f2382540b91143ae8291)|
  
  
# TABLE NOTICE
  
### 테이블 개요

* **테이블 목적**: Weverse 플랫폼 내 일일 포스트 조회 정보를 담고 있다.
* **데이터 레벨**: AGGREGATED DATA(STATISTICS)
* **파티션 키**: `part_art_id`, `part_date`
* **주요 키**: `key_date`, `hour`, `we_art_id`, `post_id`, `wv_user_id`

### 테이블 특징

* `we_art_id`와 `we_art_name`은 각각 아티스트 ID와 이름을 나타내며, `we_mart.we_artist` 테이블에서 가져온다.
* `is_comm_user`, `is_fc`, `is_fc_post`, `is_art_post`는 각각 커뮤니티 가입자 유무, 멤버십 유무, FC only 포스트 유무, 아티스트 포스트 유무를 나타내는 boolean 값이다.
* `media_type`은 포스트에 포함된 미디어 종류를 나타내며, `IMAGE`, `YOUTUBE`, `LIVE`, `VOD` 등의 값을 가진다.
* `media_cat_ids`는 미디어 카테고리 ID를 담은 배열 형태로, `we_mart.wv_media` 테이블에서 가져온다.
* `media_rel_dt`는 미디어 릴리즈 일시를 나타내며, `we_mart.wv_media` 테이블에서 가져온다.

### 데이터 추출 및 생성 과정

1. **주요 데이터 소스**:
    * `wev_prod.we_mart.wv_server_log_base`: Weverse 플랫폼 사용자의 서버 로그 정보를 담고 있다.
    * `wev_prod.we_mart.wv_live`: Weverse 플랫폼 내 라이브 방송 정보를 담고 있다.
    * `wev_prod.weverse2.community_content_post`: Weverse 커뮤니티 내 포스트 정보를 담고 있다.
    * `wev_prod.we_mart.wv_comm_user`: Weverse 커뮤니티 가입자 정보를 담고 있다.
    * `wev_prod.weverse2.community_member_special_member`: Weverse 커뮤니티 내 특별 멤버 정보를 담고 있다.
    * `wev_prod.we_mart.we_artist`: Weverse 플랫폼 내 아티스트 정보를 담고 있다.
    * `wev_prod.we_mart.wv_media`: Weverse 플랫폼 내 미디어 정보를 담고 있다.
2. **데이터 전처리**:
    * `wev_prod.we_mart.wv_server_log_base` 테이블에서 `url` 컬럼을 기반으로 조회 수(`view_cnt`)와 재생 수(`play_cnt`)를 계산한다.
    * `wev_prod.we_mart.wv_live` 테이블과 조인하여 라이브 방송 여부(`is_live`)와 종류(`live_type`)를 확인한다.
    * `wev_prod.weverse2.community_content_post` 테이블과 조인하여 포스트 작성자 ID(`auther_user_id`), 섹션 유형(`section_type`), 포스트 유형(`post_type`), 언어(`lang`) 등의 정보를 가져온다.
    * `wev_prod.we_mart.wv_comm_user` 테이블과 조인하여 커뮤니티 가입자 유무(`is_comm_user`)를 확인한다.
    * `wev_prod.weverse2.community_member_special_member` 테이블과 조인하여 아티스트 특별 멤버 정보(`art_indi_name`)를 가져온다.
    * `wev_prod.we_mart.we_artist` 테이블과 조인하여 아티스트 ID(`we_art_id`)와 이름(`we_art_name`)을 가져온다.
    * `wev_prod.we_mart.wv_media` 테이블과 조인하여 미디어 정보(`is_photo`, `is_pitem`, `media_cat_ids`, `media_name`, `media_rel_dt`, `media_dur`, `cnt_photo`)를 가져온다.
3. **데이터 통합**:
    * 위의 과정을 통해 추출된 데이터를 통합하여 `wv_post_view` 테이블을 생성한다.
4. **최종 테이블 생성**:
    * `key_date`, `hour`, `we_art_id`, `post_id`, `wv_user_id`를 기준으로 데이터를 집계한다.
    * `part_art_id`와 `part_date`를 파티션 키로 사용하여 테이블을 분할한다.

### 테이블 활용 가이드

* **주요 활용**:
    * Weverse 플랫폼 내 일일 포스트 조회 트렌드 분석
    * 아티스트별 포스트 조회 현황 분석
    * 커뮤니티 가입자와 비가입자의 포스트 조회 행태 비교 분석
    * 포스트 유형별 조회 수 분석
    * 미디어 종류별 조회 수 분석
* **조인 시 유의사항**:
    * `wev_prod.we_mart.wv_media` 테이블과 조인할 때는 `post_id` 컬럼을 사용해야 한다.
    * `wev_prod.we_mart.we_artist` 테이블과 조인할 때는 `we_art_id` 컬럼을 사용해야 한다.

### 추가 정보

* `run_timestamp` 컬럼은 데이터 적재 시간을 나타낸다.
* 본 테이블은 일일 단위로 집계된 데이터를 담고 있다.
* `we_mart.wv_post_view` 테이블은 `wev_prod.we_mart.wv_media` 테이블의 `part_date` 컬럼과 동일한 날짜를 기준으로 파티션된다.  
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|key_date|date|기준일자|
|1|hour|int|기준시각|
|2|we_art_id|int|아티스트ID|
|3|we_art_name|string|아티스트명|
|4|wv_user_id|string|weverse user_id|
|5|we_member_id|bigint|account_id|
|6|is_comm_user|int|커뮤니티 가입자 유무|
|7|is_fc|int|멤버십 여부|
|8|ip_ctry|string|접속 국가|
|9|post_id|string|조회한 post_id|
|10|view_cnt|bigint|조회수|
|11|play_cnt|bigint|영상 재생수|
|12|auther_user_id|bigint|post 작성자 wv_user_id|
|13|section_type|string|포스트 위치|
|14|post_type|string|포스트 종류|
|15|is_fc_post|int|fc only 포스트 유무|
|16|lang|string|포스트 작성 언어|
|17|is_art_post|int|아티스트 포스트 유무|
|18|art_indi_name|string|작성 아티스트명|
|19|media_type|string|미디어 종류|
|20|is_photo|int|포토 포스트 유무|
|21|is_pitem|int|유료 미디어 유무|
|22|media_cat_ids|array<int>|미디어 카테고리 id|
|23|media_name|string|미디어 제목|
|24|media_rel_dt|timestamp|미디어 릴리즈 일시|
|25|media_dur|int|미디어 총 재생시간|
|26|cnt_photo|int|미디어 포함 포토수|
|27|part_art_id|int|partition key|
|28|part_date|string|partition key|
|29|run_timestamp|timestamp|적재 시간|
  
    
---
# HOW TO USE
  
No content.  
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_we_mart_priority_daily`

### Update Interval: DAILY

### Update Type: APPEND

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/mart/we_mart/wv_post_view.py)

### Airflow: [DAG](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_we_mart_priority_daily.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|we_mart.we_artist|we_mart.stats_wv_d_art_content_act|
|we_mart.wv_comm_user|we_mart.stats_wv_d_comm_engage_usr|
|we_mart.wv_live|we_mart.stats_wv_d_media_live_pv_bypost|
|we_mart.wv_media|we_mart.stats_wv_d_posting_activity|
|we_mart.wv_server_log_base|we_mart.stats_wv_m_comm_engage_usr|
|weverse2.community_content_post|we_mart.stats_wv_w_comm_engage_usr|
|weverse2.community_member_special_member| |

## 🐤 Downstream Tables Info
  
No content.  
---