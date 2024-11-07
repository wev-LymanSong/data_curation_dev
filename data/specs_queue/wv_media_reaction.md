
we_mart.wv_media_reaction
=========================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_mart**|
|**Table Type**|MART PRIMARY|
|**Partitioned by**|`part_comm_id`, `part_date`|
|**Created/ Last Updated At**|2021-11-29 / 2024-07-03|
|**Created By**|박상민|
|**Last Updated By**|송재영|
|**Collaborators**|윤상혁[14], 송재영[7], 박상민[5], 구민서[1]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2021-11-29|박상민|[PR](https://github.com/benxcorp/databricks/commit/8dc809154614d40353684e2d2023adee4ec47cef)|
|2021-12-01|박상민|[PR](https://github.com/benxcorp/databricks/commit/cc3cd0becda316e17d965e85bf6078d2de2fd217)|
|2021-12-22|박상민|[PR](https://github.com/benxcorp/databricks/commit/a935c9dd835d0b40de0d969085712ce7be393927)|
|2022-01-04|박상민|[PR](https://github.com/benxcorp/databricks/commit/28815d69a6d10da118e7ffe350ad3d0fcd58d77a)|
|2022-01-05|박상민|[PR](https://github.com/benxcorp/databricks/commit/13c8c48ecd648066e5e686a8d2357f88b0cacd63)|
|2022-07-18|송재영|[PR](https://github.com/benxcorp/databricks/commit/6c380688a5f4f1ba2490b1639833d425698d8dce)|
|2022-07-18|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/f904a4924014252117ed33e97eb44565e25c7186)|
|2022-07-18|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/51e647354a0ca91f0567a102beee8797aec7eb49)|
|2022-07-19|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/f1ecccf56b8ea4aa49b49810e3a8fc7deab4ef50)|
|2022-07-20|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/e902a8a8dd7b7256ba429239f7d3c498bae7158b)|
|2022-07-21|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/055fdc18d386579f55a1a772bd3fe246d3a57860)|
|2022-07-21|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/93cbb288318bdea85f5f41fa3ae66a37c4432124)|
|2022-07-22|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/f25da1e5e1bcb580666189b1c01639a637377c94)|
|2022-07-25|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/74b40d6c2cf899abb4b76cc5de7e482e20264ae5)|
|2022-08-01|송재영|[PR](https://github.com/benxcorp/databricks/commit/c0e595008c462abf59d3b323472fb6ff5d301712)|
|2022-10-18|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/6bbc236af5492ff1fbf654fa2000e0854df17910)|
|2022-10-18|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/c5a9399f0df845452725ccc417851e80a1f54bc3)|
|2022-10-18|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/c98d7a2a82d5ea2adf93a3e3416209f33267e227)|
|2022-11-01|송재영|[PR](https://github.com/benxcorp/databricks/commit/3e77e4866771ad78f08b825ec0a28920fa218ec0)|
|2022-11-01|송재영|[PR](https://github.com/benxcorp/databricks/commit/c84dee2579b5f738f96cde2236c0242d7b618a49)|
|2022-11-14|송재영|[PR](https://github.com/benxcorp/databricks/commit/83e303dfaf21f61db5e8ffddaea837dfde1e6f1d)|
|2022-11-28|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/f9b20e39663d3529bd82fdf1356dd963b5008248)|
|2022-12-09|구민서|[PR](https://github.com/benxcorp/databricks/commit/bd183779c84e616084e05627330a1c8b98ca44bd)|
|2022-12-23|송재영|[PR](https://github.com/benxcorp/databricks/commit/0e0c099c6b874c7d5ff02da6d93ea9dc60dead75)|
|2023-10-16|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/6d3501e175a20e6423e7320054e6a58d3c0f0fdd)|
|2024-03-20|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/3f08ce4d6aed1bca75a7df13a2bcce930c7e54eb)|
|2024-07-03|송재영|[PR](https://github.com/benxcorp/databricks/commit/69b36d8e869e328cb096dbcf7e8a90a9520fcbe3)|
  
  
# TABLE NOTICE
  
### 테이블 개요

*   **테이블 목적**: 위버스 사용자가 미디어에 대한 리액션(댓글, 좋아요) 정보를 담고 있다.
*   **데이터 레벨**: AGGREGATED DATA(STATISTICS)
*   **파티션 키**: `part_comm_id`, `part_date`
*   **주요 키**: `key_date`, `media_id`, `wv_user_id`, `member_id`

### 테이블 특징
* `media_id` 컬럼을 통해 `we_mart.wv_media` 테이블과 조인 가능하다.
* `wv_user_id` 컬럼을 통해 `we_mart.we_user` 테이블과 조인 가능하다.
* `member_id` 컬럼은 위버스 사용자의 멤버십 정보를 나타내는 `we_member_id`와 `wv_user_id` 컬럼을 조합하여 생성했다.
* `we_art_id` 컬럼을 통해 `we_mart.we_artist` 테이블과 조인 가능하다.
* `comm_id` 컬럼은 위버스 커뮤니티 ID를 나타낸다.
* `is_fc` 컬럼은 위버스 사용자가 팬클럽 회원인지 여부를 나타낸다.
* `fc_id` 컬럼은 팬클럽 회원 ID를 나타낸다.
* `ip_ctry` 컬럼은 위버스 사용자의 IP 주소 기반 국가 코드를 나타낸다.
* `is_reaction`, `is_cmnt`, `is_like` 컬럼은 각각 리액션, 댓글, 좋아요 여부를 나타내는 boolean 값이다.
* `tot_cmnt`, `media_cmnt`, `cmnt_cmnt` 컬럼은 각각 댓글 수, 미디어 댓글 수, 대댓글 수를 나타낸다.
* `cmnt_reported`, `cmnt_blinded` 컬럼은 각각 신고 댓글 수, 블라인드 댓글 수를 나타낸다.
* `cmnt_lang_cnt`, `cmnt_lang` 컬럼은 각각 댓글 언어 수, 댓글 언어 목록을 나타낸다.
* `tot_like`, `media_like`, `cmnt_like` 컬럼은 각각 전체 좋아요 수, 미디어 좋아요 수, 댓글 좋아요 수를 나타낸다.
* `is_fc_only`, `is_photo`, `is_pitem` 컬럼은 각각 팬클럽 회원 전용 미디어 여부, 사진 여부, 구매 미디어 여부를 나타낸다.
* `media_dur` 컬럼은 미디어 재생 시간을 나타낸다.
* `tvod_id`, `svod_id`, `svod_group_id`, `paid_item_id` 컬럼은 각각 tvod_id, svod_id, svod_group_id, paid_item_id를 나타낸다.
* `media_cat_id`, `media_cat_name` 컬럼은 각각 미디어 카테고리 ID, 미디어 카테고리 이름을 나타낸다.
* `media_rel_dt` 컬럼은 미디어 릴리즈 일시를 나타낸다.
* `cnt_photo` 컬럼은 미디어 사진 수를 나타낸다.

### 데이터 추출 및 생성 과정

1.  **주요 데이터 소스**:
    *   `weverse2.community_content_comment`: 위버스 커뮤니티 댓글 정보
    *   `weverse2.community_content_comment_emotion_relation`: 위버스 커뮤니티 댓글 좋아요 정보
    *   `weverse2.community_content_post_emotion_relation`: 위버스 커뮤니티 게시글 좋아요 정보
    *   `weverse2.community_common_report`: 위버스 커뮤니티 신고 정보
    *   `weverse2.community_common_punish`: 위버스 커뮤니티 블라인드 정보
    *   `we_mart.wv_media`: 위버스 미디어 정보
    *   `we_mart.we_user`: 위버스 사용자 정보
    *   `we_mart.ws_fc_user_history`: 위버스 팬클럽 회원 정보
    *   `we_mart.wv_user_ctry_history`: 위버스 사용자 국가 정보
2.  **데이터 전처리**:
    *   `weverse2.community_content_comment` 테이블에서 댓글 작성 시간, 댓글 ID, 댓글 작성자 정보, 댓글 내용 등을 추출한다.
    *   `weverse2.community_content_comment_emotion_relation` 테이블에서 댓글 좋아요 정보를 추출한다.
    *   `weverse2.community_content_post_emotion_relation` 테이블에서 게시글 좋아요 정보를 추출한다.
    *   `weverse2.community_common_report` 테이블에서 댓글 신고 정보를 추출한다.
    *   `weverse2.community_common_punish` 테이블에서 댓글 블라인드 정보를 추출한다.
    *   `we_mart.wv_media` 테이블에서 미디어 ID, 미디어 타입, 미디어 이름, 미디어 릴리즈 일시 등을 추출한다.
    *   `we_mart.we_user` 테이블에서 위버스 사용자 정보를 추출한다.
    *   `we_mart.ws_fc_user_history` 테이블에서 팬클럽 회원 정보를 추출한다.
    *   `we_mart.wv_user_ctry_history` 테이블에서 위버스 사용자 국가 정보를 추출한다.
3.  **데이터 통합**:
    *   댓글 정보, 좋아요 정보, 신고 정보, 블라인드 정보, 미디어 정보, 사용자 정보, 팬클럽 회원 정보, 국가 정보 등을 `LEFT JOIN` 및 `FULL JOIN`을 사용하여 통합한다.
4.  **최종 테이블 생성**:
    *   통합된 데이터를 `we_mart.wv_media_reaction` 테이블에 저장한다.
    *   `part_comm_id`, `part_date` 컬럼을 파티션 키로 활용하여 데이터를 분할 저장한다.

### 테이블 활용 가이드

*   **주요 활용**:
    *   위버스 사용자의 미디어 리액션 분석
    *   미디어별 댓글, 좋아요 수 분석
    *   팬클럽 회원의 미디어 리액션 분석
    *   국가별 미디어 리액션 분석
    *   아티스트별 미디어 리액션 분석
*   **조인 시 유의사항**:
    *   `media_id` 컬럼을 사용하여 `we_mart.wv_media` 테이블과 조인하여 미디어 정보를 추가할 수 있다.
    *   `wv_user_id` 컬럼을 사용하여 `we_mart.we_user` 테이블과 조인하여 사용자 정보를 추가할 수 있다.
    *   `we_art_id` 컬럼을 사용하여 `we_mart.we_artist` 테이블과 조인하여 아티스트 정보를 추가할 수 있다.

### 추가 정보

*   `we_mart.wv_media_reaction` 테이블은 위버스 사용자의 미디어 리액션에 대한 통계 정보를 제공한다.
*   `key_date` 컬럼을 사용하여 특정 날짜의 데이터를 추출할 수 있다.
*   `media_id` 컬럼을 사용하여 특정 미디어에 대한 데이터를 추출할 수 있다.
*   `wv_user_id` 컬럼을 사용하여 특정 사용자에 대한 데이터를 추출할 수 있다.  
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|key_date|date|key_date|
|1|media_id|int|media_id|
|2|post_id|string|post_id|
|3|media_type|string|미디어 타입|
|4|section_type|string|post sectionType|
|5|media_name|string|미디어 명|
|6|wv_user_id|bigint|wv_user_id|
|7|member_id|string|member_id|
|8|we_member_id|bigint|we_member_id|
|9|is_fc|int|멤버쉽여부|
|10|fc_id|bigint|멤버쉽 id|
|11|ip_ctry|string|국가 코드|
|12|we_art_id|int|아티스트 id|
|13|we_art_name|string|아티스트 명|
|14|comm_id|bigint|커뮤니티 id|
|15|is_reaction|int|리액션 여부|
|16|is_cmnt|int|댓글 여부|
|17|is_like|int|좋아요 여부|
|18|is_cmnt_cmnt|int|대댓글 여부|
|19|is_cmnt_tags|int|댓글 태그 여부|
|20|tot_cmnt|bigint|댓글 수|
|21|media_cmnt|bigint|미디어 댓글 수|
|22|cmnt_cmnt|bigint|대댓글 수|
|23|cmnt_reported|bigint|신고 댓글 수|
|24|cmnt_blinded|bigint|블라인드 댓글 수|
|25|cmnt_lang_cnt|bigint|댓글 언어 수|
|26|cmnt_lang|array<string>|댓글 언어|
|27|tot_like|bigint|전체 좋아요|
|28|media_like|bigint|미디어 좋아요|
|29|cmnt_like|bigint|댓글 좋아요|
|30|is_fc_only|int|fc 회원전용 미디어 여부|
|31|is_photo|int|사진 여부|
|32|media_dur|bigint|미디어 재생시간|
|33|is_pitem|int|구매 미디어 여부|
|34|tvod_id|bigint|tvod_id|
|35|svod_id|bigint|svod_id|
|36|svod_group_id|bigint|svod_group_id|
|37|paid_item_id|bigint|paid_item_id|
|38|media_cat_id|array<int>|media_cat_id|
|39|media_cat_name|array<string>|미디어 카데고리 명|
|40|media_rel_dt|timestamp|미디어 릴리즈 일시|
|41|cnt_photo|bigint|미디어 사진 수|
|42|part_comm_id|bigint|part_comm_id|
|43|part_date|string|part_date|
|44|run_timestamp|timestamp|run_timestamp|
  
    
---
# HOW TO USE
  
### Downstream Table/View
- `we_mart.stats_wv_d_art_indi_comm_activity` 테이블 생성에 `wv_media_reaction` 테이블 활용
    - ```sql
    create or replace table we_mart.stats_wv_d_art_indi_comm_activity
    (
        key_date date comment '기준일자'
        , we_art_id int comment '아티스트id'
        , we_art_name string comment '아티스트명'
        , art_indi_id string comment '멤버id'
        , art_indi_name string comment '멤버명'
        , tot_post_cnt bigint comment '포스트 수'
        , tofans_post_cnt bigint comment '모먼트 수'
        , tot_cmnt_cnt bigint comment '댓글 수'
        , fan_post_cmnt_cnt bigint comment '팬포스트 댓글 수'
        , art_post_cmnt_cnt bigint comment '아티스트 포스트 댓글 수'
        , fan_post_like_cnt bigint comment '팬포스트 좋아요 수'
        , post_view_cnt bigint comment '포스트 조회 수'
        , fltr_view_cnt bigint comment '팬레터 조회 수'
        , run_timestamp timestamp comment '적재 시간(UTC)'
    )
    using DELTA
    partitioned by(key_date)
    comment '아티스트 멤버 단위 커뮤니티 활동 통계마트'
    ;
    ```
- `we_mart.stats_we_d_visit_new` 테이블 생성에 `wv_media_reaction` 테이블 활용
    - ```sql
    create or replace table we_mart.stats_we_d_visit_new
    (
      key_date date comment '기준일자'
    , we_art_id int comment '아티스트id'
    , we_art_name string comment '아티스트명'
    , ctry string comment '국가코드'
    , cnt_usr_d bigint comment 'DAU'
    , cnt_usr_w bigint comment 'WAU'
    , cnt_usr_m bigint comment 'MAU'
    , sum_usr_sess_dur bigint comment '체류시간(초)'
    , run_timestamp timestamp comment '적재 시간(UTC)'
    )
    using DELTA
    partitioned by(key_date)
    comment '위버스 방문 통계마트'
    ;
    ```
- `we_mart.stats_wv_d_comm_join` 테이블 생성에 `wv_media_reaction` 테이블 활용
    - ```sql
    create or replace table we_mart.stats_wv_d_comm_join
    (
        key_date date comment '기준일자'
        , we_art_id int comment '아티스트id'
        , we_art_name string comment '아티스트명'
        , comm_id bigint comment '커뮤니티 id'
        , nor_cnt bigint comment '일별 커뮤니티 가입자 수'
        , run_timestamp timestamp comment '적재 시간(UTC)'
    )
    using DELTA
    partitioned by(key_date)
    comment '위버스 커뮤니티 가입 통계마트'
    ;
    ```
- `we_mart.stats_wv_d_fanletter_reaction` 테이블 생성에 `wv_media_reaction` 테이블 활용
    - ```sql
    create or replace table we_mart.stats_wv_d_fanletter_reaction
    (
        key_date date comment '기준일자'
        , we_art_id int comment '아티스트id'
        , we_art_name string comment '아티스트명'
        , reaction_art_indi_name string comment '멤버명'
        , like_d_a bigint comment '팬레터 좋아요 수'
        , run_timestamp timestamp comment '적재 시간(UTC)'
    )
    using DELTA
    partitioned by(key_date)
    comment '위버스 팬레터 좋아요 통계마트'
    ;
    ```

### Data Extraction
- 특정 아티스트의 특정 날짜 미디어 반응 정보 추출
    - ```sql
    select * 
    from we_mart.wv_media_reaction
    where part_date = '2024-01-01'
    and we_art_id = 1234
    ;
    ```
- 특정 날짜에 특정 미디어 타입에 대한 사용자 반응 정보 추출
    - ```sql
    select * 
    from we_mart.wv_media_reaction
    where part_date = '2024-01-01'
    and media_type = 'VIDEO'
    ;
    ```
- 특정 아티스트의 특정 날짜 미디어 댓글 수 집계
    - ```sql
    select we_art_id, sum(tot_cmnt) as tot_cmnt_cnt
    from we_mart.wv_media_reaction
    where part_date = '2024-01-01'
    group by we_art_id
    ;
    ```
- 특정 날짜에 미디어 반응이 있는 사용자 수 집계
    - ```sql
    select count(distinct wv_user_id) as reaction_user_cnt
    from we_mart.wv_media_reaction
    where part_date = '2024-01-01'
    and is_reaction = 1
    ;
    ```  
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_we_mart_priority_daily`

### Update Interval: DAILY

### Update Type: OVERWRITE

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/mart/we_mart/wv_media_reaction.py)

### Airflow: [DAG](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_we_mart_priority_daily.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|we_mart.we_user|we_mart.stats_wv_d_comm_engage_usr|
|we_mart.ws_fc_user_history|we_mart.stats_wv_d_live|
|we_mart.wv_media|we_mart.stats_wv_d_live_agg|
|we_mart.wv_user_ctry_history|we_mart.stats_wv_m_comm_engage_usr|
|weverse2.community_common_punish|we_mart.stats_wv_w_comm_engage_usr|
|weverse2.community_common_report| |
|weverse2.community_content_comment| |
|weverse2.community_content_comment_emotion_relation| |
|weverse2.community_content_common_community_media_relation| |
|weverse2.community_content_post_emotion_relation| |

## 🐤 Downstream Tables Info
  
### Downstream Tables
- **stats_wv_w_art_indi_comm_activity** : 아티스트 커뮤니티 활동성 통계
    - `wv_media_reaction` 테이블에서 `is_reaction` 컬럼을 활용하여 미디어 반응(댓글, 좋아요) 수를 집계
    - `we_art_id` 컬럼을 파티션 키로 활용
    - `key_date` 컬럼을 파티션 키로 활용
    -  `part_date` 컬럼을 기준으로 해당 주에 속하는 데이터를 필터링
    - 해당 테이블에서 아티스트별 커뮤니티 활동성을 분석할 수 있음
- **stats_wv_d_comm_engage_usr** : 커뮤니티 적극활동유저수 통계
    - `wv_media_reaction` 테이블에서 `is_reaction` 컬럼을 활용하여 미디어 반응(댓글, 좋아요) 수를 집계
    - `we_art_id` 컬럼을 파티션 키로 활용
    - `key_date` 컬럼을 파티션 키로 활용
    -  `part_date` 컬럼을 기준으로 해당일에 속하는 데이터를 필터링
    - 해당 테이블에서 커뮤니티 적극 활동 유저 수를 분석할 수 있음
- **stats_wv_m_comm_engage_usr** : 커뮤니티 적극활동유저수 통계
    - `wv_media_reaction` 테이블에서 `is_reaction` 컬럼을 활용하여 미디어 반응(댓글, 좋아요) 수를 집계
    - `we_art_id` 컬럼을 파티션 키로 활용
    - `key_date` 컬럼을 파티션 키로 활용
    - `part_date` 컬럼을 기준으로 해당 월에 속하는 데이터를 필터링
    - 해당 테이블에서 커뮤니티 적극 활동 유저 수를 분석할 수 있음
- **stats_wv_w_comm_engage_usr** : 커뮤니티 적극활동유저수 통계
    - `wv_media_reaction` 테이블에서 `is_reaction` 컬럼을 활용하여 미디어 반응(댓글, 좋아요) 수를 집계
    - `we_art_id` 컬럼을 파티션 키로 활용
    - `key_date` 컬럼을 파티션 키로 활용
    - `part_date` 컬럼을 기준으로 해당 주에 속하는 데이터를 필터링
    - 해당 테이블에서 커뮤니티 적극 활동 유저 수를 분석할 수 있음
- **stats_wv_d_media_activity** : 일간 위버스 미디어 유저 활동 집계
    - `wv_media_reaction` 테이블에서 `is_reaction` 컬럼을 활용하여 미디어 반응(댓글, 좋아요) 수를 집계
    - `we_art_id` 컬럼을 파티션 키로 활용
    - `key_date` 컬럼을 파티션 키로 활용
    - `part_date` 컬럼을 기준으로 해당일에 속하는 데이터를 필터링
    - 해당 테이블에서 미디어별 유저 활동을 분석할 수 있음
- **stats_wv_d_media_agg_activity** : 일간 위버스 미디어 유저 활동 집계
    - `wv_media_reaction` 테이블에서 `is_reaction` 컬럼을 활용하여 미디어 반응(댓글, 좋아요) 수를 집계
    - `we_art_id` 컬럼을 파티션 키로 활용
    - `key_date` 컬럼을 파티션 키로 활용
    - `part_date` 컬럼을 기준으로 해당일에 속하는 데이터를 필터링
    - 해당 테이블에서 미디어별 유저 활동을 분석할 수 있음
- **stats_wv_d_media_agg_activity_cp** : 일간 위버스 미디어 유저 활동 집계
    - `wv_media_reaction` 테이블에서 `is_reaction` 컬럼을 활용하여 미디어 반응(댓글, 좋아요) 수를 집계
    - `we_art_id` 컬럼을 파티션 키로 활용
    - `key_date` 컬럼을 파티션 키로 활용
    - `part_date` 컬럼을 기준으로 해당일에 속하는 데이터를 필터링
    - 해당 테이블에서 미디어별 유저 활동을 분석할 수 있음

### Downstream View Tables
- **media_act_data_raw** : `wv_media_reaction` 테이블에서 `is_reaction`, `is_cmnt`, `is_like` 컬럼을 활용하여 미디어 반응(댓글, 좋아요) 정보를 추출
    - `we_art_id` 컬럼을 파티션 키로 활용
    - `key_date` 컬럼을 파티션 키로 활용
    - `part_date` 컬럼을 기준으로 해당일에 속하는 데이터를 필터링
    - 해당 뷰 테이블에서 미디어별 유저 활동 정보를 추출할 수 있음
- **med_cat_data** : `wv_media_reaction` 테이블에서 `is_reaction`, `is_cmnt`, `is_like` 컬럼을 활용하여 미디어 반응(댓글, 좋아요) 정보를 추출
    - `we_art_id` 컬럼을 파티션 키로 활용
    - `key_date` 컬럼을 파티션 키로 활용
    - `part_date` 컬럼을 기준으로 해당일에 속하는 데이터를 필터링
    - `media_cat_id` 컬럼을 파티션 키로 활용
    - 해당 뷰 테이블에서 미디어 카테고리별 유저 활동 정보를 추출할 수 있음
- **pkg_cat_data** : `wv_media_reaction` 테이블에서 `is_reaction`, `is_cmnt`, `is_like` 컬럼을 활용하여 미디어 반응(댓글, 좋아요) 정보를 추출
    - `we_art_id` 컬럼을 파티션 키로 활용
    - `key_date` 컬럼을 파티션 키로 활용
    - `part_date` 컬럼을 기준으로 해당일에 속하는 데이터를 필터링
    - `media_cat_id` 컬럼을 파티션 키로 활용
    - 해당 뷰 테이블에서 패키지 카테고리별 유저 활동 정보를 추출할 수 있음
- **art_total_data** : `wv_media_reaction` 테이블에서 `is_reaction`, `is_cmnt`, `is_like` 컬럼을 활용하여 미디어 반응(댓글, 좋아요) 정보를 추출
    - `we_art_id` 컬럼을 파티션 키로 활용
    - `key_date` 컬럼을 파티션 키로 활용
    - `part_date` 컬럼을 기준으로 해당일에 속하는 데이터를 필터링
    - 해당 뷰 테이블에서 아티스트별 유저 활동 정보를 추출할 수 있음
- **total_total_data** : `wv_media_reaction` 테이블에서 `is_reaction`, `is_cmnt`, `is_like` 컬럼을 활용하여 미디어 반응(댓글, 좋아요) 정보를 추출
    - `key_date` 컬럼을 파티션 키로 활용
    - `part_date` 컬럼을 기준으로 해당일에 속하는 데이터를 필터링
    - 해당 뷰 테이블에서 전체 유저 활동 정보를 추출할 수 있음
- **cp_prod_data** : `wv_media_reaction` 테이블에서 `is_reaction`, `is_cmnt`, `is_like` 컬럼을 활용하여 미디어 반응(댓글, 좋아요) 정보를 추출
    - `key_date` 컬럼을 파티션 키로 활용
    - `part_date` 컬럼을 기준으로 해당일에 속하는 데이터를 필터링
    - `product_id` 컬럼을 파티션 키로 활용
    - 해당 뷰 테이블에서 상품별 유저 활동 정보를 추출할 수 있음
- **cp_pkgc_data** : `wv_media_reaction` 테이블에서 `is_reaction`, `is_cmnt`, `is_like` 컬럼을 활용하여 미디어 반응(댓글, 좋아요) 정보를 추출
    - `key_date` 컬럼을 파티션 키로 활용
    - `part_date` 컬럼을 기준으로 해당일에 속하는 데이터를 필터링
    - `media_cat_id` 컬럼을 파티션 키로 활용
    - 해당 뷰 테이블에서 패키지 카테고리별 유저 활동 정보를 추출할 수 있음
- **cp_tot_data** : `wv_media_reaction` 테이블에서 `is_reaction`, `is_cmnt`, `is_like` 컬럼을 활용하여 미디어 반응(댓글, 좋아요) 정보를 추출
    - `key_date` 컬럼을 파티션 키로 활용
    - `part_date` 컬럼을 기준으로 해당일에 속하는 데이터를 필터링
    - 해당 뷰 테이블에서 전체 유저 활동 정보를 추출할 수 있음  
---