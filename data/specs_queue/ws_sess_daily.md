
we_mart.ws_sess_daily
=====================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_mart**|
|**Table Type**|MART PRIMARY|
|**Partitioned by**|`part_date`|
|**Created/ Last Updated At**|2023-11-06 / 2023-11-06|
|**Created By**|박상민|
|**Last Updated By**|박상민|
|**Collaborators**|박상민[1]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2023-11-06|박상민|[PR](https://github.com/benxcorp/databricks/commit/866e7a324036f3894626e5af7d5303afa4fb60ed)|
  
  
# TABLE NOTICE
  
### 테이블 개요

*   **테이블 목적**: 위버스샵 일별 세션 정보를 담고 있습니다.
*   **데이터 레벨**: TRANSACTIONAL DATA
*   **파티션 키**: `part_date`
*   **주요 키**: `sess_id`, `user_sess_id`

### 테이블 특징
* `sess_id`는  `sess_start_dt`와 `device_id`를 조합하여 생성됩니다.
* `user_sess_id`는 로그인 여부에 따라 `sess_id` 또는 `sess_id`와 `account_id`를 조합하여 생성됩니다.
* `sess_start_dt`, `sess_end_dt`, `user_sess_start_dt`, `user_sess_end_dt`는 모두 UTC 타임존을 기준으로 합니다.
* `platform`, `os`, `app_ver`는 `new_dvc_id`를 기준으로 마지막 값을 가져옵니다.
* `user_ctry`, `user_lang`, `sess_ctry`, `sess_lang`은 해당 컬럼의 마지막 값을 가져옵니다.
* `is_fc`는 Fan Club 회원 여부를 나타내는 컬럼입니다.

### 데이터 추출 및 생성 과정

1.  **주요 데이터 소스**:
    *   `wev_prod.service_log.weplyapi_client_log`: 위버스샵 클라이언트 로그 데이터
    *   `wev_prod.we_mart.we_artist`: 아티스트 정보 데이터
    *   `wev_prod.we_mart.ws_fc_user_history`: Fan Club 회원 정보 데이터
2.  **데이터 전처리**:
    *   `user_device_id`를 기반으로 `account_fill_1` 컬럼을 생성합니다.
    *   `account_fill_1`을 기반으로 `account_sess_seq`를 생성하여 로그인 세션을 구분합니다.
    *   `wv_device_id`, `s_device_type`, `user_device_id`를 사용하여 `new_dvc_id`를 생성합니다.
    *   `new_dvc_id`와 `log_dt`를 사용하여 `sess_seq`를 생성합니다.
    *   `sess_seq`를 사용하여 `sess_id`, `sess_start_dt`, `sess_end_dt`를 생성합니다.
    *   `sess_id`를 기반으로 `account_fill` 컬럼을 다시 생성합니다.
    *   `account_fill`과 `s_wemember_id`를 비교하여 `user_id_matched` 컬럼을 생성합니다.
    *   `user_id_matched`를 사용하여 `user_id_fill` 컬럼을 생성합니다.
    *   `sess_start_dt`와 `sess_end_dt`를 사용하여 `sess_dur`를 생성합니다.
    *   `user_sess_start_dt`와 `user_sess_end_dt`를 사용하여 `user_sess_dur`를 생성합니다.
3.  **데이터 통합**:
    *   `we_art_id`, `we_art_name` 컬럼을 `we_artist` 테이블에서 조인합니다.
    *   `we_member_id` 컬럼을 `ws_fc_user_history` 테이블에서 조인합니다.
4.  **최종 테이블 생성**:
    *   필요한 컬럼만 선택하고 중복된 데이터를 제거합니다.
    *   `run_timestamp` 컬럼을 추가합니다.

### 테이블 활용 가이드

*   **주요 활용**:
    *   위버스샵 세션 정보 분석
    *   사용자 행동 패턴 분석
    *   Fan Club 회원 세션 분석
*   **조인 시 유의사항**:
    *   `wev_prod.we_mart.ws_user_daily` 테이블과 조인하여 사용자 정보를 함께 분석할 수 있습니다.
    *   `wev_prod.we_mart.ws_goods_daily` 테이블과 조인하여 상품 정보를 함께 분석할 수 있습니다.

### 추가 정보

*   `is_wv`는 위버스 방문자 여부를 나타내는 컬럼입니다.
*   `is_device_login`은 `new_dvc_id`를 기준으로 로그인 여부를 나타내는 컬럼입니다.
*   `is_sess_login`은 `sess_id`를 기준으로 로그인 여부를 나타내는 컬럼입니다.
*   `shop`은 위버스샵 종류를 나타내는 컬럼입니다.
*   `part_date`는 파티션 컬럼입니다.
*   `run_timestamp`는 데이터 추출 및 생성 시간을 나타내는 컬럼입니다.  
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|key_date|date|방문일자|
|1|ws_user_id|string|shop user_id|
|2|account_id|string|account_id|
|3|device_id|string|기기id(브라우저id)|
|4|is_wv|int|위버스 방문자 여부 |
|5|user_ctry|string|유저 접속국가|
|6|user_lang|string|유저 언어|
|7|sess_ctry|string|session 접속국가(최종)|
|8|sess_lang|string|세션 언어|
|9|ws_art_id|bigint|위버스샵 id|
|10|we_art_id|int|아티스트id|
|11|we_art_name|string|아티스트명|
|12|shop|string|shop|
|13|is_device_login|int|device 로그인여부|
|14|is_sess_login|int|sess 로그인 여부|
|15|sess_id|string|로그시작일시(yyyyMMddHHmmssSSS)\|user_info_device_id|
|16|sess_start_dt|timestamp|세션id 최초일시(UTC)|
|17|sess_end_dt|timestamp|세션id 최종일시(UTC)|
|18|sess_dur|bigint|세션 체류 시간|
|19|user_sess_id|string|session_id + user_id|
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
- 샵 통합 후 위버스 앱 및 샵 앱 사용 여부를 추가한 테이블 생성
    - ```sql
      create table wev_prod.we_mart.ws_sess_daily_v2
      as
      select
      *
      , case when platform = 'APP' and is_wv = 1 then 1 else 0 end as is_wv_app
      , case when platform = 'APP' and is_wv = 0 then 1 else 0 end as is_ws_app
      from wev_prod.we_mart.ws_sess_daily
      ;
      ```
- 위버스 앱 사용자의 샵 방문 및 구매 통계를 위한 테이블 생성
    - ```sql
      create table wev_prod.we_mart.stats_wv_user_ws_visit
      as
      select
      a.key_date
      , a.we_member_id
      , a.we_art_name
      , sum(b.is_ws_app) as ws_visit_cnt
      , sum(b.is_sess_login) as ws_login_cnt
      , count(distinct b.sess_id) as ws_sess_cnt
      , sum(case when b.is_sess_login = 1 then b.sess_dur else 0 end) as ws_sess_dur
      , count(distinct b.user_sess_id) as ws_user_sess_cnt
      , sum(case when b.is_sess_login = 1 then b.user_sess_dur else 0 end) as ws_user_sess_dur
      from wev_prod.we_mart.wv_sess_daily a
      left join wev_prod.we_mart.ws_sess_daily_v2 b on a.we_member_id = b.account_id and a.key_date = b.key_date
      group by 1,2,3
      ;
      ```
- 위버스 샵 사용자의 위버스 앱 이용 정보를 위한 뷰 생성
    - ```sql
      create or replace view wev_prod.we_mart.v_ws_user_wv_visit as
      select
      a.key_date
      , a.account_id
      , a.we_art_name
      , sum(b.is_wv_app) as wv_visit_cnt
      , sum(b.is_sess_login) as wv_login_cnt
      , count(distinct b.sess_id) as wv_sess_cnt
      , sum(case when b.is_sess_login = 1 then b.sess_dur else 0 end) as wv_sess_dur
      , count(distinct b.user_sess_id) as wv_user_sess_cnt
      , sum(case when b.is_sess_login = 1 then b.user_sess_dur else 0 end) as wv_user_sess_dur
      from wev_prod.we_mart.ws_sess_daily a
      left join wev_prod.we_mart.ws_sess_daily_v2 b on a.account_id = b.account_id and a.key_date = b.key_date
      where b.is_wv_app = 1
      group by 1,2,3
      ;
      ```

### Data Extraction
- 특정 기간 동안 위버스샵 앱을 통해 구매한 사용자들의 국가별 구매 금액 추출
    - ```sql
      select
      user_ctry
      , sum(ord_item_amt_krw) as total_purchase_amount
      from wev_prod.we_mart.ws_sess_daily
      where key_date between '2024-01-01' and '2024-02-01'
      and platform = 'APP'
      and is_sess_login = 1
      group by 1
      order by 2 desc
      ;
      ```
- 특정 아티스트의 위버스샵에서 특정 기간 동안 발생한 주문 건수와 총 주문 금액 추출
    - ```sql
      select
      count(distinct ord_sheet_number) as order_count
      , sum(ord_item_amt_krw) as total_order_amount
      from wev_prod.we_mart.ws_sess_daily
      where key_date between '2024-01-01' and '2024-02-01'
      and we_art_name = 'ARTIST'
      ;
      ```
- 특정 기간 동안 위버스샵에서 발생한 구매 건수와 구매 금액을 플랫폼별로 집계하여 추출
    - ```sql
      select
      platform
      , count(distinct ord_sheet_number) as order_count
      , sum(ord_item_amt_krw) as total_order_amount
      from wev_prod.we_mart.ws_sess_daily
      where key_date between '2024-01-01' and '2024-02-01'
      group by 1
      order by 2 desc
      ;
      ```  
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_ws_mart_daily`

### Update Interval: DAILY

### Update Type: OVERWRITE

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/mart/we_mart/ws_sess_daily.py)

### Airflow: [DAG](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_ws_mart_daily.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|service_log.weplyapi_client_log|we_mart.stats_we_d_integ_visit|
|we_mart.we_artist|we_mart.stats_ws_d_ord_shop_integ|
|we_mart.ws_fc_user_history|we_mart.ws_platform_daily|

## 🐤 Downstream Tables Info
  
### Downstream Tables
- **`we_mart.ws_platform_daily`**: 일별 위버스샵 방문/결제 플랫폼 확인
    - `ws_sess_daily` 테이블에서 `key_date`, `account_id`, `platform`, `is_wv`, `user_sess_id`, `user_sess_start_dt`, `user_sess_end_dt` 컬럼을 사용하여 위버스샵 방문 정보를 추출
    - `ws_order` 테이블과 조인하여 결제 정보(주문 수량, 금액)를 추가
    - `wv_sess_daily` 테이블과 조인하여 위버스 방문 여부를 확인
    - `key_date`, `account_id`, `vst_type`, `vst_type_dtl`, `vst_platform`, `pay_qty`, `pay_krw`, `pay_order`, `part_date`, `run_timestamp` 컬럼을 사용하여 데이터를 추출
- **`we_mart.stats_ws_d_ord_shop_integ`**: 샵통합 관련 구매 상품의 구매 플랫폼 측정용 통계
    - `ws_sess_daily` 테이블에서 `key_date`, `account_id`, `platform`, `is_wv`, `user_sess_id`, `user_sess_start_dt`, `user_sess_end_dt` 컬럼을 사용하여 위버스샵 방문 정보를 추출
    - `ws_order` 테이블과 조인하여 주문 정보(아티스트 ID, 상품 ID, 상품 종류, 주문 수량, 금액)를 추가
    - `key_date`, `we_art_id`, `shop`, `sale_id`, `goods_id`, `album_qty`, `goods_cat`, `logi_cat`, `vst_type`, `vst_platform`, `vst_type_dtl`, `cnt_pay_user`, `cnt_ord`, `cnt_ord_item`, `pay_krw`, `pay_qty`, `part_date`, `run_timestamp` 컬럼을 사용하여 데이터를 추출
- **`we_mart.stats_we_d_integ_visit`**: 샵통합 방문자 통계
    - `ws_sess_daily` 테이블에서 `key_date`, `account_id`, `device_id`, `platform`, `is_wv` 컬럼을 사용하여 위버스샵 방문 정보를 추출
    - `wv_sess_daily` 테이블과 조인하여 위버스 방문 정보를 추가
    - `ws_order` 테이블과 조인하여 주문 정보를 추가
    - `key_date`, `date_type`, `vst_type_dtl`, `vst_type`, `is_ws_app`, `is_wv_app`, `is_ord`, `cnt_user`, `part_date`, `run_timestamp` 컬럼을 사용하여 데이터를 추출
- **`we_mart.stats_we_w_shop_integ_visit`**: 주간 샵통합 방문자 통계
    - `ws_sess_daily` 테이블에서 `key_date`, `account_id`, `device_id`, `platform`, `is_wv` 컬럼을 사용하여 위버스샵 방문 정보를 추출
    - `wv_sess_daily` 테이블과 조인하여 위버스 방문 정보를 추가
    - `ws_order` 테이블과 조인하여 주문 정보를 추가
    - `key_date`, `date_type`, `vst_type_dtl`, `vst_type`, `is_ws_app`, `is_wv_app`, `is_ord`, `cnt_user`, `part_date`, `run_timestamp` 컬럼을 사용하여 데이터를 추출

### Downstream View Tables
- **`we_mart.stats_ws_d_ord_shop_integ_summary`**: 샵통합 관련 구매 상품의 구매 플랫폼 측정용 통계 요약
    - `stats_ws_d_ord_shop_integ` 테이블에서 `key_date`, `platform`, `cnt_pay_user`, `pay_krw`, `pay_qty` 컬럼을 사용하여 요약 통계를 생성
    - `key_date`, `platform`, `au`, `pay_user`, `pay_krw`, `part_date`, `run_timestamp` 컬럼을 사용하여 데이터를 추출  
---