
we_mart.ws_album_sale
=====================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_mart**|
|**Table Type**|MART PRIMARY|
|**Partitioned by**|`part_date`|
|**Created/ Last Updated At**|2022-12-08 / 2024-08-21|
|**Created By**|송재영|
|**Last Updated By**|송재영|
|**Collaborators**|송재영[8], 구민서[1]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2022-12-08|송재영|[PR](https://github.com/benxcorp/databricks/commit/a31a165cadc4305359d7aae4155e229526669f4e)|
|2022-12-09|구민서|[PR](https://github.com/benxcorp/databricks/commit/bd183779c84e616084e05627330a1c8b98ca44bd)|
|2023-01-18|송재영|[PR](https://github.com/benxcorp/databricks/commit/14a51e34fb3141b2169a8fb3ef59af68d571dc67)|
|2023-02-06|송재영|[PR](https://github.com/benxcorp/databricks/commit/77568161a6c5b1b66a3be9ff26617b052c1891bd)|
|2023-06-27|송재영|[PR](https://github.com/benxcorp/databricks/commit/dfbf61f8bba4dbdc03e5fa12192366161ed0b03f)|
|2023-07-19|송재영|[PR](https://github.com/benxcorp/databricks/commit/b7caf5ca888638f86c1958e1e046b2053b12a9f8)|
|2023-10-27|송재영|[PR](https://github.com/benxcorp/databricks/commit/a673836ae888e689223e0083ba5c7874dd3b3691)|
|2023-10-27|송재영|[PR](https://github.com/benxcorp/databricks/commit/0b6a0e02caa8580479aa8367ffc038c7dfcd896c)|
|2024-08-21|송재영|[PR](https://github.com/benxcorp/databricks/commit/ade9bf92d7f221603932c53f81f190b173f360ef)|
  
  
# TABLE NOTICE
  
### 테이블 개요

* **테이블 목적**: 위버스샵 앨범 판매 정보를 담은 테이블
* **데이터 레벨**: TRANSACTIONAL DATA
* **파티션 키**: `part_date`
* **주요 키**: `sale_id`, `ord_item_id`

### 테이블 특징

* 위버스샵에서 판매된 앨범에 대한 상세 정보를 제공
* `ord_cre_dt`, `pay_dt`, `cx_dt`, `first_album_pay_dt`, `first_album_qty_pay_dt`, `first_album_opt_pay_dt`, `first_album_qty_opt_pay_dt`, `first_album_shop_pay_dt`, `first_album_qty_shop_pay_dt`, `first_album_opt_shop_pay_dt`, `first_album_qty_opt_shop_pay_dt` 컬럼은 모두 UTC 시간 기준
* 앨범 단위, 옵션 단위, 샵별 단위로 최초 결제 시간을 기록
* `run_timestamp` 컬럼은 데이터 적재 시간을 기록

### 데이터 추출 및 생성 과정

1. **주요 데이터 소스**:
    * `we_mart.ws_order` : 위버스샵 주문 정보
    * `we_meta.ws_album` : 위버스샵 앨범 정보
2. **데이터 전처리**:
    * `we_meta.ws_album` 테이블에서 `album_scm_option_type` 컬럼을 제외하고 `wa_album_id` 컬럼을 추가
    * `we_meta.ws_album` 테이블과 `we_mart.wa_album` 테이블을 `album_id` 컬럼으로 조인하여 `has_weverse_album` 컬럼 생성
    * `we_mart.ws_order` 테이블의 모든 시간 컬럼에 -9 hours를 적용하여 UTC 시간으로 변환
3. **데이터 통합**:
    * `we_mart.ws_order` 테이블과 `album_meta` 테이블을 `sale_id` 컬럼으로 조인
    * `logi_cat` 컬럼이 'ALBUM'이고 `is_pay` 컬럼이 1인 데이터만 추출
4. **최종 테이블 생성**:
    * `pay_dt` 컬럼과 `cx_dt` 컬럼에 +9 hours를 적용하여 당일 결제 및 취소 데이터만 추출
    * `run_timestamp` 컬럼과 `part_date` 컬럼을 추가

### 테이블 활용 가이드

* **주요 활용**:
    * 위버스샵 앨범 판매 현황 분석
    * 앨범별, 옵션별, 샵별 매출 분석
    * 결제 패턴 분석
* **조인 시 유의사항**:
    * `we_mart.ws_order` 테이블과 조인할 경우 `sale_id` 컬럼을 사용
    * `we_meta.ws_album` 테이블과 조인할 경우 `album_id` 컬럼을 사용
    * 시간 관련 컬럼은 모두 UTC 시간 기준이므로 시간 변환에 유의

### 추가 정보

* `album_meta` 테이블은 임시 테이블로, `ws_album` 테이블을 변환하여 생성
* `has_weverse_album` 컬럼은 해당 앨범이 위버스 앨범을 가지고 있는지 여부를 나타냄
* `is_weverse_album` 컬럼은 위버스 앨범 여부를 나타냄
* `wa_album_id` 컬럼은 위버스 앨범 ID를 나타냄  
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|we_member_id|bigint|we_member_id|
|1|is_fc|int|구매시 멤버십 상태 여부|
|2|shop|string|위버스샵의 구매 샵 명|
|3|ord_sheet_number|bigint|주문번호|
|4|ord_item_id|bigint|상품 주문 ID|
|5|ws_label_name|string|아티스트 소속 레이블명|
|6|we_art_id|int|we_art_id|
|7|we_art_name|string|we_art_name|
|8|sale_id|bigint|판매 id|
|9|goods_name|string|상품명|
|10|goods_option_code|string|상품 코드명|
|11|goods_option_name|string|상품 옵션명|
|12|album_id|int|앨범 고유 번호|
|13|album_name|string|정식 앨범명 (가급적 영어)|
|14|has_weverse_album|int|해당 앨범이 위버스 앨범을 가지고 있는지 여부|
|15|album_qty_type|string|앨범 판매 단위 종류 (랜덤, 세트, 옵션 등)|
|16|album_option_type|string|앨범 패키지 및 형태 구분 (디럭스, 일반, 위버스앨범, 스페셜, 콤팩트, KiT..)|
|17|album_physical_type|string|앨범 물리 형태 구분(실물 앨범 유형)|
|18|is_weverse_album|int|위버스 앨범 여부|
|19|wa_album_id|int|위버스 앨범 ID|
|20|sale_price|double|판매금액|
|21|currency_code|string|통화|
|22|ord_item_qty|int|주문상품 수량|
|23|ord_item_cx_qty|int|취소/환불상품 수량|
|24|album_qty|int|앨범수량|
|25|ord_item_amt|double|주문상품 금액|
|26|ord_item_amt_krw|double|주문상품 금액 (KRW)|
|27|ord_item_cx_amt|double|취소/환불상품 금액|
|28|ord_item_cx_amt_krw|double|취소/환불상품 금액 (KRW)|
|29|currency_rate|double|환율|
|30|ord_cre_dt|timestamp|주문 생성 시간 (UTC)|
|31|pay_dt|timestamp|결제 완료 시간 (UTC)|
|32|cx_dt|timestamp|취소 시간 (UTC)|
|33|first_album_pay_dt|timestamp|앨범 단위 가장 먼저 결제 완료한 시간 (UTC)|
|34|first_album_qty_pay_dt|timestamp|앨범 판매 수 단위 가장 먼저 결제 완료한 시간 (UTC)|
|35|first_album_opt_pay_dt|timestamp|앨범 옵션 단위 가장 먼저 결제 완료한 시간 (UTC)|
|36|first_album_qty_opt_pay_dt|timestamp|앨범 판매 수 and 옵션 단위 가장 먼저 결제 완료한 시간 (UTC)|
|37|first_album_shop_pay_dt|timestamp|샵별 앨범 단위 가장 먼저 결제 완료한 시간 (UTC)|
|38|first_album_qty_shop_pay_dt|timestamp|샵별 앨범 판매 수 단위 가장 먼저 결제 완료한 시간 (UTC)|
|39|first_album_opt_shop_pay_dt|timestamp|샵별 앨범 옵션 단위 가장 먼저 결제 완료한 시간 (UTC)|
|40|first_album_qty_opt_shop_pay_dt|timestamp|샵별 앨범 판매 수 and 옵션 단위 가장 먼저 결제 완료한 시간 (UTC)|
|41|ctry_code|string|user 국가코드|
|42|ship_ctry_code|string|배송 국가코드|
|43|run_timestamp|timestamp|데이터 적재 시간 (UTC)|
|44|part_date|string|파티션 key date 값|
  
    
---
# HOW TO USE
  
No content.  
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_ws_mart_daily`

### Update Interval: DAILY

### Update Type: APPEND

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/mart/we_mart/ws_album_sale.py)

### Airflow: [DAG](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_ws_mart_daily.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|we_mart.wa_album|we_mart.stats_ws_d_album_cumul_chart_release|
|we_mart.ws_order|we_mart.stats_ws_d_album_cumul_kpi|
|we_meta.ws_album|we_mart.stats_ws_d_album_sale|
| |we_mart.stats_ws_d_album_sale_scm|
| |we_mart.stats_ws_d_album_sale_smry|

## 🐤 Downstream Tables Info
  
`
### Downstream Tables
- **stats_ws_d_album_sale** : 일간 앨범 판매 통계 마트
    - `we_mart.ws_album_sale` 테이블을 사용하여 국가, 멤버십, 앨범 종류, 샵별로 일별 앨범 판매 통계를 집계
    - 특정 앨범의 일별 판매 통계를 확인하려면 `album_id` 컬럼을 이용하여 필터링
    - 특정 국가의 일별 판매 통계를 확인하려면 `ctry_code` 컬럼을 이용하여 필터링
    - 특정 앨범의 첫 구매 날짜 기준으로 일별 판매 통계를 확인하려면 `first_album_pay_dt` 컬럼을 이용하여 필터링
- **stats_ws_w_album_sale_mart_upsert** : 위버스샵 앨범 상품 판매 지표 백필
    - `we_mart.ws_album_sale` 테이블을 사용하여 `we_mart.stats_ws_d_album_sale_smry` 테이블과 비교하여 앨범 판매 수량 차이를 확인하고 백필
    - 백필 대상 앨범은 `we_mart.stats_ws_d_album_sale` 테이블과 `we_mart.stats_ws_d_album_sale_smry` 테이블의 판매 수량 차이가 10장 이상인 앨범으로 필터링
    - 백필 대상 앨범에 대해 `we_mart.ws_album_sale` 테이블을 업데이트
    - 백필 대상 앨범에 대해 `we_mart.stats_ws_d_album_sale` 테이블과 `we_mart.stats_ws_d_album_sale_smry` 테이블을 업데이트
- **stats_ws_d_album_sale_smry** : 일간 위버스 앨범 판매 요약 통계 마트
    - `we_mart.ws_album_sale` 테이블을 사용하여 국가, 멤버십, 앨범 종류, 샵별로 일별 앨범 판매 요약 통계를 집계
    - 특정 앨범의 일별 판매 요약 통계를 확인하려면 `album_id` 컬럼을 이용하여 필터링
    - 특정 국가의 일별 판매 요약 통계를 확인하려면 `ctry_code` 컬럼을 이용하여 필터링
    - 특정 앨범의 첫 구매 날짜 기준으로 일별 판매 요약 통계를 확인하려면 `first_album_pay_dt` 컬럼을 이용하여 필터링
- **stats_ws_d_album_scm** : SCM 옵션을 적용한 앨범 판매 통계 마트
    - `we_mart.ws_album_sale` 테이블을 사용하여 앨범 판매 수량을 SCM 옵션별로 집계
    - `we_meta.ws_album` 테이블을 사용하여 앨범 메타 정보를 가져오고, `we_meta.ws_album_scm_exc` 테이블을 사용하여 앨범 발매일 예외 케이스를 적용
    - 특정 앨범의 SCM 옵션별 판매 통계를 확인하려면 `album_id` 컬럼을 이용하여 필터링
    - 특정 앨범의 첫 구매 날짜 기준으로 SCM 옵션별 판매 통계를 확인하려면 `first_album_pay_dt` 컬럼을 이용하여 필터링
- **stats_ws_d_album_cumul_chart_release** : 위버스샵 앨범 판매 <=> 앨범 차트 반영분 집계 및 차이
    - `we_mart.ws_album_sale` 테이블을 사용하여 위버스샵 앨범 판매 수량을 집계
    - `we_meta.ws_album_latest` 테이블을 사용하여 앨범 메타 정보를 가져오고, `weverseshop.album_chart_release` 테이블을 사용하여 앨범 차트 반영 정보를 가져옴
    - 특정 앨범의 위버스샵 판매 수량과 앨범 차트 반영 수량의 차이를 확인하려면 `album_id` 컬럼을 이용하여 필터링
    - 특정 앨범의 초동 기간 마감일 기준으로 위버스샵 판매 수량과 앨범 차트 반영 수량의 차이를 확인하려면 `album_cumulative_date` 컬럼을 이용하여 필터링

### Downstream View Tables
- **album_meta** : `we_meta.ws_album` 테이블을 사용하여 앨범 메타 정보를 가져오고, `we_mart.wa_album` 테이블을 사용하여 위버스 앨범 정보를 조인
    - `we_mart.ws_album_sale` 테이블을 사용하여 앨범 메타 정보를 가져오고, 위버스 앨범 정보를 조인
    - 특정 앨범의 메타 정보를 확인하려면 `album_id` 컬럼을 이용하여 필터링
- **album_sale_data** : `we_mart.ws_album_sale` 테이블을 사용하여 앨범 판매 정보를 가져오고, 앨범별, 회원별, 주문번호별, 판매 ID별로 최종 취소 여부를 확인
    - `we_mart.ws_album_sale` 테이블을 사용하여 앨범 판매 정보를 가져오고, 앨범별, 회원별, 주문번호별, 판매 ID별로 최종 취소 여부를 확인
    - 특정 앨범의 판매 정보를 확인하려면 `album_id` 컬럼을 이용하여 필터링
- **album_meta_{album_id}** : `we_meta.ws_album` 테이블을 사용하여 특정 앨범의 메타 정보를 가져오고, `we_mart.wa_album` 테이블을 사용하여 위버스 앨범 정보를 조인
    - `we_mart.ws_album` 테이블을 사용하여 특정 앨범의 메타 정보를 가져오고, `we_mart.wa_album` 테이블을 사용하여 위버스 앨범 정보를 조인
    - 특정 앨범의 메타 정보를 확인하려면 `album_id` 컬럼을 이용하여 필터링
- **temp_stats_main** : `we_mart.ws_album_sale` 테이블을 사용하여 특정 앨범의 일별 판매 통계를 집계
    - `we_mart.ws_album_sale` 테이블을 사용하여 특정 앨범의 일별 판매 통계를 집계
    - 특정 앨범의 일별 판매 통계를 확인하려면 `album_id` 컬럼을 이용하여 필터링
- **temp_stats_smry** : `we_mart.ws_album_sale` 테이블을 사용하여 특정 앨범의 일별 판매 요약 통계를 집계
    - `we_mart.ws_album_sale` 테이블을 사용하여 특정 앨범의 일별 판매 요약 통계를 집계
    - 특정 앨범의 일별 판매 요약 통계를 확인하려면 `album_id` 컬럼을 이용하여 필터링
- **expt_base** : 구글 시트에서 앨범 SCM 관련 데이터를 가져옴
    - 구글 시트에서 앨범 SCM 관련 데이터를 가져옴
    - 특정 앨범의 SCM 관련 데이터를 확인하려면 `album_id` 컬럼을 이용하여 필터링
- **expt** : `expt_base` 뷰를 사용하여 앨범 SCM 관련 데이터를 가공
    - `expt_base` 뷰를 사용하여 앨범 SCM 관련 데이터를 가공
    - 특정 앨범의 SCM 관련 데이터를 확인하려면 `album_id` 컬럼을 이용하여 필터링
- **nvr_base** : 구글 시트에서 네이버 앨범 판매 데이터를 가져옴
    - 구글 시트에서 네이버 앨범 판매 데이터를 가져옴
    - 특정 앨범의 네이버 판매 데이터를 확인하려면 `album_id` 컬럼을 이용하여 필터링
- **base** : `we_mart.stats_ws_d_album_sale_scm` 테이블을 사용하여 SCM 옵션별 앨범 판매 통계를 가져오고, `expt` 뷰와 `nvr_base` 뷰를 사용하여 외부 채널 판매 데이터를 조인
    - `we_mart.stats_ws_d_album_sale_scm` 테이블을 사용하여 SCM 옵션별 앨범 판매 통계를 가져오고, `expt` 뷰와 `nvr_base` 뷰를 사용하여 외부 채널 판매 데이터를 조인
    - 특정 앨범의 SCM 옵션별 판매 통계를 확인하려면 `album_id` 컬럼을 이용하여 필터링
- **base_cx** : `we_mart.stats_ws_d_album_sale_scm` 테이블을 사용하여 SCM 옵션별 앨범 판매 통계를 가져오고, `expt` 뷰와 `nvr_base` 뷰를 사용하여 외부 채널 판매 데이터를 조인 (취소 당일 반영)
    - `we_mart.stats_ws_d_album_sale_scm` 테이블을 사용하여 SCM 옵션별 앨범 판매 통계를 가져오고, `expt` 뷰와 `nvr_base` 뷰를 사용하여 외부 채널 판매 데이터를 조인 (취소 당일 반영)
    - 특정 앨범의 SCM 옵션별 판매 통계를 확인하려면 `album_id` 컬럼을 이용하여 필터링
- **target_albums** : `we_meta.ws_album_latest` 테이블을 사용하여 앨범 메타 정보를 가져오고, 앨범 초동 기간 마감이 아직 안된 앨범들을 필터링
    - `we_meta.ws_album_latest` 테이블을 사용하여 앨범 메타 정보를 가져오고, 앨범 초동 기간 마감이 아직 안된 앨범들을 필터링
    - 특정 앨범의 메타 정보를 확인하려면 `album_id` 컬럼을 이용하여 필터링
- **chart** : `weverseshop.album_chart_release` 테이블을 사용하여 앨범 차트 반영 정보를 가져옴
    - `weverseshop.album_chart_release` 테이블을 사용하여 앨범 차트 반영 정보를 가져옴
    - 특정 앨범의 차트 반영 정보를 확인하려면 `album_id` 컬럼을 이용하여 필터링
- **ws** : `we_mart.ws_album_sale` 테이블을 사용하여 위버스샵 앨범 판매 정보를 가져옴
    - `we_mart.ws_album_sale` 테이블을 사용하여 위버스샵 앨범 판매 정보를 가져옴
    - 특정 앨범의 위버스샵 판매 정보를 확인하려면 `album_id` 컬럼을 이용하여 필터링  
---