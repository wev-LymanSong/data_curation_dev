
we_mart.we_jelly_charge
=======================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_mart**|
|**Table Type**|MART PRIMARY|
|**Partitioned by**| |
|**Created/ Last Updated At**|2023-05-10 / 2024-08-08|
|**Created By**|송재영|
|**Last Updated By**|송재영|
|**Collaborators**|송재영[20], 구민서[6]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2023-05-10|송재영|[PR](https://github.com/benxcorp/databricks/commit/2fa49acfc0ae158cb10333d4993db2207df08177)|
|2023-05-10|송재영|[PR](https://github.com/benxcorp/databricks/commit/e3c19e4599f029aac94a03d94d02b981a4d7699c)|
|2023-05-12|송재영|[PR](https://github.com/benxcorp/databricks/commit/75f953a8379aa388a8a9ebbf3cdb9d6cd495b117)|
|2023-07-21|송재영|[PR](https://github.com/benxcorp/databricks/commit/e1bb21f406cb0b32616e7ffdbcda0d93bf3822f8)|
|2023-07-25|송재영|[PR](https://github.com/benxcorp/databricks/commit/99cfabac5904eb073e650ce643739960f1e6a3c4)|
|2023-07-27|송재영|[PR](https://github.com/benxcorp/databricks/commit/202ca162c0038bcd119fe5147a43986f1f9218b7)|
|2023-08-02|송재영|[PR](https://github.com/benxcorp/databricks/commit/f6c10cb4390edff034676e44636daedf14bebfd0)|
|2023-09-13|송재영|[PR](https://github.com/benxcorp/databricks/commit/65097c805fa895e84903c3a0b63e2b8a5fb34628)|
|2023-09-18|송재영|[PR](https://github.com/benxcorp/databricks/commit/3c2c65e38e77ea09f4105682f3041fbb223f785c)|
|2023-09-18|송재영|[PR](https://github.com/benxcorp/databricks/commit/6e66abd8e650aca025d7e8bb821ef39890c3e28c)|
|2023-10-30|송재영|[PR](https://github.com/benxcorp/databricks/commit/bb989c25d4271adb8ce26d67a64922dbba35ab3a)|
|2023-10-31|구민서|[PR](https://github.com/benxcorp/databricks/commit/171ac599f22f35c840c62d9662fc64305441d312)|
|2023-11-02|구민서|[PR](https://github.com/benxcorp/databricks/commit/26ef97d4d2e7e1f15805c09decb5b44e620f289e)|
|2023-11-09|송재영|[PR](https://github.com/benxcorp/databricks/commit/f434fce5783268929e509e49ec73e29369dab4df)|
|2024-01-03|구민서|[PR](https://github.com/benxcorp/databricks/commit/45c4365383107ae1e8efe7211383c9f3924d8d17)|
|2024-01-03|구민서|[PR](https://github.com/benxcorp/databricks/commit/7838f1ace351dacc2e49be0dcd4d7e2bc2995372)|
|2024-01-04|구민서|[PR](https://github.com/benxcorp/databricks/commit/60bb531bea609d52ff6b2b442436ab60f4ddfe66)|
|2024-01-15|구민서|[PR](https://github.com/benxcorp/databricks/commit/7d1d59a1d74d176c40cdebbdf839a305d36dce1b)|
|2024-03-04|송재영|[PR](https://github.com/benxcorp/databricks/commit/9f316b99fdfbedd05d4d630bd2d0db01b40ef50e)|
|2024-03-06|송재영|[PR](https://github.com/benxcorp/databricks/commit/15cb24bbcb63a707307e11dcf5a62c928097aed4)|
|2024-05-29|송재영|[PR](https://github.com/benxcorp/databricks/commit/964c62c089c2238a9ce615e32905b55b6f5627aa)|
|2024-05-30|송재영|[PR](https://github.com/benxcorp/databricks/commit/f0d3c4ba3262ea1ef26037a5d1d8695930bec7a4)|
|2024-05-30|송재영|[PR](https://github.com/benxcorp/databricks/commit/513fb33efe109ee81d45bbcadd57d4a04567f2d9)|
|2024-07-01|송재영|[PR](https://github.com/benxcorp/databricks/commit/4f06e9b9ef59c8bcaefe6b79d759edb90dcbd60a)|
|2024-07-02|송재영|[PR](https://github.com/benxcorp/databricks/commit/21da1795061451c1397d4b388a87e68712a3483e)|
|2024-08-08|송재영|[PR](https://github.com/benxcorp/databricks/commit/137aad3401e77e90b82628e9c736bc5b9bc5d599)|
  
  
# TABLE NOTICE
  
### 테이블 개요

* **테이블 목적**: 위버스 플랫폼에서 젤리 충전 내역을 기록하는 테이블
* **데이터 레벨**: TRANSACTIONAL DATA
* **파티션 키**: 없음
* **주요 키**: `charge_id`

### 테이블 특징

* 젤리 충전 내역과 연관된 다양한 정보를 포함: 충전된 젤리 종류, 결제 방식, 결제 상태, 국가 정보 등
* `we_member_id`를 통해 위버스 플랫폼 유저 정보와 연결 가능
* `pur_transaction_id`를 통해 `we_mart.purchase_table` 테이블과 조인 가능
* `jelly_id`를 통해 `we_mart.we_jelly` 테이블과 조인 가능
* `product_id`를 통해 `we_meta.we_digital_product` 테이블과 조인 가능

### 데이터 추출 및 생성 과정

1. **주요 데이터 소스**:
    * `wev_prod.jelly.transaction`: 젤리 충전 내역 정보
    * `wev_prod.we_mart.we_jelly`: 젤리 상품 정보
    * `wev_prod.we_mart.we_user`: 위버스 유저 정보
    * `purchase_table`: 위버스 플랫폼에서 젤리 상품 구매 내역을 정리한 임시 테이블
    * `user_ctry`: 위버스 유저의 국가 정보를 정리한 임시 테이블
    * `wev_prod.we_meta.we_digital_product`: 젤리 상품의 판매 가격 정보
2. **데이터 전처리**:
    * `wev_prod.jelly.transaction` 테이블에서 `transaction_type`이 `CHARGE`인 데이터만 추출
    * `purchase_table` 테이블과 `wev_prod.jelly.transaction` 테이블을 `payment_id`를 통해 조인하여 결제 정보를 추가
    * `wev_prod.we_mart.we_jelly` 테이블과 `wev_prod.jelly.transaction` 테이블을 `point_id`를 통해 조인하여 젤리 상품 정보를 추가
    * `wev_prod.we_meta.we_digital_product` 테이블과 `wev_prod.jelly.transaction` 테이블을 `product_id`와 `jelly_store_item_id`를 통해 조인하여 상품 판매 가격 정보를 추가
    * `user_ctry` 테이블과 `wev_prod.jelly.transaction` 테이블을 `user_id`와 `cre_dt`를 통해 조인하여 위버스 유저의 국가 정보를 추가
3. **데이터 통합**:
    * 위에서 전처리된 데이터를 하나의 데이터프레임으로 통합
4. **최종 테이블 생성**:
    * 통합된 데이터프레임을 `we_mart.we_jelly_charge` 테이블에 저장

### 테이블 활용 가이드

* **주요 활용**:
    * 젤리 충전 내역 분석
    * 젤리 상품 판매 현황 분석
    * 젤리 결제 시스템 성능 분석
    * 위버스 유저의 젤리 충전 패턴 분석
* **조인 시 유의사항**:
    * `we_member_id`를 통해 `we_mart.we_user` 테이블과 조인하여 유저 정보를 추가할 수 있음
    * `pur_transaction_id`를 통해 `we_mart.purchase_table` 테이블과 조인하여 결제 정보를 추가할 수 있음
    * `jelly_id`를 통해 `we_mart.we_jelly` 테이블과 조인하여 젤리 상품 정보를 추가할 수 있음
    * `product_id`를 통해 `we_meta.we_digital_product` 테이블과 조인하여 상품 판매 가격 정보를 추가할 수 있음

### 추가 정보

* `jelly_qty_total` 컬럼은 일반 젤리 충전량과 프로모션 젤리 충전량을 합쳐서 나타냄
* `sale_price_krw` 컬럼은 젤리 상품의 판매 가격을 한화로 표시
* `ord_ctry_code` 컬럼은 젤리 충전 시 사용된 IP 주소 기반 국가 정보
* `wv_ctry_code` 컬럼은 위버스 유저의 접속 IP 주소 기반 국가 정보
* `run_timestamp` 컬럼은 젤리 충전 내역 데이터가 테이블에 적재된 시간을 나타냄  
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|we_member_id|bigint|위버스 플랫폼 통합 유저 ID|
|1|charge_id|string|충전 거래 ID(transaction_id)|
|2|jelly_id|bigint|젤리 ID(jelly.point.id)|
|3|product_id|bigint|젤리 상품 ID(product.product.id)|
|4|jelly_group_id|bigint|젤리 그룹 ID(jelly.point_group.id)|
|5|jelly_product_name|string|젤리 상품 명|
|6|promotion_jelly_id|bigint|젤리 프로모션 ID(jelly.promotion_point.id)|
|7|promotion_jelly_product_name|string|젤리 프로모션 상품 명|
|8|jelly_store_item_id|string|인앱 상품 ID|
|9|channel|string|젤리 상품이 속한 결제 채널|
|10|jelly_unit_price|double|젤리 내부 단가|
|11|jelly_selling_unit_price|double|젤리 판매 단가|
|12|promotion_jelly_unit_price|double|프로모션 젤리 환산 단가|
|13|jelly_qty|int|젤리 상품의 충전 젤리 수|
|14|jelly_qty_promotion|int|프로모션 젤리 상품의 충전 젤리 수|
|15|jelly_qty_total|int|전체 충전 젤리 수(일반 충전량 + 프로모션 충전량)|
|16|jelly_wallet_type|string|젤리 유무료 여부(PAID: 유료 충전 젤리, FREE: 무료 지급 젤리)|
|17|pay_system|string|결제 방식(= billing.purchase.system)|
|18|pay_method|string|결제 방식에 따른 결제 수단|
|19|pay_method_detail|string|결제 방식, 결제 수단에 따른 세부 결제 사|
|20|pay_card_type|string|결제에 사용한 체크/신용카드|
|21|payment_id|string|결제 ID(= billing.purchase.purchase_id)|
|22|trx_cre_dt|timestamp|거래 기준 생성일(UTC)|
|23|first_exp_dt|timestamp|첫 충전 시 만료 일시(UTC)|
|24|pur_transaction_id|string|구매 거래 ID(= billing.purchase.transaction_id)|
|25|is_pay|int|결제 여부|
|26|is_cx|int|결제 취소 여부|
|27|is_partial_cx|int|부분 취소 여부|
|28|ord_status|string|결제 상태|
|29|ord_ctry_code|string|결제 기준 IP 국가 정보|
|30|wv_ctry_code|string|위버스 접속 기준 IP 국가 정보|
|31|ord_item_qty|int|주문 상품 수|
|32|ord_item_amt|double|주문 상품 총 결제액|
|33|ord_item_cx_amt|double|주문 상품 총 취소액|
|34|sale_price_krw|double|상품 한화 판매 가격|
|35|currency_code|string|주문 통화|
|36|pur_confirmed_at|timestamp|결제 완료 일시(UTC)|
|37|cx_dt|timestamp|결제 취소 일시(UTC)|
|38|run_timestamp|timestamp|데이터 적재 일시(UTC)|
  
    
---
# HOW TO USE
  
### Downstream Table/View
- `we_jelly_charge` 테이블을 사용하여 `wv_user` 테이블과 조인하여 유저별 젤리 충전 정보를 생성하는 테이블
    - ```sql
      select 
      we_jelly_charge.we_member_id,
      we_jelly_charge.jelly_id,
      we_jelly_charge.jelly_qty,
      we_jelly_charge.jelly_qty_promotion,
      we_jelly_charge.jelly_wallet_type,
      we_jelly_charge.pay_system,
      we_jelly_charge.pay_method,
      we_jelly_charge.trx_cre_dt,
      we_user.wv_user_id,
      we_user.user_nm,
      we_user.user_email,
      we_user.user_phone
      from wev_prod.we_mart.we_jelly_charge
      left join wev_prod.we_mart.we_user
      on we_jelly_charge.we_member_id = we_user.we_member_id
      ```
- `we_jelly_charge` 테이블을 사용하여 월별 국가별 젤리 충전 현황을 집계하는 테이블
    - ```sql
      select 
      date_trunc('month', we_jelly_charge.trx_cre_dt) as charge_month,
      we_jelly_charge.wv_ctry_code,
      sum(we_jelly_charge.jelly_qty) as total_jelly_qty,
      count(distinct we_jelly_charge.charge_id) as charge_count
      from wev_prod.we_mart.we_jelly_charge
      group by 1, 2
      ```
- `we_jelly_charge` 테이블을 사용하여 유료/무료 젤리 충전 현황을 집계하는 테이블
    - ```sql
      select
      we_jelly_charge.jelly_wallet_type,
      sum(we_jelly_charge.jelly_qty) as total_jelly_qty,
      count(distinct we_jelly_charge.charge_id) as charge_count
      from wev_prod.we_mart.we_jelly_charge
      group by 1
      ```
- `we_jelly_charge` 테이블을 사용하여 결제 수단별 젤리 충전 현황을 집계하는 테이블
    - ```sql
      select
      we_jelly_charge.pay_system,
      we_jelly_charge.pay_method,
      sum(we_jelly_charge.jelly_qty) as total_jelly_qty,
      count(distinct we_jelly_charge.charge_id) as charge_count
      from wev_prod.we_mart.we_jelly_charge
      group by 1, 2
      ```

### Data Extraction
- 특정 기간 동안 특정 국가에서 발생한 젤리 충전 내역 조회
    - ```sql
      select 
      we_member_id,
      jelly_id,
      jelly_qty,
      jelly_qty_promotion,
      jelly_wallet_type,
      pay_system,
      pay_method,
      trx_cre_dt
      from wev_prod.we_mart.we_jelly_charge
      where wv_ctry_code = 'KR' 
      and trx_cre_dt >= '2024-01-01' 
      and trx_cre_dt < '2024-02-01'
      ```
- 특정 유저의 젤리 충전 내역 조회
    - ```sql
      select 
      charge_id,
      jelly_id,
      jelly_qty,
      jelly_qty_promotion,
      jelly_wallet_type,
      pay_system,
      pay_method,
      trx_cre_dt
      from wev_prod.we_mart.we_jelly_charge
      where we_member_id = 12345
      ```
- 특정 젤리 상품에 대한 충전 내역 조회
    - ```sql
      select 
      we_member_id,
      charge_id,
      jelly_qty,
      jelly_qty_promotion,
      jelly_wallet_type,
      pay_system,
      pay_method,
      trx_cre_dt
      from wev_prod.we_mart.we_jelly_charge
      where jelly_id = 12345
      ```
- 특정 결제 수단을 사용한 젤리 충전 내역 조회
    - ```sql
      select 
      we_member_id,
      charge_id,
      jelly_qty,
      jelly_qty_promotion,
      jelly_wallet_type,
      trx_cre_dt
      from wev_prod.we_mart.we_jelly_charge
      where pay_system = 'PG'
      and pay_method = 'CARD'
      ```  
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_we_mart_priority_daily`

### Update Interval: DAILY

### Update Type: OVERWRITE

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/mart/we_mart/we_jelly_charge.py)

### Airflow: [DAG](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_we_mart_priority_daily.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|billing.in_app_purchase|we_mart.stats_we_d_jelly_charge|
|jelly.point|we_mart.stats_we_d_jelly_charge_smry|
|jelly.transaction|we_mart.wv_jelly_transaction|
|payment.payment|we_mart.wv_order|
|test.we_jelly| |
|we_mart.we_jelly| |
|we_mart.we_reward_history| |
|we_mart.we_user| |
|we_mart.wv_user_ctry_history| |
|we_meta.we_digital_product| |

## 🐤 Downstream Tables Info
  
### Downstream Tables
- **`we_mart.wv_jelly_transaction`**: 젤리 충전/사용/환불/환급/만료에 대한 데이터를 담고 있는 테이블. 젤리 충전 내역을 포함한 모든 거래 내역을 기록한다. 
    - `we_mart.we_jelly_charge` 테이블을 사용하여 젤리 충전 정보를 가져온다.
    - 젤리 충전/사용/환불/환급/만료에 대한 데이터를 추출할 때 사용할 수 있다.
- **`we_mart.stats_we_d_jelly_charge`**: 젤리 충전에 대한 통계 데이터를 담고 있는 테이블. 
    - `we_mart.we_jelly_charge` 테이블을 사용하여 젤리 충전 정보를 가져온다.
    - 날짜별, 채널별, 결제 수단별 젤리 충전량, 거래 건수, 유저 수 등을 분석할 때 사용할 수 있다.
- **`we_mart.stats_we_d_jelly_charge_smry`**: 젤리 충전에 대한 통계 데이터를 요약한 테이블. 
    - `we_mart.we_jelly_charge` 테이블을 사용하여 젤리 충전 정보를 가져온다.
    - 날짜별, 채널별, 결제 수단별 젤리 충전량, 거래 건수, 유저 수 등을 요약하여 분석할 때 사용할 수 있다.
- **`we_mart.stats_we_m_jelly_jp_balance`**: 일본에서 충전된 젤리의 월별 잔액을 집계한 테이블. 
    - `we_mart.we_jelly_charge` 테이블을 사용하여 일본에서 충전된 젤리 정보를 가져온다.
    - 일본에서 충전된 젤리의 월별 잔액을 분석할 때 사용할 수 있다.

### Downstream View Tables
- **`charges`**: 젤리 충전 거래 내역을 임시 뷰로 생성. 
    - `jelly.transaction` 테이블과 `jelly.point` 테이블을 조인하여 젤리 충전 정보를 가져온다.
    - 젤리 충전 정보를 임시로 조회하거나 다른 쿼리의 조인 대상으로 사용할 때 사용할 수 있다.
- **`user_ctry`**: 위버스 유저의 국가 정보를 임시 뷰로 생성. 
    - `we_mart.wv_user_ctry_history` 테이블을 사용하여 유저의 국가 정보를 가져온다.
    - 유저의 국가 정보를 임시로 조회하거나 다른 쿼리의 조인 대상으로 사용할 때 사용할 수 있다.
- **`jelly_jpy`**: 일본에서 충전된 젤리의 잔액을 계산한 임시 뷰. 
    - `we_mart.we_jelly_charge` 테이블과 `we_mart.wv_jelly_transaction` 테이블을 조인하여 일본에서 충전된 젤리 정보를 가져온다.
    - 일본에서 충전된 젤리의 잔액을 계산하는 중간 과정에서 사용된다.
- **`temp_jelly`**: 일본에서 충전된 젤리의 월별 잔액을 집계한 임시 뷰. 
    - `jelly_jpy` 뷰와 `currency_table` 뷰를 조인하여 일본에서 충전된 젤리의 월별 잔액을 계산한다.
    - `stats_we_m_jelly_jp_balance` 테이블에 데이터를 로드하기 전에 사용된다.
- **`currency_table`**: 다양한 통화들의 변환 비율을 임시 뷰로 생성. 
    - `we_meta.currency_rate_etc` 테이블을 사용하여 통화 변환 비율을 가져온다.
    - 통화 변환 비율을 임시로 조회하거나 다른 쿼리의 조인 대상으로 사용할 때 사용할 수 있다.  
---