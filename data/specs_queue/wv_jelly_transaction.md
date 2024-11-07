
we_mart.wv_jelly_transaction
============================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_mart**|
|**Table Type**|MART PRIMARY|
|**Partitioned by**|`part_date`|
|**Created/ Last Updated At**|2023-03-10 / 2024-08-16|
|**Created By**|송재영|
|**Last Updated By**|송재영|
|**Collaborators**|송재영[15]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2023-03-10|송재영|[PR](https://github.com/benxcorp/databricks/commit/cd7184dd7f741fa4245bfd96e32866094bdb681b)|
|2023-03-10|송재영|[PR](https://github.com/benxcorp/databricks/commit/feca64dfbcb44c59d89512220d256d7b9d33c2be)|
|2023-03-15|송재영|[PR](https://github.com/benxcorp/databricks/commit/eaeb09cd09433181bafe5de4e9b3865265476f5c)|
|2023-04-17|송재영|[PR](https://github.com/benxcorp/databricks/commit/019f1ba3dbce611897fcebc9454cfc21acd23ede)|
|2023-04-26|송재영|[PR](https://github.com/benxcorp/databricks/commit/874674bc1a3ab394f3f9fe59f77f9e0f50bc9022)|
|2023-04-27|송재영|[PR](https://github.com/benxcorp/databricks/commit/f6593ba97c58e5416bb3cf56b68ddb120afd2e2e)|
|2023-05-10|송재영|[PR](https://github.com/benxcorp/databricks/commit/2fa49acfc0ae158cb10333d4993db2207df08177)|
|2023-07-21|송재영|[PR](https://github.com/benxcorp/databricks/commit/e1bb21f406cb0b32616e7ffdbcda0d93bf3822f8)|
|2023-07-24|송재영|[PR](https://github.com/benxcorp/databricks/commit/b2aa76dc196efce2a5fc850d348fa05eb4025207)|
|2023-07-31|송재영|[PR](https://github.com/benxcorp/databricks/commit/654d3851c9b0abe6c52f400de519f0a563faf322)|
|2023-08-14|송재영|[PR](https://github.com/benxcorp/databricks/commit/47705b019662ec9c4244071fe3f8c603a7034e4f)|
|2023-11-16|송재영|[PR](https://github.com/benxcorp/databricks/commit/2aa87eafd6f19e6d9ab56ec6f95322916f0fd9ca)|
|2024-03-04|송재영|[PR](https://github.com/benxcorp/databricks/commit/9f316b99fdfbedd05d4d630bd2d0db01b40ef50e)|
|2024-07-02|송재영|[PR](https://github.com/benxcorp/databricks/commit/21da1795061451c1397d4b388a87e68712a3483e)|
|2024-08-16|송재영|[PR](https://github.com/benxcorp/databricks/commit/72511e0e2ba0add937bbabe376b7cedba5791704)|
  
  
# TABLE NOTICE
  
### 테이블 개요

* **테이블 목적**: 위버스 플랫폼에서 발생하는 젤리 거래 내역을 기록 및 분석하기 위한 테이블
* **데이터 레벨**: TRANSACTIONAL DATA
* **파티션 키**: `part_date`
* **주요 키**: `we_member_id`, `transaction_id`, `ldg_cre_dt`

### 테이블 특징

* 위버스 플랫폼 내 젤리 충전, 사용, 환불, 만료 등 모든 거래 내역을 담고 있으며, 각 거래에 대한 상세 정보를 제공
* 젤리 충전/환불에 의한 입수량(`income`)과 젤리 사용/환불/만료에 의한 유출량(`outcome`)을 기록하여 젤리 잔액 변동 추적 가능
* 젤리로 상품 구매 시, 구매 정보(상품 ID, 구매 수량, 지불 젤리 수량 등)를 기록하여 젤리 사용 내역 상세 분석 가능
* `run_timestamp` 컬럼을 통해 데이터 적재 시간을 기록하여 데이터 최신성 확인 가능

### 데이터 추출 및 생성 과정

1. **주요 데이터 소스**:
    * `jelly.ledger` 테이블: 젤리 거래 내역을 기록하는 테이블
    * `we_mart.we_jelly_charge` 테이블: 젤리 충전 정보를 기록하는 테이블
    * `jelly.transaction` 테이블: 젤리 거래 내역(충전, 사용, 환불 등)을 기록하는 테이블
    * `we_mart.wv_order` 테이블: 젤리로 상품 구매 시, 주문 정보를 기록하는 테이블
    * `we_mart.we_user` 테이블: 위버스 유저 정보를 기록하는 테이블
    * `user_ctry` 뷰: `we_mart.wv_user_ctry_history` 테이블에서 유저별 최신 국가 정보를 추출하여 생성한 뷰
2. **데이터 전처리**:
    * `jelly.transaction` 테이블의 `transaction_type` 컬럼 값이 `EXPIRE`이고 `refund_id` 컬럼 값이 null이 아닌 경우, 해당 거래 유형을 `REFUND`로 변경
    * `we_mart.wv_order` 테이블에서 젤리로 상품 구매 시, 상품당 젤리 지불량(`product_sale_price`)을 계산
3. **데이터 통합**:
    * `jelly.ledger` 테이블을 기준으로, 각 젤리 거래에 대한 정보를 `we_mart.we_jelly_charge`, `jelly.transaction`, `we_mart.wv_order`, `we_mart.we_user`, `user_ctry` 테이블과 조인하여 통합
4. **최종 테이블 생성**:
    * 통합된 데이터를 `we_mart.wv_jelly_transaction` 테이블에 적재

### 테이블 활용 가이드

* **주요 활용**:
    * 젤리 거래 내역 분석: 젤리 충전, 사용, 환불, 만료 현황 분석, 젤리 잔액 변동 추이 분석
    * 젤리 사용 패턴 분석: 젤리로 상품 구매 현황 분석, 구매 상품별 젤리 지불량 분석
    * 젤리 충전/환불 분석: 젤리 충전/환불 시기, 수량 분석, 충전/환불 방법 분석
* **조인 시 유의사항**:
    * `we_member_id` 컬럼을 사용하여 `we_mart.we_user` 테이블과 조인하여 유저 정보를 추가할 수 있음
    * `transaction_id` 컬럼을 사용하여 `jelly.transaction` 테이블과 조인하여 거래 상세 정보를 추가할 수 있음
    * `charge_id` 컬럼을 사용하여 `we_mart.we_jelly_charge` 테이블과 조인하여 젤리 충전 정보를 추가할 수 있음

### 추가 정보

* 젤리 거래 내역은 `part_date` 컬럼을 기준으로 분할되어 저장됨
* `run_timestamp` 컬럼은 데이터 적재 시간을 나타내며, 데이터 최신성 확인에 활용 가능
* `we_mart.we_jelly_charge` 테이블은 젤리 충전 정보를 기록하는 테이블로, 충전 일시, 결제 방법, 결제 수단 등의 정보를 포함
* `jelly.transaction` 테이블은 젤리 거래 내역을 기록하는 테이블로, 거래 유형, 거래 생성 일시, 거래 ID 등의 정보를 포함
* `we_mart.wv_order` 테이블은 젤리로 상품 구매 시, 주문 정보를 기록하는 테이블로, 주문 ID, 상품 ID, 구매 수량, 지불 젤리 수량 등의 정보를 포함
* `we_mart.we_user` 테이블은 위버스 유저 정보를 기록하는 테이블로, 유저 ID, 국가 정보, 가입 일시 등의 정보를 포함
* `user_ctry` 뷰는 `we_mart.wv_user_ctry_history` 테이블에서 유저별 최신 국가 정보를 추출하여 생성한 뷰로, 유저별 최신 국가 정보를 제공  
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|we_member_id|bigint|위버스 서비스 통합 ID|
|1|wv_user_id|bigint|위버스 유저 ID|
|2|ctry_code|string|거래 당시 접속 IP 기준 국가 정보|
|3|jelly_wallet_type|string|젤리 유무료 여부(PAID: 유료 충전 젤리, FREE: 무료 지급 젤리)|
|4|pay_system|string|결제 수단|
|5|pay_method|string|충전 마켓(스토어/위버스샵 여부)|
|6|product_id|bigint|젤리 상품 ID(product.product.id)|
|7|jelly_id|bigint|젤리 ID(jelly.point.id)|
|8|jelly_store_item_id|string|인앱 결제용 스토어 상품 ID|
|9|jelly_unit_price|double|젤리 내부 단가|
|10|jelly_selling_unit_price|double|젤리 판매 단가|
|11|charge_id|string|젤리 충전 ID(=transaction_id)|
|12|charge_dt|timestamp|충전 일시(UTC)|
|13|transaction_id|string|거래 ID|
|14|trx_type|string|거래 유형|
|15|trx_cre_dt|timestamp|거래 기준 생성일(UTC)|
|16|ldg_cre_dt|timestamp|원장 기준 생성일(UTC)|
|17|first_exp_dt|timestamp|첫 충전 시 만료 일시(UTC)|
|18|upd_exp_dt|timestamp|업데이트 된 만료 일시(환급으로 인한 만료일 변경시, UTC)|
|19|income|int|젤리 충전/환급에 의한 입수량|
|20|outcome|int|젤리 사용/환불/만료에 의한 유출량|
|21|ord_id|string| |
|22|ord_product_id|int| |
|23|ord_store_item_id|string|젤리로 상품 구매 시) 상품의 스토어 상품 ID|
|24|ord_item_qty|int| |
|25|product_sale_price|int| |
|26|paid_jelly_amt|int| |
|27|pur_confirmed_dt|timestamp|젤리로 상품 구매 시) 구매 확정 일시(UTC)|
|28|refund_id_of_withdraw|string|젤리 환불에 의한 젤리 환급시 환불 거래 ID(=transaction_id)|
|29|part_date|string|Partition 용 날짜 Key|
|30|run_timestamp|timestamp|데이터 적재 일시(UTC)|
  
    
---
# HOW TO USE
  
`
### Downstream Table/View
- `we_mart.wv_jelly_transaction` 테이블을 이용하여 젤리 충전 내역을 기반으로 한 `we_mart.stats_wv_d_jelly_balance` 테이블을 생성
    - ```sql
      select *
      from we_mart.wv_jelly_transaction
      where trx_type in ('CHARGE', 'REFUND', 'ORDER', 'WITHDRAW', 'EXPIRE')
      ```
- `we_mart.wv_jelly_transaction` 테이블과 `we_mart.we_jelly` 테이블을 이용하여 젤리 상품별 거래량을 집계하는 `we_mart.stats_wv_d_jelly_product` 테이블을 생성
    - ```sql
      select *
      from we_mart.wv_jelly_transaction as a
      join we_mart.we_jelly as b
      on a.jelly_id = b.jelly_id
      where a.trx_type in ('CHARGE', 'REFUND', 'ORDER', 'WITHDRAW', 'EXPIRE')
      ```
- `we_mart.wv_jelly_transaction` 테이블과 `we_meta.we_country` 테이블을 이용하여 국가별 젤리 거래량을 집계하는 `we_mart.stats_wv_d_jelly_ctry` 테이블을 생성
    - ```sql
      select *
      from we_mart.wv_jelly_transaction as a
      join we_meta.we_country as b
      on a.ctry_code = b.ctry_code
      where a.trx_type in ('CHARGE', 'REFUND', 'ORDER', 'WITHDRAW', 'EXPIRE')
      ```

### Data Extraction
- `we_mart.wv_jelly_transaction` 테이블에서 특정 기간 동안 젤리 충전 내역을 추출
    - ```sql
      select we_member_id, charge_id, trx_cre_dt, jelly_wallet_type, jelly_amt
      from we_mart.wv_jelly_transaction
      where trx_type = 'CHARGE'
      and part_date between '2024-01-01' and '2024-01-31'
      ```
- `we_mart.wv_jelly_transaction` 테이블에서 특정 아티스트의 젤리 사용 내역을 추출
    - ```sql
      select we_member_id, transaction_id, trx_cre_dt, jelly_amt
      from we_mart.wv_jelly_transaction
      where trx_type = 'ORDER'
      and we_art_name = 'ARTIST'
      ```
- `we_mart.wv_jelly_transaction` 테이블에서 특정 국가의 젤리 충전 내역을 추출
    - ```sql
      select we_member_id, charge_id, trx_cre_dt, jelly_amt
      from we_mart.wv_jelly_transaction
      where trx_type = 'CHARGE'
      and ctry_code = 'KR'
      ```
- `we_mart.wv_jelly_transaction` 테이블에서 젤리 환급 내역을 추출
    - ```sql
      select we_member_id, transaction_id, trx_cre_dt, jelly_amt
      from we_mart.wv_jelly_transaction
      where trx_type = 'WITHDRAW'
      ```
- `we_mart.wv_jelly_transaction` 테이블에서 특정 젤리 상품의 거래 내역을 추출
    - ```sql
      select we_member_id, transaction_id, trx_cre_dt, jelly_amt
      from we_mart.wv_jelly_transaction
      where jelly_id = 'JELLY_ID'
      ```
- `we_mart.wv_jelly_transaction` 테이블에서 특정 결제 수단으로 충전된 젤리 내역을 추출
    - ```sql
      select we_member_id, charge_id, trx_cre_dt, jelly_amt
      from we_mart.wv_jelly_transaction
      where pay_system = 'PAY_SYSTEM'
      ```
- `we_mart.wv_jelly_transaction` 테이블에서 특정 기간 동안 유저별 젤리 잔액 변화를 추출
    - ```sql
      select we_member_id, part_date, sum(income) - sum(outcome) as jelly_balance
      from we_mart.wv_jelly_transaction
      where part_date between '2024-01-01' and '2024-01-31'
      group by we_member_id, part_date
      ```
- `we_mart.wv_jelly_transaction` 테이블에서 젤리 충전 및 사용 내역을 추출하여 젤리 잔액을 계산
    - ```sql
      select we_member_id, sum(case when trx_type = 'CHARGE' then jelly_amt else 0 end) as jelly_charged
      , sum(case when trx_type = 'ORDER' then jelly_amt else 0 end) as jelly_used
      , sum(case when trx_type = 'CHARGE' then jelly_amt else 0 end) - sum(case when trx_type = 'ORDER' then jelly_amt else 0 end) as jelly_balance
      from we_mart.wv_jelly_transaction
      group by we_member_id
      ```
- `we_mart.wv_jelly_transaction` 테이블에서 특정 기간 동안 젤리 충전 및 사용 내역을 추출하여 젤리 잔액을 계산 (Spark)
    - ```py
      from pyspark.sql.functions import sum, when, col
      df = spark.read.table('we_mart.wv_jelly_transaction')
      df.groupBy('we_member_id').agg(
          sum(when(col('trx_type') == 'CHARGE', col('jelly_amt')).otherwise(0)).alias('jelly_charged'),
          sum(when(col('trx_type') == 'ORDER', col('jelly_amt')).otherwise(0)).alias('jelly_used'),
          (sum(when(col('trx_type') == 'CHARGE', col('jelly_amt')).otherwise(0)) - sum(when(col('trx_type') == 'ORDER', col('jelly_amt')).otherwise(0))).alias('jelly_balance')
      ).show()
      ```  
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_we_mart_priority_daily`

### Update Interval: DAILY

### Update Type: APPEND

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/mart/we_mart/wv_jelly_transaction.py)

### Airflow: [DAG](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_we_mart_priority_daily.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|jelly.ledger|we_mart.stats_we_w_jelly_order_detail|
|jelly.transaction|we_mart.stats_wv_d_jelly_transaction|
|we_mart.we_jelly_charge|we_mart.stats_wv_d_jelly_transaction_smry|
|we_mart.we_user| |
|we_mart.wv_order| |
|we_mart.wv_user_ctry_history| |

## 🐤 Downstream Tables Info
  
### Downstream Tables
- **`we_mart.stats_wv_d_jelly_transaction`**: 젤리 거래에 대한 일별 통계 데이터를 저장하는 테이블. `wv_jelly_transaction` 테이블을 기반으로 하여 젤리 거래 유형, 젤리 종류, 결제 방식 등 다양한 차원으로 집계하여 일별 젤리 거래량, 거래 건수, 유저 수 등을 계산한다.
    - `wv_jelly_transaction` 테이블의 `trx_type`, `jelly_wallet_type`, `pay_method`, `pay_system`, `jelly_product_type` 컬럼을 사용하여 다양한 조건으로 집계한다.
    - `key_date` 컬럼을 파티션 키로 사용하여 일별 데이터를 분리 관리한다.
    - `jelly_amt`, `jelly_trx_cnt`, `uniq_user_cnt` 등의 컬럼을 사용하여 젤리 거래량, 거래 건수, 유저 수 등의 정보를 추출할 수 있다.
- **`we_mart.stats_wv_d_jelly_transaction_smry`**: 젤리 거래에 대한 일별 통계 데이터를 요약한 테이블. `stats_wv_d_jelly_transaction` 테이블을 기반으로 하여 젤리 충전, 사용, 환불, 환급, 만료 등의 거래 유형별 젤리 총량, 유저 수, 거래 건수 등을 집계한다.
    - `stats_wv_d_jelly_transaction` 테이블의 `trx_type`, `jelly_wallet_type`, `jelly_amt`, `uniq_user_cnt`, `jelly_trx_cnt` 컬럼을 사용하여 집계한다.
    - `key_date` 컬럼을 파티션 키로 사용하여 일별 데이터를 분리 관리한다.
    - `CHARGE`, `REFUND`, `ORDER`, `WITHDRAW`, `EXPIRE`, `BALANCE` 컬럼을 사용하여 각 거래 유형별 젤리 총량, 유저 수, 거래 건수 등의 정보를 추출할 수 있다.
- **`we_mart.stats_we_w_jelly_order_detail`**: 젤리로 상품 구매 시 사용된 젤리량을 월별로 집계한 테이블. `wv_jelly_transaction` 테이블과 `wv_order` 테이블을 조인하여 젤리로 상품 구매 시 사용된 젤리량, 상품 종류, 구매 수량, 환급 젤리량 등의 정보를 추출한다.
    - `wv_jelly_transaction` 테이블의 `trx_type`, `jelly_wallet_type`, `pay_system`, `pay_method`, `jelly_product_type` 컬럼을 사용하여 젤리 사용 정보를 추출한다.
    - `wv_order` 테이블의 `transaction_id`, `product_id`, `product_name`, `ord_item_qty`, `paid_amt` 컬럼을 사용하여 상품 구매 정보를 추출한다.
    - `key_month` 컬럼을 파티션 키로 사용하여 월별 데이터를 분리 관리한다.
    - `order_product_qty`, `order_jelly_amount`, `withdraw_product_qty`, `withdraw_jelly_amount` 컬럼을 사용하여 젤리 사용량, 상품 종류, 구매 수량, 환급 젤리량 등의 정보를 추출할 수 있다.
- **`we_mart.stats_we_m_jelly_jp_balance`**: 일본 지역에서 충전된 젤리의 월별 잔액을 집계한 테이블. `wv_jelly_transaction` 테이블, `we_jelly_charge` 테이블, `currency_rate_etc` 테이블을 조인하여 일본 지역에서 충전된 젤리의 잔액, 충전량, 사용량, 환급량 등을 월별로 집계한다.
    - `wv_jelly_transaction` 테이블의 `trx_type`, `jelly_wallet_type`, `jelly_amt`, `uniq_user_cnt`, `jelly_trx_cnt` 컬럼을 사용하여 젤리 거래 정보를 추출한다.
    - `we_jelly_charge` 테이블의 `charge_id`, `jelly_id`, `jelly_qty`, `currency_code`, `total_paid_amt`, `jelly_unit_price`, `charged_at` 컬럼을 사용하여 일본 지역 젤리 충전 정보를 추출한다.
    - `currency_rate_etc` 테이블의 `rate` 컬럼을 사용하여 젤리 잔액을 JPY 단위로 환산한다.
    - `charge_month` 컬럼을 파티션 키로 사용하여 월별 데이터를 분리 관리한다.
    - `balance_jelly_amt`, `balance_cry_amt`, `balance_jpy_amt` 컬럼을 사용하여 젤리 잔액, 충전량, 사용량, 환급량 등의 정보를 추출할 수 있다.
- **`we_mart.wv_jelly_balance_by_charge`**: 젤리 충전별 일별 잔액 및 상태를 저장하는 테이블. `wv_jelly_transaction` 테이블의 `charge_id` 컬럼을 기반으로 하여 젤리 충전별 잔액, 입출금 내역, 상태 등을 추적한다.
    - `wv_jelly_transaction` 테이블의 `charge_id`, `income`, `outcome`, `jelly_wallet_type`, `pay_method`, `pay_system` 컬럼을 사용하여 젤리 충전 정보를 추출한다.
    - `part_date` 컬럼을 파티션 키로 사용하여 일별 데이터를 분리 관리한다.
    - `balance`, `type` 컬럼을 사용하여 젤리 잔액 및 상태 정보를 추출할 수 있다.

### Downstream View Tables
- **`wi_d_jelly_trx_by_ctry`**: 젤리 거래 유형별 국가별 일별 젤리 거래량을 집계한 뷰 테이블. `wv_jelly_transaction` 테이블을 기반으로 하여 젤리 거래 유형, 국가별로 젤리 거래량을 집계한다.
    - `wv_jelly_transaction` 테이블의 `trx_type`, `ctry_code`, `jelly_amt` 컬럼을 사용하여 집계한다.
    - `key_date` 컬럼을 파티션 키로 사용하여 일별 데이터를 분리 관리한다.
    - `jelly_amt` 컬럼을 사용하여 젤리 거래량을 추출할 수 있다.
- **`wi_d_jelly_trx_by_pay_method`**: 젤리 거래 유형별 결제 수단별 일별 젤리 거래량을 집계한 뷰 테이블. `wv_jelly_transaction` 테이블을 기반으로 하여 젤리 거래 유형, 결제 수단별로 젤리 거래량을 집계한다.
    - `wv_jelly_transaction` 테이블의 `trx_type`, `pay_system`, `pay_method`, `jelly_amt` 컬럼을 사용하여 집계한다.
    - `key_date` 컬럼을 파티션 키로 사용하여 일별 데이터를 분리 관리한다.
    - `jelly_amt` 컬럼을 사용하여 젤리 거래량을 추출할 수 있다.
- **`wi_m_jelly_trx_by_art`**: 젤리 거래 유형별 아티스트별 월별 젤리 거래량을 집계한 뷰 테이블. `wv_jelly_transaction` 테이블을 기반으로 하여 젤리 거래 유형, 아티스트별로 젤리 거래량을 집계한다.
    - `wv_jelly_transaction` 테이블의 `trx_type`, `we_art_id`, `jelly_amt` 컬럼을 사용하여 집계한다.
    - `key_month` 컬럼을 파티션 키로 사용하여 월별 데이터를 분리 관리한다.
    - `jelly_amt` 컬럼을 사용하여 젤리 거래량을 추출할 수 있다.  
---