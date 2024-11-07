we_mart.we_jelly
================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_mart**|
|**Table Type**|MART VIEW TABLE or NOT SCHEDULED|
|**Partitioned by**| |
|**Created/ Last Updated At**|2023-07-21 / 2024-08-23|
|**Created By**|송재영|
|**Last Updated By**|송재영|
|**Collaborators**|송재영[13], 구민서[1]|

#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2023-07-21|송재영|[PR](https://github.com/benxcorp/databricks/commit/e1bb21f406cb0b32616e7ffdbcda0d93bf3822f8)|
|2023-07-24|송재영|[PR](https://github.com/benxcorp/databricks/commit/4e12aeea7cb73e8eaa55bec4576322b5d3329d6c)|
|2023-07-26|구민서|[PR](https://github.com/benxcorp/databricks/commit/8f2b58abfe9e00a76ca07f5087c4f2f2b3df899c)|
|2023-08-02|송재영|[PR](https://github.com/benxcorp/databricks/commit/f6c10cb4390edff034676e44636daedf14bebfd0)|
|2023-09-13|송재영|[PR](https://github.com/benxcorp/databricks/commit/65097c805fa895e84903c3a0b63e2b8a5fb34628)|
|2023-09-13|송재영|[PR](https://github.com/benxcorp/databricks/commit/934e62ed8a81eb6ab4b58d8c12587c6039168e99)|
|2023-09-14|송재영|[PR](https://github.com/benxcorp/databricks/commit/e6a957105fccfebe849f5c36783720fb122a4457)|
|2023-11-09|송재영|[PR](https://github.com/benxcorp/databricks/commit/f434fce5783268929e509e49ec73e29369dab4df)|
|2023-11-10|송재영|[PR](https://github.com/benxcorp/databricks/commit/d54591e7e97d64593335979837ba2e067e208554)|
|2024-03-06|송재영|[PR](https://github.com/benxcorp/databricks/commit/15cb24bbcb63a707307e11dcf5a62c928097aed4)|
|2024-03-13|송재영|[PR](https://github.com/benxcorp/databricks/commit/d9b1cdafec815ed1df72ace967a34bb79befb87a)|
|2024-07-19|송재영|[PR](https://github.com/benxcorp/databricks/commit/d36302ed1fd501dd63db28291259df886c59868a)|
|2024-08-08|송재영|[PR](https://github.com/benxcorp/databricks/commit/137aad3401e77e90b82628e9c736bc5b9bc5d599)|
|2024-08-23|송재영|[PR](https://github.com/benxcorp/databricks/commit/96f1222308c2ab78e54b349ff842c8b47b1adfc1)|


# TABLE NOTICE

### 테이블 개요

* **테이블 목적**: Weverse 플랫폼에서 사용되는 젤리(포인트) 정보를 담고 있으며, 젤리 종류, 발급 유형, 가격, 사용처 등의 상세 정보를 제공한다.
* **데이터 레벨**: META DATA
* **파티션 키**: 없음
* **주요 키**: `jelly_id`

### 테이블 특징

* 젤리 발급 유형(`jelly_wallet_type`)에 따라 `FREE` 젤리와 `PAID` 젤리로 구분
* 젤리 발급 유형, 결제 방식, 판매 유형, 젤리 종류 등을 나타내는 다양한 컬럼 존재
* 젤리 상품 정보(`jelly_product_name`, `jelly_product_type`)와 가격(`jelly_unit_price`, `jelly_selling_unit_price`) 포함
* 젤리 활성화 기간(`activ_from_dt`, `activ_to_dt`) 정보 제공

### 데이터 추출 및 생성 과정

1. **주요 데이터 소스**:
    * `wev_prod.jelly.point`: 젤리 정보 (ID, 발급 유형, 수량, 상품 ID, 가격 등)
    * `wev_prod.jelly.point_group`: 젤리 그룹 정보 (ID, 채널, 판매 유형, 가격 등)
    * `wev_prod.jelly.promotion_point`: 프로모션 젤리 정보 (ID, 젤리 ID, 판매 유형 등)
    * `wev_prod.jelly.transaction`: 젤리 거래 정보 (발급 유형, 결제 시스템, 젤리 ID 등)
2. **데이터 전처리**:
    * 젤리 발급 유형, 결제 방식, 판매 유형 등에 따라 조건부 논리 연산을 통해 새로운 컬럼 생성
    * 젤리 종류 및 가격 관련 컬럼 생성
    * `activ_from_dt` 및 `activ_to_dt` 컬럼 생성
    * 젤리 활성화 기간(`activ_from_dt`, `activ_to_dt`) 정보 제공
3. **데이터 통합**:
    * 다섯 개의 소스 테이블을 `LEFT JOIN`을 통해 연결
4. **최종 테이블 생성**:
    * `wev_prod.we_mart.we_jelly` 테이블 생성

### 테이블 활용 가이드

* **주요 활용**:
    * Weverse 플랫폼에서 젤리 발급 및 사용 현황 분석
    * 젤리 상품별 판매 현황 분석
    * 젤리 발급 유형, 결제 방식, 판매 유형 등에 따른 사용자 행태 분석
* **조인 시 유의사항**:
    * 젤리 관련 정보를 분석할 때 다른 테이블과 조인하여 사용
    * `jelly_id` 컬럼을 통해 다른 테이블과 조인
    * 젤리 발급 유형, 결제 방식, 판매 유형 등을 기준으로 필터링하여 분석

### 추가 정보

* 젤리 발급 유형(`jelly_wallet_type`)에 따라 `FREE` 젤리와 `PAID` 젤리로 구분
* `FREE` 젤리는 프로모션, 이벤트 등을 통해 발급
* `PAID` 젤리는 유료 결제를 통해 구매
* 젤리 발급 유형, 결제 방식, 판매 유형 등에 따라 다양한 컬럼이 존재
* 젤리 종류 및 가격 관련 컬럼을 통해 젤리 상품 정보 확인
* 젤리 활성화 기간(`activ_from_dt`, `activ_to_dt`) 정보를 통해 젤리 유효 기간 확인
* 젤리 관련 분석 시 젤리 종류, 발급 유형, 결제 방식, 판매 유형 등을 고려하여 분석
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|jelly_id|bigint| |
|1|jelly_group_id|bigint| |
|2|parent_jelly_id|bigint| |
|3|promotion_jelly_id|bigint| |
|4|jelly_provision_type|string| |
|5|jelly_qty|int| |
|6|channel|string| |
|7|jelly_wallet_type|string| |
|8|charge_type|string| |
|9|sale_type|string| |
|10|pay_system|string| |
|11|pay_method|string| |
|12|jelly_store_item_id|string| |
|13|product_id|bigint| |
|14|jelly_product_name|string| |
|15|jelly_product_type|string| |
|16|display|boolean| |
|17|jelly_display_name_ko|string| |
|18|jelly_display_name_en|string| |
|19|jelly_display_name_ja|string| |
|20|jelly_unit_price|double| |
|21|jelly_selling_unit_price|double| |
|22|jelly_display_price_ko|double| |
|23|jelly_display_price_en|double| |
|24|jelly_display_price_ja|double| |
|25|is_active|int| |
|26|is_test_jelly_product|int| |
|27|activ_from_dt|timestamp| |
|28|activ_to_dt|timestamp| |


---
# HOW TO USE

### Downstream Table/View
- `we_mart.we_jelly` 테이블을 사용하여 유저별 젤리 보유 현황을 나타내는 `we_mart.wv_jelly_balance` 테이블 생성
    - ```sql
      select
        we_member_id,
        jelly_id,
        sum(jelly_qty) as jelly_balance,
        sum(case when jelly_wallet_type = 'FREE' then jelly_qty else 0 end) as free_jelly_balance,
        sum(case when jelly_wallet_type = 'PAID' then jelly_qty else 0 end) as paid_jelly_balance
      from we_mart.we_jelly
      group by we_member_id, jelly_id

      -- 파티션 키로 `part_date` 컬럼 활용
      --
      -- create or replace table we_mart.wv_jelly_balance (
      --   we_member_id bigint,
      --   jelly_id bigint,
      --   jelly_balance bigint,
      --   free_jelly_balance bigint,
      --   paid_jelly_balance bigint,
      --   part_date date
      -- )
      -- partitioned by (part_date)
      -- using delta
      -- comment '유저별 젤리 보유 현황'
      ```
- `we_mart.we_jelly` 테이블을 사용하여 젤리 상품별 판매 금액과 판매 수량을 집계하는 `we_mart.stats_we_d_jelly_sale` 테이블 생성
    - ```sql
      select
        date(trx_cre_dt) as key_date,
        jelly_product_name,
        sum(jelly_qty) as total_sold_qty,
        sum(jelly_selling_unit_price * jelly_qty) as total_sales_amt
      from we_mart.we_jelly
      where jelly_wallet_type = 'PAID'
      group by key_date, jelly_product_name

      -- 파티션 키로 `key_date` 컬럼 활용
      --
      -- create or replace table we_mart.stats_we_d_jelly_sale (
      --   key_date date,
      --   jelly_product_name string,
      --   total_sold_qty bigint,
      --   total_sales_amt double
      -- )
      -- partitioned by (key_date)
      -- using delta
      -- comment '젤리 상품별 일별 판매 금액 및 수량 집계'
      ```
- `we_mart.we_jelly` 테이블과 `we_meta.we_country` 테이블을 조인하여 국가별 젤리 충전량을 집계하는 `wi_view.wi_d_jelly_charge_ctry` 뷰 생성
    - ```sql
      select
        date(trx_cre_dt) as key_date,
        c.ctry_name_en as country_name,
        sum(jelly_qty) as total_jelly_charged,
        count(distinct we_member_id) as unique_users
      from we_mart.we_jelly j
      left join we_meta.we_country c
      on j.wv_ctry_code = c.ctry_code
      group by key_date, country_name

      --
      -- create or replace view wi_view.wi_d_jelly_charge_ctry as
      -- select * from (
      --   select * from we_mart.stats_we_d_jelly_charge
      --   where dim_upr_name = 'TOTAL'
      --   and dim_lwr_name = 'TOTAL'
      -- )
      -- order by key_date, country_name
      ```

### Data Extraction
- 특정 기간 동안 판매된 젤리 상품 목록과 판매량을 추출
    - ```sql
      select
        jelly_product_name,
        sum(jelly_qty) as total_sold_qty
      from we_mart.we_jelly
      where jelly_wallet_type = 'PAID'
      and date(trx_cre_dt) between '2024-01-01' and '2024-01-31'
      group by jelly_product_name
      order by total_sold_qty desc
      ```
- 특정 유저가 보유한 젤리 목록과 젤리 수량을 추출
    - ```sql
      select
        jelly_product_name,
        jelly_qty
      from we_mart.we_jelly
      where we_member_id = 'USER_ID'
      order by jelly_qty desc
      ```
- 특정 젤리 상품의 판매 단가와 판매 수량을 추출
    - ```sql
      select
        jelly_selling_unit_price,
        jelly_qty
      from we_mart.we_jelly
      where jelly_product_name = 'JELLY_PRODUCT_NAME'
      and jelly_wallet_type = 'PAID'
      order by trx_cre_dt desc
      ```
- 특정 유저가 특정 젤리 상품을 구매한 내역을 추출
    - ```sql
      select
        trx_cre_dt,
        jelly_qty
      from we_mart.we_jelly
      where we_member_id = 'USER_ID'
      and jelly_product_name = 'JELLY_PRODUCT_NAME'
      and jelly_wallet_type = 'PAID'
      order by trx_cre_dt desc
      ```
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `No DAG`

### Update Interval: N/A

### Update Type: N/A

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/mart/we_mart/we_jelly.py)

### Airflow: [DAG]( )


---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|jelly.point|we_mart.stats_we_d_jelly_charge|
|jelly.point_group|we_mart.stats_we_d_jelly_charge_smry|
|jelly.promotion|we_mart.stats_wv_d_jelly_transaction|
|jelly.promotion_point|we_mart.stats_wv_h_digital_prod_sale|
|jelly.transaction|we_mart.we_jelly_charge|
| |we_mart.wv_jelly_balance_by_charge|

## 🐤 Downstream Tables Info

#
---