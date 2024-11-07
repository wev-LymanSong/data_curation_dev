
we_meta.we_digital_product
==========================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_meta**|
|**Table Type**|META PRIMARY|
|**Partitioned by**|`part_date`|
|**Created/ Last Updated At**|2023-04-04 / 2024-07-25|
|**Created By**|송재영|
|**Last Updated By**|송재영|
|**Collaborators**|송재영[12]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2023-04-04|송재영|[PR](https://github.com/benxcorp/databricks/commit/c26f9e8f36ed567dbc1cacd0abd330a9b00a849b)|
|2023-04-10|송재영|[PR](https://github.com/benxcorp/databricks/commit/6c1ef1c69c8b9661287817cc006663c627436bfd)|
|2023-04-10|송재영|[PR](https://github.com/benxcorp/databricks/commit/1bc7386e3dd0e185d2c8076fe0a6d01aa3bda073)|
|2023-04-17|송재영|[PR](https://github.com/benxcorp/databricks/commit/0fdccb595a110edaba5ed09f99710525382d5a74)|
|2023-05-04|송재영|[PR](https://github.com/benxcorp/databricks/commit/0cf7df3bd1bf7b58c3d6361d00df3ec360826999)|
|2023-05-12|송재영|[PR](https://github.com/benxcorp/databricks/commit/75f953a8379aa388a8a9ebbf3cdb9d6cd495b117)|
|2023-05-23|송재영|[PR](https://github.com/benxcorp/databricks/commit/5dcf09be473a571c85d5a324e88cc63a1f5a4182)|
|2023-11-09|송재영|[PR](https://github.com/benxcorp/databricks/commit/352bb2c1389b350e07eec8912cdb846ed51ccb76)|
|2024-05-29|송재영|[PR](https://github.com/benxcorp/databricks/commit/964c62c089c2238a9ce615e32905b55b6f5627aa)|
|2024-07-23|송재영|[PR](https://github.com/benxcorp/databricks/commit/6d83b7ce51c92f350ecb13622669d5a80dec15bb)|
|2024-07-25|송재영|[PR](https://github.com/benxcorp/databricks/commit/0d109c52d0ac2baf533d0f476591d22dbd3c2d18)|
|2024-07-25|송재영|[PR](https://github.com/benxcorp/databricks/commit/18b70e78cc2bda0a4ae95f905c09a05723bfbc52)|
  
  
# TABLE NOTICE
  
### 테이블 개요

* **테이블 목적**: 위버스 및 외부 플랫폼에서 판매되는 디지털 상품 정보를 담고 있다. 
* **데이터 레벨**: META DATA
* **파티션 키**: `part_date`
* **주요 키**: `data_source`, `product_id`, `product_payment_option_id`

### 테이블 Sources
* 내부 데이터
    * `wev_prod.product.product`: 상품 기본 정보
    * `wev_prod.product.product_payment_option`: 상품 결제 옵션 정보
    * `wev_prod.we_mart.we_artist`: 아티스트 정보
    * `wev_prod.product.promotion`: 상품 프로모션 정보
    * `we_mart.ws_goods_stock`: 위버스샵 상품 재고 정보
    * `we_meta.we_media_product_sale_info`: 미디어 상품 판매 정보
    * `jelly.point`: 젤리 포인트 정보
    * `phoning.product`: 포닝 상품 정보

### 데이터 추출 및 생성 과정

1. **데이터 추출**: 각 소스 테이블에서 `part_date` 파티션에 해당하는 데이터를 추출한다.
2. **데이터 전처리**:
    * `product_type` 컬럼에 따라 `wv_product_prefix` 및 `wv_product_key` 컬럼을 생성한다.
    * `sale_start_dt` 및 `sale_end_dt` 컬럼을 `timestamp` 타입으로 변환한다.
    * `is_on_sale` 컬럼을 생성하여 현재 판매 중인 상품 여부를 나타낸다.
    * `product_source` 컬럼을 생성하여 상품의 출처를 나타낸다.
    * `pay_system` 컬럼을 생성하여 결제 시스템을 나타낸다.
    * `dur_type`, `dur_unit`, `dur_value` 컬럼을 생성하여 상품 유형에 따른 적용 정보를 나타낸다.
    * `pay_method` 컬럼을 생성하여 결제 방식에 따른 결제 옵션/마켓을 나타낸다.
    * `sale_price` 컬럼을 생성하여 판매 가격을 나타낸다.
    * `currency_code` 컬럼을 생성하여 통화 코드를 나타낸다.
    * `promotion_type`, `promotion_unit`, `promotion_value` 컬럼을 생성하여 프로모션 정보를 나타낸다.
3. **데이터 통합**: 각 소스 테이블에서 추출한 데이터를 `LEFT JOIN`으로 결합한다.
4. **최종 테이블 생성**: 
    * 위버스 상품 정보와 쿠폰 정보를 `UNION ALL`로 결합하여 최종 테이블을 생성한다.
    * `run_timestamp` 컬럼을 추가하여 데이터 입력 일시를 기록한다.

### 테이블 활용 가이드

* **주요 타겟 분야**:
    * 상품 분석: 상품별 판매 정보, 결제 정보, 프로모션 정보 등을 분석할 수 있다.
    * 사용자 분석: 상품 구매 정보를 기반으로 사용자 행태를 분석할 수 있다.
    * 매출 분석: 상품별 매출 정보, 결제 정보 등을 분석할 수 있다.
* **조인 시 유의사항**:
    * `product_id` 컬럼은 `data_source` 컬럼에 따라 중복될 수 있으므로, 조인 시 `data_source` 컬럼을 함께 고려해야 한다.
    * `product_payment_option_id` 컬럼은 `product_id` 컬럼과 함께 사용하여 상품별 결제 옵션 정보를 식별해야 한다.

### 추가 정보

* `data_source` 컬럼은 상품 정보의 출처를 나타내며, `PRODUCT`는 위버스 상품, `COUPON`은 쿠폰을 의미한다.
* `wv_product_prefix` 컬럼은 위버스 내 상품 종류를 나타내며, `p`는 TVOD/SVOD 상품, `d`는 WDM 상품, `c`는 쿠폰을 의미한다.
* `product_type` 컬럼은 상품 종류를 나타내며, `TVOD`, `SVOD`, `WDM`, `GENERAL_CHARGING_JELLY`, `FAN_LETTER`, `MEMBERSHIP_PHONING` 등이 있다.
* `is_media_product` 컬럼은 미디어 상품 여부를 나타내며, 미디어 상품인 경우 1, 그렇지 않은 경우 0을 나타낸다.
* `sale_status` 컬럼은 상품 판매 상태를 나타내며, `SALE`은 판매 중, `STOP`은 판매 중지 상태를 의미한다.
* `is_on_sale` 컬럼은 현재 판매 중인 상품 여부를 나타내며, 판매 중인 경우 1, 그렇지 않은 경우 0을 나타낸다.
* `product_source` 컬럼은 상품의 출처를 나타내며, `WS`는 위버스샵, `PH`는 포닝, `WV`는 위버스를 의미한다.
* `pay_system` 컬럼은 결제 시스템을 나타내며, `PG`는 PG 결제, `JELLY`는 젤리 포인트 결제, `INAPP`는 인앱 결제, `SUBSCRIPTION`은 구독 결제를 의미한다.
* `dur_type` 컬럼은 상품 유형에 따른 적용 유형을 나타내며, `INFINITE`는 무제한, `PERIOD`는 기간제를 의미한다.
* `pay_method` 컬럼은 결제 방식에 따른 결제 옵션/마켓을 나타내며, `WEVERSE_SHOP`, `JELLY`, `APPLE_APPSTORE`, `GOOGLE_PLAYSTORE` 등이 있다.
* `sale_price` 컬럼은 판매 가격을 나타낸다.
* `currency_code` 컬럼은 통화 코드를 나타내며, `KRW`는 한국 원화, `JELLY`는 젤리 포인트를 의미한다.
* `promotion_type` 컬럼은 프로모션 유형을 나타낸다.
* `promotion_unit` 컬럼은 프로모션 유형에 따른 단위를 나타낸다.
* `promotion_value` 컬럼은 프로모션 단위에 따른 값을 나타낸다.  
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|data_source|string|데이터를 가져온 소스|
|1|product_id|int|상품 ID, data_source에 따라 unique|
|2|wv_product_prefix|string|위버스내 상품들의 종류 별 prefix|
|3|wv_product_key|string|위버스내 관리 상품 Key|
|4|product_name|string|디지털 상품 명|
|5|ws_goods_name|string|위버스 샵에서 사용중인 상품 명|
|6|product_type|string|디지털 상품 종류|
|7|is_media_product|int|미디어 상품일 경우|
|8|we_art_id|int|위버스 플랫폼 통합 아티스트/커뮤니티 ID|
|9|we_art_name|string|위버스 플랫폼 통합 아티스트/커뮤니티 명|
|10|artist_wid|string|위버스에서 사용중인 아티스트 ID|
|11|sale_status|string|판매 상태|
|12|sale_start_dt|timestamp|판매 시작일|
|13|sale_end_dt|timestamp|판매 종료일|
|14|is_on_sale|int|현재 판매 중인지 여부|
|15|product_payment_option_id|int|상품 지불 옵션 ID|
|16|paid_item_id|int|인앱 결제 가능한 상품 ID|
|17|store_item_id|string|인앱 결제용 마켓별 사용 상품 ID|
|18|product_source|string|결제 플랫폼|
|19|pay_system|string|결제 방식/옵션|
|20|dur_type|string|구매상품 적용 단위 및 유형|
|21|dur_unit|string|구매상품 유형에 따른 단위|
|22|dur_value|int|구매상품 단위에 따른 적용 값|
|23|sale_id|bigint|weverseshop sale_id|
|24|shop|string|weverseshop shop|
|25|pay_method|string|인앱 결제 마켓|
|26|sale_price|double|판매가격|
|27|currency_code|string|통화 코드|
|28|wbs_code|string|WBS 코드|
|29|promotion_type|string|이벤트 타입|
|30|promotion_unit|string|이벤트 타입에 따른 단위|
|31|promotion_value|int|이벤트 타입에 따른 값|
|32|part_date|string|파티션 일자|
|33|run_timestamp|timestamp|데이터 입력 일시|
  
    
---
# HOW TO USE
  
### Downstream Table/View
- `we_meta.we_digital_product` 테이블을 사용하여 위버스 상품의 유무료 여부를 나타내는 새로운 컬럼을 추가.
    - ```sql
    select *, 
    case when product_type in ('TVOD', 'SVOD', 'WDM') then 1 else 0 end as is_paid_product
    from we_meta.we_digital_product
    ```
- `we_meta.we_digital_product` 테이블을 사용하여 `we_mart.we_sale` 테이블에 상품 판매 종료일 정보 추가
    - ```sql
    select *,
    case when product_source = 'WS' then 
        case when product_type = 'COUPON' then WGS.sale_end_dt else WDP.sale_end_dt end
    else WDP.sale_end_dt end as sale_end_dt
    from we_mart.we_sale as SALE
    left join we_meta.we_digital_product as WDP
    on SALE.sale_id = WDP.product_id and SALE.part_date = WDP.part_date
    left join we_mart.ws_goods_stock as WGS
    on SALE.sale_id = WGS.sale_id and SALE.part_date = WGS.part_date
    ```
- `we_meta.we_digital_product` 테이블을 사용하여 `we_mart.stats_we_d_digital_prod_sale` 테이블에 상품별 판매 가격 정보 추가
    - ```sql
    select 
    key_date, product_source, pay_system, pay_method, ctry_code, product_type, we_art_id, we_art_name, product_id
    , product_name, product_option_name, ord_qty, ord_amt, ord_amt_jelly, cx_qty, cx_amt, cx_amt_jelly
    , net_qty, net_amt, net_amt_jelly, WDP.sale_price as sale_price, WDP.currency_code as sale_currency_code
    , part_date, run_timestamp
    from we_mart.stats_we_d_digital_prod_sale as STATS
    left join we_meta.we_digital_product as WDP
    on STATS.product_id = WDP.product_id and STATS.part_date = WDP.part_date
    ```

### Data Extraction
- `we_meta.we_digital_product` 테이블에서 ARTIST의 상품 정보를 추출합니다.
    - ```sql
    select `product_name`, `sale_start_dt`, `sale_end_dt`, `sale_price`
    from we_meta.we_digital_product
    where `we_art_name` = 'ARTIST'
    ```
- `we_meta.we_digital_product` 테이블에서 "2024-01-01" 기준으로 판매 중인 상품 정보를 추출합니다.
    - ```sql
    select `product_name`, `sale_start_dt`, `sale_end_dt`, `sale_price`
    from we_meta.we_digital_product
    where `part_date` = '2024-01-01' and `is_on_sale` = 1
    ```
- `we_meta.we_digital_product` 테이블에서 "2024-01-01" 기준으로 판매 중인 "TVOD" 상품 정보를 추출합니다.
    - ```sql
    select `product_name`, `sale_start_dt`, `sale_end_dt`, `sale_price`
    from we_meta.we_digital_product
    where `part_date` = '2024-01-01' and `product_type` = 'TVOD' and `is_on_sale` = 1
    ```  
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_we_mart_priority_daily`

### Update Interval: DAILY

### Update Type: APPEND

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/meta/we_meta/we_digital_product.py)

### Airflow: [DAG](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_we_mart_priority_daily.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|jelly.point|we_mart.stats_wv_d_ops_sales|
|jelly.point_group|we_mart.stats_wv_h_digital_prod_sale|
|phoning.product|we_mart.we_jelly_charge|
|product.product|we_mart.we_order|
|product.product_payment_option|we_mart.we_sale|
|product.promotion|we_mart.wv_order|
|we_mart.we_artist|we_meta.we_digital_product_latest|
|we_mart.ws_goods_stock|we_meta.we_media_product|
|we_meta.we_media_product_sale_info| |

## 🐤 Downstream Tables Info
  
### Downstream Tables
- **we_meta.we_media_product** : 위버스 앱내 구매&쿠폰 등록&재생 가능한 유료 컨텐츠 결제 관련 메타 정보
    - `we_meta.we_digital_product` 테이블에서 `is_media_product` 컬럼이 1인 상품만 추출하고, `we_meta.we_media_product_sale_info` 테이블과 `product_id` 컬럼을 기준으로 조인하여 `sale_id`에 따른 `currency_code`와 `sale_price` 정보를 추가
    - 위버스 앱내 유료 컨텐츠 정보를 추출하기 위해 사용
        - `product_type` 컬럼으로 상품 종류 필터링 가능
        - `pay_method` 컬럼으로 결제 방식 필터링 가능
        - `sale_start_dt`, `sale_end_dt` 컬럼으로 판매 기간 필터링 가능
- **we_mart.stats_wv_h_digital_prod_sale** : 시간대별 WEVERSE 디지털 상품에 대한 주문/취소 스탯 마트
    - `we_meta.we_digital_product` 테이블에서 `is_media_product` 컬럼이 1인 상품만 추출하고, `billing.purchase`, `jelly.point`, `jelly.transaction`, `jelly.order_item`, `jelly.ledger` 테이블과 조인하여 각 시간대별 디지털 상품 주문/취소 정보를 집계
    - 시간대별 위버스 디지털 상품 주문/취소 현황을 분석하기 위해 사용
        - `product_type` 컬럼으로 상품 종류 필터링 가능
        - `pay_system` 컬럼으로 결제 시스템 필터링 가능
        - `pay_method` 컬럼으로 결제처/마켓/결제 수단 필터링 가능
        - `ctry_code` 컬럼으로 국가 필터링 가능
- **we_mart.we_jelly_charge** : 위버스 상품 결제 수단인 젤리 충전 내역
    - `jelly.transaction` 테이블에서 `transaction_type` 컬럼이 'CHARGE'인 데이터만 추출하고, `we_mart.we_jelly` 테이블과 `point_id` 컬럼을 기준으로 조인하여 젤리 충전 정보를 가져옴
    - 위버스 젤리 충전 내역을 추출하고 분석하기 위해 사용
        - `jelly_id` 컬럼으로 젤리 ID 필터링 가능
        - `jelly_wallet_type` 컬럼으로 젤리 유무료 여부 필터링 가능
        - `pay_system` 컬럼으로 결제 방식 필터링 가능
        - `pay_method` 컬럼으로 결제 수단 필터링 가능
- **we_mart.stats_wv_d_ops_sales** : 플랫폼 서비스실 일간 매출 집계 
    - `we_meta.we_digital_product`, `we_meta.we_concert`, `we_mart.ws_goods_stock`, `we_meta.ws_album`, `we_mart.we_sale`, `we_mart.ws_l2s_order` 테이블과 조인하여 각 서비스별 일간 매출 정보를 집계
    - 위버스 플랫폼 서비스실에서 일간 매출을 분석하기 위해 사용
        - `product_type` 컬럼으로 상품 종류 필터링 가능
        - `product_source` 컬럼으로 판매처 필터링 가능
        - `shop` 컬럼으로 샵명 필터링 가능
- **we_mart.we_order** : 일간 통합 구매 현황
    - `we_mart.ws_order`, `we_mart.wv_order`, `we_mart.ph_subscr` 테이블과 조인하여 각 서비스별 주문/취소 정보를 통합
    - 위버스 플랫폼 전체 일간 주문/취소 현황을 분석하기 위해 사용
        - `product_source` 컬럼으로 서비스 구분 필터링 가능
        - `product_type` 컬럼으로 상품 종류 필터링 가능
        - `pay_system` 컬럼으로 결제 시스템 필터링 가능
        - `pay_method` 컬럼으로 결제 수단 필터링 가능

### Downstream View Tables
- **[없음]** :  
---