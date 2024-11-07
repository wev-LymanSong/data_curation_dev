
we_mart.wa_album
================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_mart**|
|**Table Type**|MART PRIMARY|
|**Partitioned by**| |
|**Created/ Last Updated At**|2022-09-22 / 2023-12-08|
|**Created By**|송재영|
|**Last Updated By**|송재영|
|**Collaborators**|송재영[27], 구민서[1]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2022-09-22|송재영|[PR](https://github.com/benxcorp/databricks/commit/02e9ad45af599404e649537e1f8914c1e2254c44)|
|2022-09-22|송재영|[PR](https://github.com/benxcorp/databricks/commit/6c809dde6bd2429b13c7520bac3341833173e882)|
|2022-09-23|송재영|[PR](https://github.com/benxcorp/databricks/commit/dbe506a5a3582841bef4617e62bc6b315fec8e51)|
|2022-10-13|송재영|[PR](https://github.com/benxcorp/databricks/commit/ef74e6897b19246059482b4fbbd0d95d1827ddd1)|
|2022-10-18|송재영|[PR](https://github.com/benxcorp/databricks/commit/cce08f5b98549ccedef7177436532b9dd9e6c974)|
|2022-12-05|송재영|[PR](https://github.com/benxcorp/databricks/commit/f9eea5111b8f4a2f99f7d70d17eeb99a4c335428)|
|2022-12-09|구민서|[PR](https://github.com/benxcorp/databricks/commit/bd183779c84e616084e05627330a1c8b98ca44bd)|
|2022-12-26|송재영|[PR](https://github.com/benxcorp/databricks/commit/3b60f0af39bd86ed6bef9f657f0af88c2fc53b42)|
|2022-12-26|송재영|[PR](https://github.com/benxcorp/databricks/commit/0091c5ca7047b646a9760c25ab991aab39174ff2)|
|2023-01-02|송재영|[PR](https://github.com/benxcorp/databricks/commit/4db494a3de7087ee61192b9cf81da17cde57f8eb)|
|2023-01-18|송재영|[PR](https://github.com/benxcorp/databricks/commit/14a51e34fb3141b2169a8fb3ef59af68d571dc67)|
|2023-01-20|송재영|[PR](https://github.com/benxcorp/databricks/commit/b1a5f010e4b81103de2a0153af5101ce7f07639d)|
|2023-02-24|송재영|[PR](https://github.com/benxcorp/databricks/commit/3bf6674d3517fdd5692ea5132ad9c9fda1898700)|
|2023-03-24|송재영|[PR](https://github.com/benxcorp/databricks/commit/593681943d76bfef4efda0ad08f782f73c20d9a4)|
|2023-04-04|송재영|[PR](https://github.com/benxcorp/databricks/commit/f2e2a9cc5d90f24deb1ee2a509880d2b401c0df3)|
|2023-04-11|송재영|[PR](https://github.com/benxcorp/databricks/commit/8517c75fc00834196c45d66e8cc3dc8e8246bc28)|
|2023-04-20|송재영|[PR](https://github.com/benxcorp/databricks/commit/44a4b1edd6d132fbab6defb6e581ba1ea2a249b8)|
|2023-04-26|송재영|[PR](https://github.com/benxcorp/databricks/commit/874674bc1a3ab394f3f9fe59f77f9e0f50bc9022)|
|2023-04-30|송재영|[PR](https://github.com/benxcorp/databricks/commit/9327ca508dd723524a5f738a7793fe65c8f881cc)|
|2023-04-30|송재영|[PR](https://github.com/benxcorp/databricks/commit/a6bcdd5aeac475eef221152227b5d3d6ec19af54)|
|2023-06-14|송재영|[PR](https://github.com/benxcorp/databricks/commit/92b762fd902d1332e0e9a2ca2f16c1c08de88eef)|
|2023-07-24|송재영|[PR](https://github.com/benxcorp/databricks/commit/ccd503a9ff46ddae0243f8162790b9b19269e09f)|
|2023-09-04|송재영|[PR](https://github.com/benxcorp/databricks/commit/b1b3502ad1c1c2d2a28d52b873b2af4709842bd6)|
|2023-10-20|송재영|[PR](https://github.com/benxcorp/databricks/commit/975913b8a01415390b1e81a94a723e01fe30a770)|
|2023-11-16|송재영|[PR](https://github.com/benxcorp/databricks/commit/5dd9404c2d6a704e2e87a1f1e8b258d8d14f1a32)|
|2023-12-07|송재영|[PR](https://github.com/benxcorp/databricks/commit/d290f42623ec72f10e5f64a14c4abc30d5f59064)|
|2023-12-08|송재영|[PR](https://github.com/benxcorp/databricks/commit/600724bddae06714d5ef1bbd845af02590b67cc9)|
|2023-12-08|송재영|[PR](https://github.com/benxcorp/databricks/commit/dfde24c37d13bbad2613f20c15a7b9905260897b)|
  
  
# TABLE NOTICE
  
### 테이블 개요

* **테이블 목적**: 위버스 앨범 메타 데이터를 통합하여 제공
* **데이터 레벨**: AGGREGATED DATA(STATISTICS) 
* **파티션 키**: 없음
* **주요 키**: `wa_album_id`

### 테이블 특징

* 위버스 앨범(WA) 테이블과 위버스샵 앨범(WS) 테이블의 데이터를 통합하여 위버스 플랫폼 내 앨범 정보를 일관성 있게 제공
* 앨범 이름 유사도를 계산하여 가장 관련성 높은 WS 앨범 정보를 매칭
* 위버스 앨범(WA) 테이블, 위버스샵 앨범(WS) 테이블, 쿠폰 정보를 조인하여 앨범과 관련된 다양한 정보 제공

### 데이터 추출 및 생성 과정

1. **주요 데이터 소스**:
    * `wev_prod.album.album`: 위버스 앨범 테이블
    * `wev_prod.album.artist`: 위버스 아티스트 테이블
    * `we_meta.ws_album`: 위버스샵 앨범 테이블
    * `coupon.tb_cp_used`: 쿠폰 사용 정보 테이블
    * `coupon.tb_cp_plan`: 쿠폰 플랜 정보 테이블
2. **데이터 전처리**:
    * 위버스 앨범 테이블과 아티스트 테이블을 조인하여 앨범 정보에 아티스트 정보 추가
    * 위버스샵 앨범 테이블에서 앨범 ID, 이름, 발매일, 판매 정보 추출
    * 쿠폰 정보를 앨범 ID 기준으로 그룹핑하여 앨범별 쿠폰 플랜 ID 리스트 생성
3. **데이터 통합**:
    * 위버스 앨범 정보와 위버스샵 앨범 정보를 `we_art_id` 기준으로 조인
    * 앨범 이름 유사도를 계산하여 가장 관련성 높은 위버스샵 앨범 정보를 매칭
    * 쿠폰 플랜 정보를 앨범 ID 기준으로 조인하여 앨범별 쿠폰 플랜 정보 추가
4. **최종 테이블 생성**:
    * 위버스 앨범 테이블, 위버스샵 앨범 테이블, 쿠폰 정보를 통합하여 `we_mart.wa_album` 테이블 생성

### 테이블 활용 가이드

* **주요 활용**:
    * 위버스 앨범 정보와 위버스샵 앨범 정보를 통합하여 분석
    * 위버스 앨범과 관련된 쿠폰 정보 확인
    * 앨범 판매 정보 및 쿠폰 정보 분석을 통한 마케팅 전략 수립
* **조인 시 유의사항**:
    * `we_art_id` 컬럼은 위버스 플랫폼 내 통합 아티스트 ID이므로, 다른 테이블과 조인할 때 유의
    * `wa_album_id`는 위버스 앨범 테이블의 고유 ID이고, `album_id`는 위버스샵 앨범 테이블의 고유 ID이므로, 두 ID를 이용하여 두 테이블의 데이터를 연관하여 분석 가능

### 추가 정보

* `wa_album_id`와 `album_id` 컬럼은 각각 위버스 앨범과 위버스샵 앨범의 고유 ID를 나타내며, 두 컬럼을 이용하여 두 테이블의 데이터를 연관하여 분석 가능
* `sale_ids` 컬럼은 앨범과 관련된 모든 판매 정보를 담고 있으며, `weverse_album_sale_ids`와 `physical_album_sale_ids` 컬럼은 각각 위버스 앨범과 실물 앨범 판매 정보를 담고 있음
* `cp_plan_ids` 컬럼은 앨범과 관련된 쿠폰 플랜 ID를 담고 있으며, 해당 ID를 이용하여 `coupon.tb_cp_plan` 테이블에서 쿠폰 플랜 정보를 조회할 수 있음  
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|wa_album_id|int|위버스 앨범 ID|
|1|wa_album_name|string|위버스 앨범 명|
|2|we_art_id|int|위버스 플랫폼 내 통합 아티스트 ID|
|3|we_art_name|string|위버스 플랫폼 내 통합 아티스트 명|
|4|entity_we_art_id|bigint|wecode 에 따른 객체 아티스트 ID(솔로/그룹/유닛 분리)|
|5|wa_artist_id|bigint|위버스 앨범 아티스트 ID|
|6|wa_art_name|string|위버스 앨범 아티스트 명|
|7|wa_release_date|date|위버스 앨범 출시 일|
|8|album_id|int|앨범 ID(WS 앨범 메타)|
|9|album_name|string|앨범 명 (WS 앨범 메타)|
|10|album_release_date|date|앨범 발매일 (WS 앨범 메타)|
|11|cp_plan_ids|array<bigint>|쿠폰 cp_plan_id 리스트|
|12|sale_ids|array<bigint>|앨범과 관련된 모든 sale_id 리스트|
|13|weverse_album_sale_ids|array<bigint>|위버스 앨범 sale_id 리스트|
|14|physical_album_sale_ids|array<bigint>|실물 앨범 sale_id 리스트|
  
    
---
# HOW TO USE
  
`
### Downstream Table/View
- `we_mart.wa_album` 테이블을 사용하여 위버스 앨범 판매 현황을 일별로 집계하는 `stats_wa_d_album_sale` 테이블을 생성
    - ```sql
      select 
        date(a.part_date) as key_date
      , a.wa_album_id
      , a.wa_album_name
      , a.we_art_id
      , a.we_art_name
      , count(distinct b.we_member_id) as uu_pur_cnt
      , sum(b.ord_item_qty) as pur_cnt
      , sum(b.ord_item_amt_krw) as amt_krw
      from we_mart.wa_album as a
      left join we_mart.ws_album_sale as b
      on a.wa_album_id = b.wa_album_id
      group by 1, 2, 3, 4, 5
      order by 1, 2, 3, 4, 5
      ```
- `we_mart.wa_album` 테이블을 사용하여 위버스 앨범 별 구매 유저수를 집계하는 `wi_view.wa_album_user_cnt` 뷰를 생성
    - ```sql
      select 
        a.wa_album_id
      , a.wa_album_name
      , count(distinct b.we_member_id) as uu_pur_cnt
      from we_mart.wa_album as a
      left join we_mart.ws_album_sale as b
      on a.wa_album_id = b.wa_album_id
      group by 1, 2
      order by 1, 2
      ```

### Data Extraction
- `we_mart.wa_album` 테이블에서 아티스트 ID가 "ARTIST"인 앨범들의 정보를 추출
    - ```sql
      select 
        wa_album_id
      , wa_album_name
      , we_art_id
      , we_art_name
      from we_mart.wa_album
      where we_art_id = "ARTIST"
      order by wa_album_id
      ```
- `we_mart.wa_album` 테이블에서 "2024-01-01" 이후에 발매된 앨범들의 정보를 추출
    - ```sql
      select 
        wa_album_id
      , wa_album_name
      , we_art_id
      , we_art_name
      , wa_release_date
      from we_mart.wa_album
      where wa_release_date >= "2024-01-01"
      order by wa_album_id
      ```
- `we_mart.wa_album` 테이블에서 위버스 앨범 ID가 "1234"인 앨범의 쿠폰 정보를 추출
    - ```sql
      select 
        wa_album_id
      , wa_album_name
      , cp_plan_ids
      from we_mart.wa_album
      where wa_album_id = "1234"
      ```
- `we_mart.wa_album` 테이블에서 위버스 앨범 ID가 "1234"인 앨범의 위버스 앨범과 관련된 모든 sale_id 리스트를 추출
    - ```sql
      select 
        wa_album_id
      , weverse_album_sale_ids
      from we_mart.wa_album
      where wa_album_id = "1234"
      ```
- `we_mart.wa_album` 테이블에서 위버스 앨범 ID가 "1234"인 앨범의 실물 앨범과 관련된 sale_id 리스트를 추출
    - ```sql
      select 
        wa_album_id
      , physical_album_sale_ids
      from we_mart.wa_album
      where wa_album_id = "1234"
      ```  
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_ws_mart_daily`

### Update Interval: DAILY

### Update Type: OVERWRITE

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/mart/we_mart/wa_album.py)

### Airflow: [DAG](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_ws_mart_daily.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|album.album|we_mart.stats_wa_d_album_reg_smry|
|album.artist|we_mart.stats_ws_d_album_cumul|
|album.user_album_reg|we_mart.wa_sess_daily|
|coupon.tb_cp_plan|we_mart.wa_user_album_reg|
|coupon.tb_cp_used|we_mart.ws_album_sale|
|we_mart.we_artist| |
|wecode.tb_entity| |
|wecode.tb_entity_meta| |

## 🐤 Downstream Tables Info
  
### Downstream Tables
- **`we_mart.ws_album_sale`**: 위버스샵 앨범 상품 판매 정보를 담고 있는 마트 테이블
    - `we_mart.wa_album` 테이블을 조인하여 위버스 앨범 ID (`wa_album_id`)와 관련 정보를 추가
    - 위버스 앨범 판매 데이터를 추출하려면 이 테이블을 사용하면 됨. 예를 들어, 특정 날짜에 판매된 위버스 앨범 ID와 판매량을 알아보려면 다음 쿼리를 사용하면 됨.

    ```sql
    select wa_album_id, sum(ord_item_qty * album_qty) as total_album_qty
    from we_mart.ws_album_sale
    where part_date = '2023-12-25'
    group by wa_album_id
    ```

- **`we_mart.wa_user_album_reg`**: 위버스 앨범 등록 정보를 담고 있는 마트 테이블
    - `we_mart.wa_album` 테이블을 조인하여 위버스 앨범 ID(`wa_album_id`)와 관련 정보를 추가
    - 특정 날짜에 등록된 위버스 앨범 정보를 알아보려면 이 테이블을 사용하면 됨. 예를 들어, 2023년 12월 25일에 등록된 모든 위버스 앨범 정보를 추출하려면 다음 쿼리를 사용하면 됨.

    ```sql
    select wa_album_id, wa_album_name, we_art_id, we_art_name
    from we_mart.wa_user_album_reg
    where part_date = '2023-12-25'
    ```

### Downstream View Tables
- **`we_mart.view_ws_album_latest`**: `we_meta.ws_album` 테이블의 가장 최근 파티션 날짜 데이터만 보여주는 뷰 테이블
    - `we_mart.wa_album` 테이블을 사용하지 않음.
    - 가장 최근에 업데이트된 위버스샵 앨범 정보를 간편하게 확인할 때 사용
    - 다음 쿼리는 현재 날짜 기준 가장 최근에 업데이트된 앨범 정보를 가져옴.

    ```sql
    select album_id, album_name, we_art_id, we_art_name
    from we_mart.view_ws_album_latest
    ```

- **`we_mart.view_wa_album_latest`**: `we_mart.wa_album` 테이블의 가장 최근 파티션 날짜 데이터만 보여주는 뷰 테이블
    - `we_mart.wa_album` 테이블의 `part_date` 컬럼을 파티션 키로 사용
    - 가장 최근에 업데이트된 위버스 앨범 정보를 간편하게 확인할 때 사용
    - 다음 쿼리는 현재 날짜 기준 가장 최근에 업데이트된 앨범 정보를 가져옴.

    ```sql
    select wa_album_id, wa_album_name, we_art_id, we_art_name
    from we_mart.view_wa_album_latest
    ```

- **`we_mart.view_wa_album_sales_detail`**: `we_mart.wa_album` 테이블을 사용하여 위버스 앨범 ID, 앨범 이름, 아티스트 정보와 함께 해당 앨범의 상세 판매 정보를 제공하는 뷰 테이블
    - `we_mart.wa_album` 테이블을 조인하여 위버스 앨범 ID(`wa_album_id`)와 관련 정보를 추가
    - `we_mart.ws_album_sale` 테이블을 조인하여 앨범 판매 관련 상세 정보를 추가
    - 특정 위버스 앨범의 판매 정보를 자세히 알아보려면 이 뷰 테이블을 사용하면 됨. 예를 들어, 위버스 앨범 ID가 100인 앨범의 판매 정보를 추출하려면 다음 쿼리를 사용하면 됨.

    ```sql
    select 
        WA.wa_album_id, WA.wa_album_name, WA.we_art_id, WA.we_art_name,
        WS.shop, WS.ctry_code, WS.album_qty_type, WS.album_option_type,
        sum(WS.ord_item_qty * WS.album_qty) as total_album_qty,
        sum(WS.ord_item_amt_krw) as total_album_amt_krw
    from we_mart.wa_album as WA
    join we_mart.ws_album_sale as WS
        on WA.wa_album_id = WS.wa_album_id
    where WA.wa_album_id = 100
    group by 1, 2, 3, 4, 5, 6, 7, 8
    ```  
---