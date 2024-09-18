
we_mart.wa_album
================

# BASIC INFO

|**About**| |
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

* **테이블 목적**: 위버스 앨범과 위버스샵 앨범 메타 정보를 통합하여 매칭된 앨범 정보를 제공
* **데이터 레벨**: 앨범 단위 (aggregated)
* **파티션 키**: 없음
* **주요 키**: `wa_album_id`

### 테이블 특징
* 위버스 앨범 (`wev_prod.album.album`)과 위버스샵 앨범 (`we_meta.ws_album`) 정보를 `we_art_id` 기준으로 조인하여 통합
* 앨범 이름 유사도를 계산하여 가장 유사한 앨범을 매칭
* 위버스 앨범, 위버스샵 앨범, 쿠폰 정보를 하나의 테이블에 통합

### 데이터 추출 및 생성 과정

1.  **주요 데이터 소스**:
    *   `wev_prod.album.album`: 위버스 앨범 정보
    *   `wev_prod.album.artist`: 위버스 앨범 아티스트 정보
    *   `we_meta.ws_album`: 위버스샵 앨범 정보
    *   `coupon.tb_cp_used`: 쿠폰 사용 정보
    *   `coupon.tb_cp_plan`: 쿠폰 플랜 정보
2.  **데이터 전처리**:
    *   `wev_prod.wecode.tb_entity`: 위버스 플랫폼 내 아티스트 ID 및 이름 정보를 활용하여 `we_art_id` 및 `we_art_name` 컬럼 생성
    *   `we_meta.ws_album`: `album_id` 및 `album_name` 컬럼 생성
    *   `coupon.tb_cp_plan`: 쿠폰 플랜 ID를 `cp_plan_ids` 컬럼으로 변환
3.  **데이터 통합**:
    *   `wev_prod.album.album` 테이블과 `wev_prod.album.artist` 테이블을 `artist_id` 기준으로 조인
    *   위에서 생성된 테이블과 `wev_prod.wecode.tb_entity` 테이블을 `wecode` 기준으로 조인
    *   위버스 앨범 정보와 `we_meta.ws_album` 테이블을 `we_art_id` 기준으로 조인
    *   쿠폰 정보를 `album_id` 기준으로 조인
4.  **최종 테이블 생성**:
    *   앨범 이름 유사도를 계산하여 가장 유사한 앨범을 매칭
    *   최종적으로 `wa_album_id`, `wa_album_name`, `we_art_id`, `we_art_name`, `album_id`, `album_name`, `album_release_date`, `cp_plan_ids`, `sale_ids`, `weverse_album_sale_ids`, `physical_album_sale_ids` 컬럼을 포함하는 `we_mart.wa_album` 테이블 생성

### 테이블 활용 가이드

* **주요 활용**:
    *   위버스 앨범과 위버스샵 앨범 간 매칭 정보 확인
    *   앨범과 관련된 쿠폰 정보 확인
    *   앨범 판매 정보 분석
* **조인 시 유의사항**:
    *   `we_art_id` 컬럼을 이용하여 다른 테이블과 조인할 경우, 위버스 플랫폼 내 아티스트 정보가 일관성을 유지하도록 주의
    *   `sale_ids`, `weverse_album_sale_ids`, `physical_album_sale_ids` 컬럼을 활용하여 판매 정보를 분석할 경우, 해당 컬럼에 포함된 `sale_id` 값의 의미를 정확히 파악해야 함

### 추가 정보

*   `wa_album_id`는 위버스 앨범의 고유 ID
*   `we_art_id`는 위버스 플랫폼 내 통합 아티스트 ID
*   `weverse_album_sale_ids`는 위버스 앨범 판매와 관련된 `sale_id` 목록
*   `physical_album_sale_ids`는 실물 앨범 판매와 관련된 `sale_id` 목록
*   `sale_ids`는 `weverse_album_sale_ids`과 `physical_album_sale_ids`을 모두 포함 하는 `sale_id` 목록  
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
  
### Downstream Table/View
- 위버스 앨범 정보와 Weverse Shop 앨범 정보를 조인하여 통합 앨범 테이블 생성
    - ```sql
      select
        wa.wa_album_id
      , wa.wa_album_name
      , wa.we_art_id
      , wa.we_art_name
      , wa.entity_we_art_id
      , wa.wa_artist_id
      , wa.wa_art_name
      , wa.wa_release_date
      , ws.album_id
      , ws.album_name
      , ws.album_release_date
      , wa.cp_plan_ids
      , ws.sale_ids
      , ws.weverse_album_sale_ids
      , ws.physical_album_sale_ids
      from we_mart.wa_album as wa
      left join we_meta.ws_album_latest as ws
      on wa.album_id = ws.album_id
      -- wa_album_id 를 파티션 키로 활용
      PARTITIONED BY (wa_album_id)
      -- wa_album_id, we_art_id 를 클러스터링 키로 활용
      CLUSTERED BY (wa_album_id, we_art_id)
      ```
- 위버스 앨범 정보와 Weverse Shop 앨범 정보를 조인하여 통합 앨범 테이블 생성 (pyspark)
    - ```py
      from pyspark.sql.functions import col
      from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, ArrayType, LongType

      # wa_album 테이블 스키마 정의
      wa_album_schema = StructType([
        StructField("wa_album_id", IntegerType(), True),
        StructField("wa_album_name", StringType(), True),
        StructField("we_art_id", IntegerType(), True),
        StructField("we_art_name", StringType(), True),
        StructField("entity_we_art_id", LongType(), True),
        StructField("wa_artist_id", LongType(), True),
        StructField("wa_art_name", StringType(), True),
        StructField("wa_release_date", DateType(), True),
        StructField("album_id", IntegerType(), True),
        StructField("album_name", StringType(), True),
        StructField("album_release_date", DateType(), True),
        StructField("cp_plan_ids", ArrayType(LongType()), True),
        StructField("sale_ids", ArrayType(LongType()), True),
        StructField("weverse_album_sale_ids", ArrayType(LongType()), True),
        StructField("physical_album_sale_ids", ArrayType(LongType()), True)
      ])

      # wa_album 테이블 로딩
      wa_album_df = spark.read.schema(wa_album_schema).table("we_mart.wa_album")

      # ws_album_latest 테이블 스키마 정의
      ws_album_latest_schema = StructType([
        StructField("album_id", IntegerType(), True),
        StructField("album_name", StringType(), True),
        StructField("album_release_date", StringType(), True),
        StructField("we_art_id", IntegerType(), True),
        StructField("we_art_name", StringType(), True)
      ])

      # ws_album_latest 테이블 로딩
      ws_album_latest_df = spark.read.schema(ws_album_latest_schema).table("we_meta.ws_album_latest")

      # 두 테이블 조인
      merged_df = wa_album_df.join(ws_album_latest_df, on=["album_id"], how="left")

      # 파티셔닝 및 클러스터링
      merged_df.write.mode("overwrite").partitionBy("wa_album_id").bucketBy(2, ["wa_album_id", "we_art_id"]).saveAsTable("we_mart.wa_album_merged")
      ```
- 위버스 앨범 정보를 기반으로 아티스트별 앨범 목록 생성
    - ```sql
      select
        wa.we_art_id
      , wa.we_art_name
      , wa.wa_album_id
      , wa.wa_album_name
      , wa.wa_release_date
      from we_mart.wa_album as wa
      group by 1, 2, 3, 4, 5
      order by 1, 5
      ```

### Data Extraction
- ARTIST 아티스트의 앨범 목록을 추출
    - ```sql
      select
        wa.wa_album_id
      , wa.wa_album_name
      , wa.we_art_id
      , wa.we_art_name
      , wa.wa_release_date
      from we_mart.wa_album as wa
      where wa.we_art_name = 'ARTIST'
      order by wa.wa_release_date desc
      ```
- 2024-01-01 이후 출시된 앨범 목록을 추출
    - ```sql
      select
        wa.wa_album_id
      , wa.wa_album_name
      , wa.we_art_id
      , wa.we_art_name
      , wa.wa_release_date
      from we_mart.wa_album as wa
      where wa.wa_release_date >= '2024-01-01'
      order by wa.wa_release_date desc
      ```
- ARTIST 아티스트의 앨범 중 Weverse Shop 앨범 판매 ID가 포함된 앨범 목록을 추출
    - ```sql
      select
        wa.wa_album_id
      , wa.wa_album_name
      , wa.we_art_id
      , wa.we_art_name
      , wa.wa_release_date
      from we_mart.wa_album as wa
      where wa.we_art_name = 'ARTIST'
      and wa.weverse_album_sale_ids is not null
      order by wa.wa_release_date desc
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
  
- fd a
  - fd
---