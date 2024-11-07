we_mart.wa_album
================

# BASIC INFO

|**About**| 위버스 앨범 메타 정보 |
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

* **테이블 목적**: 위버스 앨범 메타 정보를 담고 있는 테이블
* **데이터 레벨**: META DATA
* **파티션 키**: 없음
* **주요 키**: `wa_album_id`

### 테이블 특징

* 위버스 앨범, 위버스샵 앨범, 쿠폰 정보를 통합하여 각 앨범에 대한 상세 메타 정보를 제공
* 위버스 앨범 정보 (`album.album`)와 위버스샵 앨범 정보 (`we_meta.ws_album`)를 `we_art_id`를 기준으로 조인하여 통합
* `wecode` 테이블을 사용하여 위버스 플랫폼 내 통합 아티스트 정보를 제공
* 위버스 앨범 정보와 위버스샵 앨범 정보 간 유사도 분석을 통해 가장 관련성 높은 앨범 매칭
    * `album_name` 컬럼을 사용하여 앨범 이름 유사도 분석
* `cp_plan_ids` 컬럼을 사용하여 앨범과 관련된 쿠폰 정보 제공
* `sale_ids`, `weverse_album_sale_ids`, `physical_album_sale_ids` 컬럼을 사용하여 앨범과 관련된 판매 정보 제공

### 데이터 추출 및 생성 과정

1. **주요 데이터 소스**:
    * `album.album` : 위버스 앨범 정보
    * `album.artist` : 위버스 앨범 아티스트 정보
    * `we_meta.ws_album` : 위버스샵 앨범 정보
    * `wecode.tb_entity` : 위버스 플랫폼 내 통합 아티스트 정보
    * `coupon.tb_cp_used` : 쿠폰 사용 정보
    * `coupon.tb_cp_plan` : 쿠폰 플랜 정보
2. **데이터 전처리**:
    * `album.album` 테이블에서 `album_id`, `title`, `artist_id`, `release_date` 컬럼을 추출
    * `album.artist` 테이블에서 `artist_id`, `artist_name` 컬럼을 추출
    * `we_meta.ws_album` 테이블에서 `we_art_id`, `we_art_name`, `album_id`, `album_name`, `album_release_date`, `sale_ids`, `weverse_album_sale_ids`, `physical_album_sale_ids` 컬럼을 추출
    * `wecode.tb_entity` 테이블에서 `entity_id`, `entity_code`, `we_art_id` 컬럼을 추출
    * `coupon.tb_cp_used` 테이블에서 `cp_cd`, `cp_plan_id` 컬럼을 추출
    * `coupon.tb_cp_plan` 테이블에서 `id` 컬럼을 추출
3. **데이터 통합**:
    * `album.album` 테이블과 `album.artist` 테이블을 `artist_id` 컬럼으로 조인
    * 위에서 생성된 테이블과 `wecode.tb_entity` 테이블을 `wecode` 컬럼으로 조인
    * `we_meta.ws_album` 테이블과 `wecode.tb_entity` 테이블을 `we_art_id` 컬럼으로 조인
        * Artist 명을 기준으로 같은 아티스트의 앨범들 중 위버스 앨범명과 가장 유사도가 높은 앨범을 매핑
    * 위에서 생성된 테이블과 `coupon.tb_cp_used` 테이블을 `coupon_num` 컬럼으로 조인
    * 위에서 생성된 테이블과 `coupon.tb_cp_plan` 테이블을 `cp_plan_id` 컬럼으로 조인
4. **최종 테이블 생성**:
    * 위에서 생성된 테이블에서 필요한 컬럼을 선택하여 `wa_album` 테이블 생성
    * 위버스 앨범 정보와 위버스샵 앨범 정보 간 유사도 분석 수행
    * 유사도 분석 결과를 사용하여 가장 관련성 높은 앨범 매칭

### 테이블 활용 가이드

* **주요 활용**:
    * 위버스 앨범 및 위버스샵 앨범에 대한 상세 메타 정보 제공
    * 앨범 판매 현황 분석
    * 앨범과 관련된 쿠폰 정보 확인
    * 앨범 유사도 분석 결과 활용
* **조인 시 유의사항**:
    * `wa_album_id` 컬럼을 사용하여 다른 테이블과 조인할 수 있음
    * `we_art_id` 컬럼을 사용하여 아티스트 정보가 담긴 다른 테이블과 조인할 수 있음
    * `sale_ids` 컬럼을 사용하여 판매 정보가 담긴 다른 테이블과 조인할 수 있음
    * `cp_plan_ids` 컬럼을 사용하여 쿠폰 정보가 담긴 다른 테이블과 조인할 수 있음

### 추가 정보

* `sale_ids` 컬럼은 위버스 앨범과 위버스샵 앨범에 대한 모든 판매 정보를 포함
* `weverse_album_sale_ids` 컬럼은 위버스 앨범에 대한 판매 정보만 포함
* `physical_album_sale_ids` 컬럼은 실물 앨범에 대한 판매 정보만 포함
* `cp_plan_ids` 컬럼은 앨범과 관련된 모든 쿠폰 플랜 정보를 포함
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
- 위버스 앨범 정보와 위버스샵 앨범 정보를 결합하여 위버스 앨범 판매 통계를 위한 테이블 생성
    - ```sql
      select
      WA_ALBUM.wa_album_id, WA_ALBUM.wa_album_name, WA_ALBUM.we_art_id, WA_ALBUM.we_art_name, WA_ALBUM.entity_we_art_id, WA_ALBUM.wa_artist_id, WA_ALBUM.wa_art_name, WA_ALBUM.wa_release_date, WA_ALBUM.album_id, WA_ALBUM.album_name, WA_ALBUM.album_release_date, WA_ALBUM.cp_plan_ids, WA_ALBUM.sale_ids, WA_ALBUM.weverse_album_sale_ids, WA_ALBUM.physical_album_sale_ids, WS_ALBUM.album_release_date, WS_ALBUM.we_art_name, WS_ALBUM.sale_ids, WS_ALBUM.weverse_album_sale_ids, WS_ALBUM.physical_album_sale_ids
      from we_mart.wa_album AS WA_ALBUM
      left join we_meta.ws_album AS WS_ALBUM
      on WA_ALBUM.album_id = WS_ALBUM.album_id
      ```

### Data Extraction
- 특정 아티스트의 위버스 앨범 정보 추출
    - ```sql
      select
      wa_album_id, wa_album_name, we_art_id, we_art_name, entity_we_art_id, wa_artist_id, wa_art_name, wa_release_date, album_id, album_name, album_release_date, cp_plan_ids, sale_ids, weverse_album_sale_ids, physical_album_sale_ids
      from we_mart.wa_album
      where we_art_id = "ARTIST"
      ```
- 특정 날짜에 발매된 앨범 정보 추출 (위버스 앨범 정보만)
    - ```sql
      select
      wa_album_id, wa_album_name, we_art_id, we_art_name, entity_we_art_id, wa_artist_id, wa_art_name, wa_release_date, album_id, album_name, album_release_date, cp_plan_ids, sale_ids, weverse_album_sale_ids, physical_album_sale_ids
      from we_mart.wa_album
      where wa_release_date = "2024-01-01"
      and album_id is not null
      ```
- 위버스 앨범 정보와 위버스샵 앨범 정보를 결합하여 위버스 앨범 판매 통계를 위한 데이터 추출
    - ```sql
      select
      WA_ALBUM.wa_album_id, WA_ALBUM.wa_album_name, WA_ALBUM.we_art_id, WA_ALBUM.we_art_name, WA_ALBUM.entity_we_art_id, WA_ALBUM.wa_artist_id, WA_ALBUM.wa_art_name, WA_ALBUM.wa_release_date, WA_ALBUM.album_id, WA_ALBUM.album_name, WA_ALBUM.album_release_date, WA_ALBUM.cp_plan_ids, WA_ALBUM.sale_ids, WA_ALBUM.weverse_album_sale_ids, WA_ALBUM.physical_album_sale_ids, WS_ALBUM.album_release_date, WS_ALBUM.we_art_name, WS_ALBUM.sale_ids, WS_ALBUM.weverse_album_sale_ids, WS_ALBUM.physical_album_sale_ids
      from we_mart.wa_album AS WA_ALBUM
      left join we_meta.ws_album AS WS_ALBUM
      on WA_ALBUM.album_id = WS_ALBUM.album_id
      where WA_ALBUM.we_art_id = "ARTIST"
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
- **`we_mart.ws_album_sale`**: 위버스샵 앨범 판매 정보를 담는 테이블
    - `we_mart.wa_album` 테이블과 `album_id` 컬럼을 기준으로 조인하여 위버스 앨범 ID (`wa_album_id`)를 매핑한다.
    - 위버스샵에서 앨범 판매 정보를 추출하고자 할 때, `we_art_id`, `album_id`, `album_name`, `wa_album_id` 등의 컬럼을 사용하여 필터링 및 집계를 수행할 수 있다.

- **`we_mart.wa_user_album_reg`**: 위버스 앨범 등록 정보를 담는 테이블
    - `we_mart.wa_album` 테이블과 `wa_album_id` 컬럼을 기준으로 조인하여 위버스 앨범 정보를 매핑한다.
    - 위버스 앨범 등록 정보를 추출하고자 할 때, `we_art_id`, `wa_album_id`, `wa_album_name` 등의 컬럼을 사용하여 필터링 및 집계를 수행할 수 있다.


### Downstream View Tables
- **[해당 target table을 활용하는 View table은 없음]**