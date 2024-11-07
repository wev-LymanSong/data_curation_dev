
we_meta.wv_clog_page
====================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_meta**|
|**Table Type**|META PRIMARY|
|**Partitioned by**| |
|**Created/ Last Updated At**|2023-12-26 / 2024-01-15|
|**Created By**|송재영|
|**Last Updated By**|송재영|
|**Collaborators**|송재영[3]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2023-12-26|송재영|[PR](https://github.com/benxcorp/databricks/commit/aa3014ed607e444346d48145faa81fd497357e0a)|
|2023-12-27|송재영|[PR](https://github.com/benxcorp/databricks/commit/c89b466d69bf789ddc916af8fdde7c3392962747)|
|2024-01-15|송재영|[PR](https://github.com/benxcorp/databricks/commit/06bcae095ec7ba460d2c5b9b5fafe96376234e18)|
  
  
# TABLE NOTICE
  
### 테이블 개요

* **테이블 목적**: 위버스 플랫폼 내에서 사용되는 모든 페이지 목록을 정의하고, 각 페이지에 대한 메타 정보를 제공합니다.
* **데이터 레벨**: META DATA
* **파티션 키**: 없음
* **주요 키**: `page_uid`

### 테이블 Sources

* 내부 데이터
    * `wev_prod.we_meta.wv_clog_page`: 이전에 생성된 `wv_clog_page` 테이블의 데이터를 사용하여 최신 페이지 정보를 업데이트합니다.

### 데이터 추출 및 생성 과정

1. **데이터 추출**: Google 스프레드시트 [clog 스키마 import](https://docs.google.com/spreadsheets/d/1nBsHvVOplIjIy3cuTdZyVT-4vka3aSoV74Ow_e2UXJs/edit#gid=0)에서 페이지 목록과 메타 정보를 추출합니다.
2. **데이터 전처리**:
    * 스프레드시트에서 추출한 데이터를 Spark DataFrame으로 변환합니다.
    * 컬럼명을 영문으로 변경하고, 컬럼 타입을 변환합니다.
    * `page_type`, `page_status` 컬럼의 값을 표준화합니다.
    * `cre_date`, `upd_date` 컬럼을 `date` 타입으로 변환합니다.
3. **데이터 통합**:
    * 추출한 데이터를 `wev_prod.we_meta.wv_clog_page` 테이블과 `page_uid` 컬럼을 기준으로 왼쪽 조인합니다.
    * 조인 결과에서 각 컬럼의 값을 우선순위에 따라 병합합니다.
    * `run_timestamp` 컬럼을 추가하여 데이터 생성 시간을 기록합니다.
4. **최종 테이블 생성**:
    * 최종 데이터를 `wev_prod.we_meta.wv_clog_page` 테이블에 덮어쓰기합니다.

### 테이블 활용 가이드

* **주요 타겟 분야**:
    * 위버스 플랫폼 내에서 사용되는 페이지 목록 관리
    * 페이지 별 메타 정보 확인 및 분석
    * 페이지별 로그 데이터 분석 및 활용
* **조인 시 유의사항**:
    * `page_uid` 컬럼을 기준으로 다른 테이블과 조인하여 페이지 관련 정보를 통합할 수 있습니다.
    * 다른 테이블과 조인할 때, `page_uid` 컬럼의 데이터 유형이 일치하는지 확인해야 합니다.
    * `page_status` 컬럼을 이용하여 삭제된 페이지 또는 정의/수정 중인 페이지를 제외하고 분석할 수 있습니다.

### 추가 정보

* `is_formatted` 컬럼은 페이지 포맷 설정이 완료되었는지 여부를 나타냅니다.
* `num_logs` 컬럼은 페이지에 정의된 로그(action, view, impression 등)의 수를 나타냅니다.
* `cre_by` 컬럼은 페이지를 정의한 팀원명을 나타냅니다.
* `url_link` 컬럼은 페이지와 관련된 URL 링크를 나타냅니다.
* `run_timestamp` 컬럼은 데이터 생성 시간을 나타냅니다.
* `page_desc` 컬럼은 페이지에 대한 간단한 설명을 나타냅니다.  
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|page_uid|int|페이지별 유니크 integer id|
|1|page_id|string|페이지 명|
|2|page_type|string|페이지 타입{일반(NORMAL), 공통(SHARE)}|
|3|page_desc|string|페이지에 대한 간단한 설명|
|4|cre_by|string|정의한 팀원명|
|5|cre_date|date|정의한 날짜|
|6|upd_date|date|수정한 날짜|
|7|url_link|string|url|
|8|page_status|string|페이지 정의 상태{정상(NORAML, NORMAL_NOT_FORMATTED), 삭제(DELETE), 정의/수정 중(ON_DEFINITION)|
|9|is_formatted|int|포맷화 여부|
|10|num_logs|int|정의된 로그(action, view, impression 등) 수|
|11|run_timestamp|timestamp|배치 실행 시간|
  
    
---
# HOW TO USE
  
No content.  
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_we_mart_weekly`

### Update Interval: WEEKLY

### Update Type: OVERWRITE

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/meta/we_meta/wv_clog_page.py)

### Airflow: [DAG](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_we_mart_weekly.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|we_meta.wv_clog_page|we_mart.stats_wv_w_clog_status|
| |we_meta.wv_clog_action|
| |we_meta.wv_clog_gnb_page_action|
| |we_meta.wv_clog_page|

## 🐤 Downstream Tables Info
  
No content.  
---