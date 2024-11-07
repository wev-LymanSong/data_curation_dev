
we_meta.wv_clog_action
======================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_meta**|
|**Table Type**|META PRIMARY|
|**Partitioned by**|`part_week`|
|**Created/ Last Updated At**|2023-12-26 / 2024-09-03|
|**Created By**|송재영|
|**Last Updated By**|송재영|
|**Collaborators**|송재영[6]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2023-12-26|송재영|[PR](https://github.com/benxcorp/databricks/commit/aa3014ed607e444346d48145faa81fd497357e0a)|
|2024-01-04|송재영|[PR](https://github.com/benxcorp/databricks/commit/8acda14ad4e5941c57ef6685bb2705078dfbe387)|
|2024-02-28|송재영|[PR](https://github.com/benxcorp/databricks/commit/46f2719d9c25d04ea579ceb487ad5742218f6cff)|
|2024-03-26|송재영|[PR](https://github.com/benxcorp/databricks/commit/599814362127c59a1243dbcb40a3f31c288df0fe)|
|2024-07-29|송재영|[PR](https://github.com/benxcorp/databricks/commit/1588c54e76a0b90529f298336f5d63c2fe3062c2)|
|2024-09-03|송재영|[PR](https://github.com/benxcorp/databricks/commit/630b88dc0171cab62ab24813b5941741eb734ccf)|
  
  
# TABLE NOTICE
  
### 테이블 개요

*   **테이블 목적**: 위버스 플랫폼의 로그 정의서에서 정의된 로그 정보들을 수집하여 분석에 사용할 수 있도록 데이터를 저장
*   **데이터 레벨**: META DATA
*   **파티션 키**: `part_week`
*   **주요 키**: `page_uid`, `action_uid`

### 테이블 Sources

*   내부 데이터
    *   `wev_prod.we_meta.wv_clog_page`: 위버스 플랫폼의 페이지 정보를 저장하는 테이블
    *   `wev_prod.we_meta.wv_clog_action`: 위버스 플랫폼의 로그 정보를 저장하는 테이블
*   외부 데이터
    *   [Google Spreadsheets](https://docs.google.com/spreadsheets/d/1nBsHvVOplIjIy3cuTdZyVT-4vka3aSoV74Ow_e2UXJs/edit#gid=0): 로그 정의서를 저장하는 Google Spreadsheets 문서

### 데이터 추출 및 생성 과정

1.  **Google Spreadsheets에서 로그 정의서 데이터 추출**:
    *   `importrange` 함수를 사용하여 Google Spreadsheets에서 로그 정의서 데이터를 추출
2.  **데이터 전처리**:
    *   `log_data` 변수에 Google Spreadsheets에서 추출한 로그 정의서 데이터를 저장
    *   `cols` 변수에 로그 정의서 컬럼 이름을 저장
    *   `cur_logs` 변수에 `log_data`에서 컬럼 이름을 적용하여 Pandas DataFrame 형태로 변환
    *   `actions` 변수에 `cur_logs`를 사용하여 Spark DataFrame을 생성
3.  **데이터 통합**:
    *   `page_sdf`와 `actions`를 `page_uid` 컬럼을 기준으로 왼쪽 조인
    *   `event_value_list` 컬럼을 `array<string>` 타입으로 변환
    *   `ios`, `aos`, `mw`, `pc`, `tvos` 컬럼을 `string` 타입으로 변환
    *   `part_week` 컬럼에 `key` 값을 추가
    *   `run_timestamp` 컬럼에 현재 시간을 추가
4.  **최종 테이블 생성**:
    *   `log_df` 변수에 최종적으로 생성된 Spark DataFrame을 저장
    *   `dflow` 변수에 Dataflow 객체를 생성
    *   `dflow` 객체의 `run` 메소드를 사용하여 `log_df`를 `wev_prod.we_meta.wv_clog_action` 테이블에 저장
    *   `part_week` 컬럼을 파티션 키로 사용

### 테이블 활용 가이드

*   **주요 타겟 분야**: 위버스 플랫폼의 로그 정의서를 기반으로 한 로그 정보 분석
*   **조인 시 유의사항**:
    *   `page_uid` 컬럼을 사용하여 `wev_prod.we_meta.wv_clog_page` 테이블과 조인하여 페이지 정보를 추가
    *   `action_uid` 컬럼을 사용하여 `wev_prod.we_meta.wv_clog_action` 테이블과 조인하여 로그 정보를 추가

### 추가 정보

*   `event_value_list` 컬럼은 로그에 대한 연계 변수 리스트를 저장
*   `ios`, `aos`, `mw`, `pc`, `tvos` 컬럼은 각 플랫폼에서 로그가 지원되는지 여부를 나타냄
*   `part_week` 컬럼은 데이터가 적재된 주를 나타냄
*   `run_timestamp` 컬럼은 데이터가 적재된 시간을 나타냄  
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|page_uid|int|페이지별 유니크 integer id|
|1|cre_by|string|정의한 팀원명|
|2|log_status|string|로그 상태값|
|3|action_uid|int|로그 별 유니크 integer id|
|4|page_id|string|페이지 명|
|5|action_id|string|로그 명|
|6|impression_match_type|string|노출 영역 타입|
|7|impression_content_type|string|노출 단위 객체 타입|
|8|shop_referrer|string|샵 랜딩에 대한 referrer 값 정보|
|9|event_desc|string|로그에 대한 설명|
|10|event_desc_add|string|로그에 대한 부가 설명|
|11|ios|string|iOS 로그 상태|
|12|aos|string|Android 로그 상태|
|13|mw|string|Mobile Web 로그 상태|
|14|pc|string|PC Web 로그 상태|
|15|tvos|string|tvOS 로그 상태|
|16|event_value_list|array<string>|연계 로그 변수 리스트|
|17|part_week|string|적재 주|
|18|run_timestamp|timestamp|적재 당시 시간(UTC)|
  
    
---
# HOW TO USE
  
No content.  
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_we_mart_weekly`

### Update Interval: WEEKLY

### Update Type: APPEND

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/meta/we_meta/wv_clog_action.py)

### Airflow: [DAG](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_we_mart_weekly.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|we_meta.wv_clog_page|we_mart.stats_wv_d_clog_sess_impression|
| |we_mart.stats_wv_w_clog_status|
| |we_meta.wv_clog_gnb_page_action|

## 🐤 Downstream Tables Info
  
No content.  
---