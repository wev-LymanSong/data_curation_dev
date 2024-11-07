
we_mart.wv_survey_answer_log
============================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_mart**|
|**Table Type**|SURVEY SECONDARY|
|**Partitioned by**|`part_date`|
|**Created/ Last Updated At**|2023-05-24 / 2024-08-01|
|**Created By**|사공재현|
|**Last Updated By**|사공재현|
|**Collaborators**|사공재현[9], 송재영[1]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2023-05-24|사공재현|[PR](https://github.com/benxcorp/databricks/commit/e81a55bf7ec4fa654aae9c44095c75393eac7b1d)|
|2023-05-30|사공재현|[PR](https://github.com/benxcorp/databricks/commit/6b4ae51041b8240e69040b79051552614149196c)|
|2023-06-20|사공재현|[PR](https://github.com/benxcorp/databricks/commit/9f929508e1f509685970bf7d9b552e1299fba07b)|
|2023-06-25|사공재현|[PR](https://github.com/benxcorp/databricks/commit/01beb4f88306b9072dbc5102e89c35be8061b316)|
|2023-12-20|사공재현|[PR](https://github.com/benxcorp/databricks/commit/128a4ab6bc446529d344c1234755f673d359e40c)|
|2024-01-16|사공재현|[PR](https://github.com/benxcorp/databricks/commit/59eab293a238a2ba328c286d1b3dd67557d3d15a)|
|2024-05-03|사공재현|[PR](https://github.com/benxcorp/databricks/commit/fdd08dce19d213b85123c9188435cff221fb707b)|
|2024-05-21|사공재현|[PR](https://github.com/benxcorp/databricks/commit/703677898cf91a10419ecb0b9e8b8c83a65db370)|
|2024-07-08|송재영|[PR](https://github.com/benxcorp/databricks/commit/05059b8fe9303f5ab04da0e69662c170b7ed2246)|
|2024-08-01|사공재현|[PR](https://github.com/benxcorp/databricks/commit/884bbf501415dae98899c991a089c4167832936f)|
  
  
# TABLE NOTICE
  
### 테이블 개요

* **테이블 목적**: Weverse 설문 응답 데이터를 저장
* **데이터 레벨**: TRANSACTIONAL DATA
* **파티션 키**: `part_date`
* **주요 키**: `we_member_id`, `survey_info_id`, `question_number`, `option_sort`

### 테이블 특징

* `we_member_id` 컬럼은 Weverse 플랫폼 사용자 ID를 나타냄
* `survey_info_id` 컬럼은 설문 ID를 나타냄
* `question_number` 컬럼은 설문 질문 번호를 나타냄
* `option_sort` 컬럼은 설문 질문 보기 순서를 나타냄
* `option_label` 컬럼은 설문 질문 보기 라벨을 나타냄
* `answer_time` 컬럼은 사용자가 설문에 응답한 시간을 나타냄
* `run_timestamp` 컬럼은 데이터가 테이블에 로드된 시간을 나타냄
* `part_date` 컬럼은 데이터가 입력된 날짜를 나타내며 파티션 키로 사용됨

### 데이터 추출 및 생성 과정

1. **주요 데이터 소스**:
    * `fanvoice_survey.old_user_answer` 테이블: 과거 설문 응답 데이터
    * `fanvoice_survey.user_answer` 테이블: 현재 설문 응답 데이터
    * `fanvoice_survey.question_lang` 테이블: 설문 질문 정보
    * `fanvoice_survey.option_lang` 테이블: 설문 질문 보기 정보
2. **데이터 전처리**:
    * `fanvoice_survey.old_user_answer` 및 `fanvoice_survey.user_answer` 테이블에서 필요한 컬럼을 선택
    * `question_lang` 테이블에서 `question` 컬럼의 HTML 태그를 제거
    * `update_time` 컬럼을 `answer_time` 컬럼으로 변환
3. **데이터 통합**:
    * `fanvoice_survey.old_user_answer` 및 `fanvoice_survey.user_answer` 테이블과 `fanvoice_survey.question_lang` 및 `fanvoice_survey.option_lang` 테이블을 조인하여 설문 질문 정보를 추가
4. **최종 테이블 생성**:
    * `we_mart.wv_survey_answer_log` 테이블을 생성하고 전처리 및 통합된 데이터를 로드

### 테이블 활용 가이드

* **주요 활용**:
    * Weverse 설문 응답 데이터 분석
    * 사용자 설문 응답 패턴 분석
    * 설문 결과 시각화
* **조인 시 유의사항**:
    * `we_member_id` 컬럼을 사용하여 다른 사용자 관련 테이블과 조인 가능
    * `survey_info_id` 컬럼을 사용하여 다른 설문 관련 테이블과 조인 가능
    * `question_number` 컬럼을 사용하여 다른 질문 관련 테이블과 조인 가능
    * `option_sort` 컬럼을 사용하여 다른 보기 관련 테이블과 조인 가능

### 추가 정보

* `part_date` 컬럼을 사용하여 특정 날짜의 설문 응답 데이터를 조회 가능
* `run_timestamp` 컬럼을 사용하여 데이터 로드 시간을 확인 가능
* `answer_time` 컬럼을 사용하여 사용자 응답 시간을 분석 가능
* `we_member_id` 컬럼을 사용하여 사용자별 설문 응답 데이터를 조회 가능  
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|we_member_id|bigint|통합 계정인 wemember의 ID, 주요 키|
|1|survey_info_id|int|설문ID|
|2|question_number|string|질문 번호|
|3|question|string|질문|
|4|option_sort|int|질문의 보기 ID|
|5|option_label|string|질문의 응답 항목|
|6|number_value|int|주관식 숫자형 답변|
|7|boolean_value|tinyint|선택형 답변|
|8|string_value|string|주관식 문자형 답변|
|9|url_value|string|업로드 미디어 경로|
|10|answer_time|timestamp|응답시간|
|11|run_timestamp|timestamp|데이터가 insert된 시간|
|12|part_date|string|데이터가 입력된 날짜, string이지만 partition key로 사용|
  
    
---
# HOW TO USE
  
No content.  
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_we_mart_late_daily`

### Update Interval: DAILY

### Update Type: OVERWRITE

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/survey/wv_survey_answer_log.py)

### Airflow: [DAG](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_we_mart_late_daily.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|fanvoice_survey.option_lang|we_mart.stats_wv_d_survey_answer|
|fanvoice_survey.question_lang|we_mart.we_user_demo_log|
|fanvoice_survey.survey_lang| |
|fanvoice_survey.user_answer| |

## 🐤 Downstream Tables Info
  
No content.  
---