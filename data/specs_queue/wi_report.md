we_meta.wi_report
=================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_meta**|
|**Table Type**|META SECONDARY|
|**Partitioned by**|`part_date`|
|**Created/ Last Updated At**|2024-01-11 / 2024-08-09|
|**Created By**|송재영|
|**Last Updated By**|송재영|
|**Collaborators**|송재영[9], 윤상혁[1]|

#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2024-01-11|송재영|[PR](https://github.com/benxcorp/databricks/commit/dfbed8ca68330fa053a536faabb1b235decd9c5f)|
|2024-02-05|송재영|[PR](https://github.com/benxcorp/databricks/commit/689da6f258e1587c9c109d9a4f8e7503e83f59f0)|
|2024-02-05|송재영|[PR](https://github.com/benxcorp/databricks/commit/46b60e72648202c3d0a7102aba2efe6603439485)|
|2024-02-20|송재영|[PR](https://github.com/benxcorp/databricks/commit/766d7b2eef166387b7de30b0d08cf383101c1dff)|
|2024-02-22|송재영|[PR](https://github.com/benxcorp/databricks/commit/3faf0abdb3e3acc15deacdd473b5dd59c647909b)|
|2024-02-22|송재영|[PR](https://github.com/benxcorp/databricks/commit/d800369ef8269f90f5a979671e62617c0980cc36)|
|2024-02-28|송재영|[PR](https://github.com/benxcorp/databricks/commit/46f2719d9c25d04ea579ceb487ad5742218f6cff)|
|2024-04-04|송재영|[PR](https://github.com/benxcorp/databricks/commit/e6ce3cb91b681e8cd0d4ccc97c93826045b1ec80)|
|2024-05-18|윤상혁|[PR](https://github.com/benxcorp/databricks/commit/1591ceb820b2e0c15d67471f0100c483ba4aaf7e)|
|2024-08-09|송재영|[PR](https://github.com/benxcorp/databricks/commit/64fb346f8c9226b7835819c190bea892103ac462)|


# TABLE NOTICE

### 테이블 개요

*   **테이블 목적**: 위버스 인사이트 리포트의 메타 데이터를 저장
*   **데이터 레벨**: META DATA
*   **파티션 키**: `part_date`
*   **주요 키**: `report_id`

### 테이블 Sources

*   내부 데이터
    *   `wev_prod.we_meta.wi_report`: 위버스 인사이트 리포트의 메타 데이터를 저장하는 테이블

### 데이터 추출 및 생성 과정

1.  **데이터 추출**:
    *   `requests` 라이브러리를 사용하여 Weverse Insight API에서 리포트 메타 데이터를 가져온다.
2.  **데이터 전처리**:
    *   `json` 라이브러리를 사용하여 API 응답을 JSON 형식으로 변환한다.
    *   `pandas` 라이브러리를 사용하여 데이터프레임으로 변환한다.
    *   `pyspark` 라이브러리를 사용하여 스파크 데이터프레임으로 변환한다.
    *   `update_period` 컬럼을 생성하기 위해 `categories` 컬럼을 처리한다.
    *   `parent_report_id` 컬럼을 생성하기 위해 Google 스프레드시트에서 리포트 정보를 가져와 `report_ids` 데이터프레임을 생성한다.
    *   `report_ids` 데이터프레임을 `df` 데이터프레임과 조인한다.
    *   필요없는 컬럼을 제거하고 최종 컬럼을 선택한다.
    *   `part_date` 컬럼을 추가하고 `run_timestamp` 컬럼을 현재 시간으로 설정한다.
3.  **데이터 통합**:
    *   전처리된 데이터를 `wev_prod.we_meta.wi_report` 테이블에 저장한다.
4.  **최종 테이블 생성**:
    *   `Dataflow` 클래스를 사용하여 데이터를 테이블에 저장한다.
    *   Google 스프레드시트에 테이블 데이터를 업데이트한다.

### 테이블 활용 가이드

*   **주요 타겟 분야**:
    *   위버스 인사이트 리포트의 메타 데이터를 분석하고 활용할 수 있다.
    *   리포트의 특징, 주기, 설명 등을 분석하여 리포트 관리 및 활용에 활용할 수 있다.
*   **조인 시 유의사항**:
    *   `report_id` 컬럼을 사용하여 다른 테이블과 조인할 수 있다.
    *   `part_date` 컬럼을 사용하여 다른 테이블과 조인할 때, 동일한 기간의 데이터를 조인해야 한다.

### 추가 정보

*   `wev_prod.we_meta.wi_report` 테이블은 `part_date` 컬럼을 기준으로 파티션된다.
*   `report_id` 컬럼은 각 리포트를 식별하는 고유 키이다.
*   `update_period` 컬럼은 리포트 업데이트 주기를 나타낸다.
*   `description` 컬럼은 리포트 설명을 저장한다.
*   `tags` 컬럼은 리포트 태그를 저장한다.
*   `url` 컬럼은 리포트 URL을 저장한다.
*   `categories` 컬럼은 리포트 카테고리를 저장한다.
*   `i18n_report_title` 컬럼은 리포트 제목의 다국어 버전을 저장한다.
*   `i18n_description` 컬럼은 리포트 설명의 다국어 버전을 저장한다.
*   `run_timestamp` 컬럼은 데이터가 저장된 시간을 나타낸다.
*   API 호출 시 `result['message']` 값이 `success`가 아니면 슬랙 알림을 보내고 작업을 종료한다.
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|report_title|string|리포트 명|
|1|parent_report_title|string|상위 리포트 명|
|2|report_id|string|리포트 별 유니크 key 값|
|3|parent_report_id|string|기본 리포트 key 값|
|4|update_period|string|리포트 타입|
|5|description|string|리포트 기본 설명|
|6|tags|string|리포트 tag들|
|7|url|string|리포트 url|
|8|categories|string|iOS 로그 상태|
|9|i18n_report_title|struct<en:string,ko:string,ja:string>|다국어 값:리포트 명|
|10|i18n_description|struct<en:string,ko:string,ja:string>|다국어 값:리포트 설명|
|11|part_date|string|적재 일자|
|12|run_timestamp|timestamp|적재 당시 시간(UTC)|


---
# HOW TO USE

### Downstream Table/View
- `wi_report` 테이블을 사용하여 `wi_report_daily` 테이블 생성
    - ```sql
      create or replace table wev_prod.we_meta.wi_report_daily
      as
      select *
      from wev_prod.we_meta.wi_report
      where part_date = '2024-01-01';
      ```
- `wi_report` 테이블을 사용하여 `wi_report_view` 뷰 생성
    - ```sql
      create or replace view wev_prod.we_meta.wi_report_view
      as
      select report_title, parent_report_title, report_id, parent_report_id, update_period, description, tags, url, categories, i18n_report_title, i18n_description
      from wev_prod.we_meta.wi_report
      where part_date = '2024-01-01';
      ```
- `wi_report` 테이블과 `we_artist` 테이블을 조인하여 `wi_report_artist` 뷰 생성
    - ```sql
      create or replace view wev_prod.we_meta.wi_report_artist
      as
      select wr.report_title, wr.parent_report_title, wr.report_id, wr.parent_report_id, wr.update_period, wr.description, wr.tags, wr.url, wr.categories, wr.i18n_report_title, wr.i18n_description, wa.we_art_name
      from wev_prod.we_meta.wi_report wr
      join wev_prod.we_mart.we_artist wa on wr.report_title like concat('% ', wa.we_art_name, '%')
      where wr.part_date = '2024-01-01';
      ```
- `wi_report` 테이블을 사용하여 `wi_report_summary` 테이블 생성
    - ```sql
      create or replace table wev_prod.we_meta.wi_report_summary
      as
      select part_date, count(distinct report_id) as report_count, count(distinct report_title) as report_title_count
      from wev_prod.we_meta.wi_report
      group by part_date;
      ```

### Data Extraction
- `wi_report` 테이블에서 `part_date`가 '2024-01-01'인 모든 리포트 정보 추출
    - ```sql
      select *
      from wev_prod.we_meta.wi_report
      where part_date = '2024-01-01';
      ```
- `wi_report` 테이블에서 `report_title`에 "ARTIST"가 포함된 모든 리포트 정보 추출
    - ```sql
      select *
      from wev_prod.we_meta.wi_report
      where report_title like '%ARTIST%';
      ```
- `wi_report` 테이블에서 `update_period`가 'H'인 모든 리포트 정보 추출
    - ```sql
      select *
      from wev_prod.we_meta.wi_report
      where update_period = 'H';
      ```
- `wi_report` 테이블에서 `part_date`가 '2024-01-01'이고 `parent_report_id`가 null인 모든 리포트 정보 추출
    - ```sql
      select *
      from wev_prod.we_meta.wi_report
      where part_date = '2024-01-01' and parent_report_id is null;
      ```
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_we_stats_late_daily`

### Update Interval: DAILY

### Update Type:

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/meta/we_meta/wi_report.py)

### Airflow: [DAG](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_we_stats_late_daily.py)


---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|None.None| |

## 🐤 Downstream Tables Info


---