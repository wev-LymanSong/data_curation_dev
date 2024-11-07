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

* **테이블 목적**: 위버스 플랫폼 내 모든 페이지 정보(페이지 ID, 타입, 설명, 작성자, URL, 상태, 포맷 여부, 로그 수 등)를 저장
* **데이터 레벨**: META DATA
* **파티션 키**: 없음
* **주요 키**: `page_uid`

### 테이블 Sources
* 내부 데이터
    * `wev_prod.we_meta.wv_clog_page`: 이전에 생성된 페이지 정보를 저장
    * `wev_prod.we_meta.wv_clog_scheme`: 페이지 로그 스키마 정보를 저장

### 데이터 추출 및 생성 과정

1. **데이터 추출**:
    * Google 스프레드시트에서 `화면목록` 워크시트에서 페이지 정보를 추출
2. **데이터 전처리**:
    * `page_list`라는 스파크 데이터프레임 생성
    * `no` 컬럼을 `page_uid` 컬럼으로 변환
    * `type` 컬럼을 `page_type` 컬럼으로 변환 (일반 -> NORMAL, 공통 -> SHARE, 나머지 -> ETC)
    * `페이지명` 컬럼을 `page_desc` 컬럼으로 변환
    * `바꿔` 컬럼을 `cre_by` 컬럼으로 변환
    * `이벤트_로그정의서_link` 컬럼을 `url_link` 컬럼으로 변환
    * `페이지_정의_상태`와 `포맷설정_완료` 컬럼을 `page_status` 컬럼으로 변환 (정상 + 포맷설정_완료 == O -> NORMAL, 정상 + 포맷설정_완료 != O -> NORMAL_NOT_FORMATTED, 삭제 -> DELETED, 나머지 -> ON_DEFINITION)
    * `포맷설정_완료` 컬럼을 `is_formatted` 컬럼으로 변환 (O -> 1, 나머지 -> 0)
    * `정의된_로그_수` 컬럼을 `num_logs` 컬럼으로 변환
    * `최초_작성일` 컬럼을 `cre_date` 컬럼으로 변환
    * `최신_작성일` 컬럼을 `upd_date` 컬럼으로 변환
3. **데이터 통합**:
    * `page_list` 데이터프레임과 `pages_last_week` 데이터프레임을 `page_uid` 컬럼 기준으로 왼쪽 조인
    * `page_uid`, `page_id`, `page_type`, `page_desc`, `cre_by`, `cre_date`, `upd_date`, `url_link`, `page_status`, `is_formatted`, `num_logs` 컬럼을 선택
    * `run_timestamp` 컬럼을 추가
4. **최종 테이블 생성**:
    * 최종 데이터프레임을 `wev_prod.we_meta.wv_clog_page` 테이블에 저장

### 테이블 활용 가이드

* **주요 타겟 분야**:
    * 위버스 플랫폼 내 페이지 정보 분석
    * 페이지 상태 및 포맷 현황 파악
    * 페이지별 로그 수 추적
    * 페이지 작성자 및 작성 시점 확인
* **조인 시 유의사항**:
    * `page_uid` 컬럼을 다른 테이블과 조인하여 페이지 관련 데이터를 통합
    * 페이지 정보는 Google 스프레드시트에서 추출된 데이터이므로, 스프레드시트의 최신 정보와 일치하는지 확인

### 추가 정보

* `page_uid`는 페이지별 고유 ID로, 다른 테이블과 조인할 때 사용
* `page_status` 컬럼은 페이지 정의 상태를 나타내며, `NORMAL`, `NORMAL_NOT_FORMATTED`, `DELETED`, `ON_DEFINITION` 등의 값을 가짐
* `is_formatted` 컬럼은 페이지 포맷 여부를 나타내며, 1은 포맷 완료, 0은 포맷 미완료를 의미
* `num_logs` 컬럼은 페이지에 정의된 로그 수를 나타냄
* `run_timestamp` 컬럼은 데이터 로딩 시간을 나타냄
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