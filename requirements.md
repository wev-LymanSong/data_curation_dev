간단한 웹 앱을 하나만들고 싶어. 마크다운 편집기 앱이고 서버에서 정의한 여러 파이썬 함수와 langchain을 활용한 생성 기능을 통해 마크다운 파일을 편집할거야

앱은 총 3가지 영역으로 구성돼있어. Side Navigation Bar(SNB) 그리고 편집기 패널, 그리고 컴파일해서 프리뷰를 보여주는 패널 이렇게 3개야

1. SNB 파트에는 현재 대기중인 마크다운 파일들의 리스트와 선태가능한 탐색기가 있어
   1. 여기에서 한 md 파일을 선택하면 편집기 영역과 프리뷰 패널에 해당 파일이 선택돼
   2. 탐색기 아래에는 현재 md파일을 table of contents가 표시되고 해당 파트 옆엔 reroll 버튼이 있어. reroll 버튼을 누르면 해당 파트의 내용이 다시 생성되는거야
   3. SNB영역 마지막엔 PR 하기 버튼이 있어. 이 버튼의 기능은 현재 선택한 파일을 현재 상태 그대로 github repo에 커밋하고 PR을 생성해주는거야. 버튼을 누르면 미리 연결한 repo에 main을 카피한 branch를 하나 만들고 현재 파일을 추가/수정한 PR을 만들어 github PR 생성 페이지로 이동해.
2. 편집기 패널은 마크다운 편집기야. 기본적인 텍스트 편집기와 같아. 아마 외부 오픈소스 편집기 모듈을 임포트해서 사용하면 될 것 같아.
3. 프리뷰 페널은 편집기 패널에서 편집한 마크다운 내용을 실시간으로 컴파일해 보여주는 기능만 해.

md 파일은 형식이 정해져있어 아래는 전체 파일의 템플릿이야

# BASIC INFO

|**About**| |
| :--- | :--- |
|**Database**|**table_name**|
|**Table Type**|MART PRIMARY|
|**Partitioned by**|partition_column |
|**Created/ Last Updated At**|2022-09-22 / 2023-12-08|
|**Created By**|staff|
|**Last Updated By**|staff2|
|**Collaborators**|staff[N], staff2[M-N]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2022-09-22|staff|[PR](https://github.com/owner/reponame/commit/id)|
|...|...|[..](link)|
|2023-12-08|staff2|[PR](https://github.com/owner/reponame/commit/id)|
  
  
# TABLE NOTICE

### 테이블 개요

*   **테이블 목적**: 내용
*   **데이터 레벨**: 내용
*   **파티션 키**: 내용
*   **주요 키**: 내용

### 테이블 특징
* 특징1
* 특징2
* 특징3
* 특징4

### 데이터 추출 및 생성 과정

1.  **주요 데이터 소스**:
    *   내용
    *   내용
2.  **데이터 전처리**:
    *   내용
    *   내용
3.  **데이터 통합**:
    *   내용
4.  **최종 테이블 생성**:
    *   내용

### 테이블 활용 가이드

*   **주요 활용**:
    *   내용
    *   내용
*   **조인 시 유의사항**:
    *   내용
    *   내용

### 추가 정보

*   정보
*   정보
*   정보
*   정보 
    
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|column_1|data type|설명 1|
|...|...|...|...|
|N|column_N|data type|설명 N|
  
    
---
# HOW TO USE

### Downstream Table/View
- [Brief description of the use case]
    - ```sql/py
      [Your SQL query/Python code block here]
      ```
- [Brief description of the use case]
    - ```sql/py
      [Your SQL query/Python code block here]
      ```

### Data Extraction
- [Brief description of the use case]
    - ```sql/py
      [Your SQL query/Python code block here]
      ```
- [Brief description of the use case]     
    - ```sql/py
      [Your SQL query/Python code block here]
      ```
    
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `DAG NAME`

### Update Interval: DAILY

### Update Type: OVERWRITE
  
  
## 📍 LINK URLs

### Github: [Source Code](https://github.com/owner/reponame/src/source_code.py)

### Airflow: [DAG](https://github.com/owner/reponame2/src/airflow_dag.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|db_name.source_table1|db_name.target_table1|
|db_name.source_table2|db_name.target_table2|
|...|...|
|db_name.source_tableN-1|db_name.target_tableN-1|
||db_name.target_tableN|

## 🐤 Downstream Tables Info

설명...
    
---  