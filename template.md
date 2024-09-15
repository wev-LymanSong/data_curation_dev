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