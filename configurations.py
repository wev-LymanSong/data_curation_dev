import os
import pandas as pd
import re
import json

KEY_DIR = "/Users/lymansong/Documents/GitHub/keys"
BASE_DIR = "/Users/lymansong/Documents/GitHub/mtms"
DATA_DIR = os.path.join(BASE_DIR, 'data')
REQ_DIR = os.path.join(DATA_DIR, 'requests_extraction')
SPEC_DIR = os.path.join(DATA_DIR, "specs")
SOURCECODE_DIR = os.path.join(DATA_DIR, "source_codes")

REPO_DIR = '/Users/lymansong/Documents/GitHub/databricks'
CODE_DIR = os.path.join(REPO_DIR, "src/data_analytics")
WE_MART_DIR = os.path.join(CODE_DIR, "mart/we_mart")
WE_META_DIR = os.path.join(CODE_DIR, "meta/we_meta")
WE_STAT_DIR = os.path.join(CODE_DIR, "stats/we_mart")
WI_VIEW_DIR = os.path.join(CODE_DIR, "stats/wi_view")


from dotenv import load_dotenv
print(load_dotenv(dotenv_path= os.path.join(KEY_DIR, ".env")))

COLLABORATOR_DICT = {
    # DATA INSIGHT
      "hyunjjin": "이현지"
    , "sanghyoon": "윤상혁"
    , "jemmalim11": "임지연"
    , "sangminwev": "박상민"
    , "Park-Shinyoung": "박신영"
    , "wev-LymanSong": "송재영"

    , "hyunji@weversecompany.com": "이현지"
    , "sanghyoon@weversecompany.com": "윤상혁"
    , "jemmalim@weversecompany.com": "임지연"
    , "sangmin@weversecompany.com": "박상민"
    , "shinyoung.park@weversecompany.com": "박신영"
    , "lymansong@weversecompany.com": "송재영"

    # DATA ASSETIZATION
    , "hyekyunglim": "임혜경"
    , "hyeinseo-wev": "서혜인"
    , "sakongjh": "사공재현"

    , "hyekyung.lim@weversecompany.com": "임혜경"
    , 'lhk2502@naver.com': "임혜경"
    , "hyein.seo@weversecompany.com": "서혜인"
    , "sakongjh@weversecompany.com": "사공재현"

    # DATA PLATFORM
    , "min.koo": "구민서"
    , "minseo-koo": "구민서"
    , "hayoung0927": "정하영"
    , "kitae00": "김기태"
    , "jaina3066": "김민정"
    , "jangsjang": "장준호"

    #UNKNOWN USERS
    
}

PRIMARY_DAG = [
    "analytics_meta_daily", 
    "analytics_mart_daily", 
    "analytics_ws_mart_daily", 
    "analytics_wv_mart_daily", 
    "analytics_we_mart_daily",
    "analytics_log_daily",
    "analytics_wv_stats_daily",
    "analytics_we_mart_priority_daily",
    "analytics_ws_stats_daily",
    "analytics_we_stats_priority_daily",
    "analytics_we_stats_daily",
    "analytics_we_mart_hourly",
    "analytics_we_mart_monthly",
    "analytics_we_mart_weekly",
]
SECONDARY_DAG = [
    "analytics_we_stats_late_daily",
    "analytics_we_mart_late_daily",
    "analytics_mvp_collection_daily",
]


GENERAL_GUIDE = """
## General Overview of Weverse Platform
Weverse(위버스) is a global fan platform where fans and artists interact and share content. It offers multiple services, including fan communities, exclusive artist content, and merchandise sales.
The term "Weverse" also refers to the fan community service itself, while Weverse Shop(위버스샵), a key component of the platform, is referred to as "shop." These services are coded as WV (Weverse) and WS (Weverse Shop). Other services include Weverse Album(위버스 앨범, WA), Phoning(포닝, PH), and Weverse Magazine(위버스 매거진, MZ). The overall platform is referred to as WE.
Weverse Insight(위버스 인사이트, WI) is the business intelligence tool used by the Data Insight Team for data visualization and analysis.

## The Weverse Datawarehouse
The Weverse Datawarehouse (DW) is primarily managed by the Data Insight Team, responsible for collecting, processing, and analyzing data to provide insights and support decision-making.
Weverse DW consists of three main schemas: we_meta, we_mart, and wi_view.
- we_meta: Contains metadata about platform operations.
- we_mart: Contains transactional and analytical data.
- wi_view: Contains view tables used by Weverse Insight for business intelligence purposes.

Each table in the DW has a corresponding Databricks notebook that extracts and loads data from the source tables, documents the data processing logic, and handles transformations. These notebooks follow the same naming conventions as the tables.
- All notebooks are stored in the databricks GitHub repository, owned by the Data Insight Team.
- The ETL (Extract, Transform, Load) process is typically automated and uses Apache Airflow for workflow orchestration, defining task sequences and dependencies through Directed Acyclic Graphs (DAGs). The DAGs are managed in the dp-airflow repository, with task updates handled by the Data Insight Team.

## General Naming Conventions
The following are the general rules for naming tables in the Weverse Datawarehouse:
- Time intervals are abbreviated as follows: hourly (h), daily (d), weekly (w), monthly (m), and yearly (y). For tables covering multiple time intervals, the abbreviation "t" is used.
- All table and column names use lowercase letters and underscores to separate words.

## Table Types
- **`we_meta` tables**: Primarily sourced from external datasets. For example, `wv_clog_page` stores page definitions from Google Spreadsheets. Some tables are joined with internal data sources or created as views.
  Example: `ws_album_latest` is a view table that filters the latest partition date from `ws_album`.

- **`we_mart` tables**: There are three primary types of tables:

  - Mart tables: Transactional data collected from operational databases and `we_meta`. The names are prefixed with the service code and the subject.
    - Example: `we_user` represents user information from Weverse, and `ws_order` stores order details from Weverse Shop.

  - Stat tables: Aggregated, processed data used for analytical purposes. Stat tables are prefixed with "stats," followed by the time interval, service code, and subject.
    - Example: `stats_we_d_user_join` tracks daily user join statistics for Weverse, and `stats_ws_album_sale_smry` summarizes album sales data for Weverse Shop.

  - ODS tables: Operational Data Store tables capture snapshots of data at specific points in time, typically partitioned by `part_date`
    - Example: `ods_ws_goods` stores snapshots of the `goods` table from Weverse Shop. Tables prefixed with `wv` originate from the `weverse2` operational database (for the Weverse Community), while tables prefixed with `ws` originate from the `weverseshop` database (for Weverse Shop).

- **`wi_view` tables**: Used for visualization and analysis within Weverse Insight. These tables aggregate data from `we_meta` and `we_mart`.
  - Example: `wi_d_ord_ctry` aggregates Weverse order data by country on a daily basis.

## General Table Structure and Partitioning
- For meta and mart tables, each row represents a unique entity or event (e.g., user, order) and is associated with a timestamp or identifier.
- Stat tables store aggregated data for specific time intervals or dimensions (e.g., service_code, ctry_code).
  - A `run_timestamp` column tracks the time the data was ingested or processed.
  - Many tables are partitioned by date (`part_date`, `part_week`, `part_month`) or batch time intervals.
  - Stat tables often use dimension columns such as `service_code`, `ctry_code`, and `we_art_id` for indexing.

## Frequently used columns
- **Dimension Columns (categorical data)**:
  - `we_art_id` and `we_art_name`: Represent artist ID and name, sourced from `we_mart.we_artist`, the table containing artist metadata.
  - `ctry_code` (or `ctry`): Represents the country code, used for filtering and aggregating data by country.
  - Sales-related columns: `sale_id` for sold products, `goods_id`, and `product_id` for orders.
  - `service_code`: Identifies the service associated with the data.
  - Example column names: `album_id` and `album_name` (for albums), `comm_id` (for communities).
  - For tables with too many dimension columns, paired columns like `dim_name` and `dim_value` represent the key-value structure.
  - Columns prefixed with `is_` indicate boolean flags (e.g., `is_pay`, `is_fc`). The data type may be an integer in some cases.
  - `fc` stands for Fan Club membership; `is_fc` indicates whether a user is part of an artist's Fan Club.
- **Identifier Columns**:
  - `we_member_id`: The Weverse platform-wide user ID. Other identifiers include `wv_user_id` for the Weverse community and `ws_user_id` for Weverse Shop.
  - `ord_item_id`: Unique identifier for order items; `ord_sheet_id` and `ord_sheet_number` for order sheets.
  - `post_id`: The unique identifier for posts in the Weverse community.
- **Date and Time Columns**:
  - In mart tables, datetime columns end with `_dt`. For example, `pay_dt` (payment date/time), `trx_cre_dt` (transaction creation date/time).
  - Common columns include `cre_dt` (row creation datetime) and `upd_dt` (row update datetime). The time zone may vary depending on the operational database.
  - `key_date`: The primary date column for stat tables, representing the date for which statistics are calculated.
  - `part_date` is commonly used for partitioning and is a string, while `key_date` is typically a date type.
  - All date columns are formatted as `yyyy-MM-dd`, and the time zone is KST (UTC+9).
- **Aggregated (Fact) Columns**:
  - Columns like `cnt` represent counts of items or events, and `uu_cnt` counts unique users.
  - For sales and orders, `ord_item_qty` (quantity ordered) and `ord_item_amt` (order amount) are common.
  - `ord_amt_krw`: The total order amount in Korean Won (KRW).
"""


MART_TABLE_NOTICE_TEMPLATE = """
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
"""