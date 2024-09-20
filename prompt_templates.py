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
        - Some tables are not transactional data but are created as meta data, and would be used as a source table for other(downstream) tables.
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

    *   **테이블 목적**: 내용 서술
    *   **데이터 레벨**: TRANSACTIONAL DATA or AGGREGATED DATA(STATISTICS) , or META DATA
    *   **파티션 키**: 파티션컬럼1, 파티션컬럼2
    *   **주요 키**: key컬럼1, key컬럼2, key컬럼3

    ### 테이블 특징
    * 특징1 서술
    * 특징2 서술
    * 특징3 서술
    * 특징4 서술

    ### 데이터 추출 및 생성 과정

    1.  **주요 데이터 소스**:
        *   내용 서술
        *   내용 서술
    2.  **데이터 전처리**:
        *   내용 서술
        *   내용 서술
    3.  **데이터 통합**:
        *   내용 서술
    4.  **최종 테이블 생성**:
        *   내용 서술

    ### 테이블 활용 가이드

    *   **주요 활용**:
        *   내용 서술
        *   내용 서술
    *   **조인 시 유의사항**:
        *   내용 서술
        *   내용 서술

    ### 추가 정보

    *   정보 서술
    *   정보 서술
    *   정보 서술
    *   정보 서술
"""

META_TABLE_NOTICE_TEMPLATE = """
    ### 테이블 개요

    *   **테이블 목적**: 내용
    *   **데이터 레벨**: META DATA
    *   **파티션 키**: 파티션컬럼1, 파티션컬럼2
    *   **주요 키**: key컬럼1, key컬럼2, key컬럼3

    ### 테이블 Sources
    *   내부 데이터
        *   내부 테이블 1: 특징 서술
        *   ...
        *   내부 테이블 N: 특징 서술
    *   외부 데이터(Optional)
        *   외부 데이터 소스1: 특징 서술
        *   외부 데이터 소스N: 특징 서술

    ### 데이터 추출 및 생성 과정

    2.  **데이터 전처리**:
        *   과정 서술
        *   과정 서술
    3.  **데이터 통합**:
        *   과정 서술
        *   과정 서술
    4.  **최종 테이블 생성**:
        *   내용 서술

    ### 테이블 활용 가이드

    *   **주요 타겟 분야**:
        *   내용 서술
        *   내용 서술
    *   **조인 시 유의사항**:
        *   내용 서술
        *   내용 서술

    ### 추가 정보

    *   정보 서술
    *   정보 서술
    *   정보 서술
    *   정보 서술
"""

STAT_TABLE_NOTICE_TEMPLATE = """
    ### 테이블 개요

    *   **테이블 목적**: 내용
    *   **데이터 레벨**: AGGREGATED DATA(STATISTICS)
    *   **파티션 키**: 파티션컬럼
    *   **주요 키**: key컬럼1, key컬럼2

    ### 테이블 소스
    *   테이블 1: 특징 서술
    *   테이블 2: 특징 서술
    *   ...
    *   테이블 N: 특징 서술
    
    ### 데이터 추출 및 생성 과정

    2.  **데이터 전처리**:
        *   과정 서술
        *   과정 서술
    3.  **데이터 통합**:
        *   과정 서술
        *   과정 서술
    4.  **최종 테이블 생성**:
        *   내용 서술

    ### 테이블 활용 가이드

    *   **타겟 서비스 및 분야**:
        *   내용 서술
        *   내용 서술
    *   **사용 시 유의사항**:
        *   내용 서술
        *   내용 서술

    ### 추가 정보

    *   정보 서술
    *   정보 서술
"""

field2table_notice_template = {
    'we_mart' : MART_TABLE_NOTICE_TEMPLATE, 
    'we_meta' : META_TABLE_NOTICE_TEMPLATE, 
    'we_stats' : STAT_TABLE_NOTICE_TEMPLATE, 
    'wi_view' : "no view table template.", 
}


TABLE_NOTICE_PROMPT = """
You are tasked with creating a data table specification document based on a Python notebook containing a series of queries for creating data. This document will help future users understand the structure, purpose, and proper usage of the target table. Follow these instructions carefully:

1. You will be provided with the Python notebook code in the following format:
<notebook_code>
{notebook_code}
</notebook_code>

2. You will also be provided with several documents that contain the data specifications of the source tables used to create the target table. Review these documents to better understand the source data:
<data_specification_documents>
{data_specification_documents}
</data_specification_documents>
3. Analyze the provided code and documents thoroughly. Pay attention to:
- The structure of the queries
- Data sources and transformations
- Column names and their purposes
- Any specific methods or design patterns used
- Here are brief explanations and general guidelines for Weverse Datawarehouse. Follow the rules in it:
    <general_guide>
    {general_guide}
    </general_guide>

4. Create a data table specification document that explains:
- The logic behind how the target table is created
- The structure of the data
- Key insights and findings based on the code analysis

5. Provide guidance for future users on how to use the target table, including:
- Cautions when joining this table with others
- How to extract meaningful insights or perform specific analyses
- The level of the target table (e.g., aggregated or detailed)
- The table could be a transactional, meta, or aggregated(statistics) data table. The options are provided in the template document, so choose the appropriate one.
    - we_mart table can be one of 3 types, on the other hand, we_meta tables are always meta tables and we_stats tables are always statistics tables.

6. Format your output as follows:
- Use a series of bullet points to summarize key insights and findings
- Use markdown formatting
- Use indentation to show relationships between content
- Wrap column names, code blocks, or other code-based object names in backticks (`)
- Use headings up to level 2 only
- 한국어 결과는 존댓말이 아닌 단답으로 해줘. (ex. "A 컬럼을 파티션 키로 활용 합니다." 대신 "A 컬럼을 파티션 키로 활용")
- If external data sources are mentioned, use markdown hyperlink format: [source page name](https://abc.com)
- The following is a template of the expected output format. Please use these as a layout example for structure and style. Note that specific details may vary depending on the table and source code structure.
    <template_document>
    {template_document}
    </template_document>

7. In your analysis, be sure to cover:
- The structure of the table, including dimension columns and their purposes
- Any distinctive columns or data representation methods
- How the table is indexed or partitioned
- The data extraction process, being as specific as possible

8. Do NOT include the following in your output:
- Detailed analysis of CSV file structure
- Basic abstract information about the table
- Explanation of specific settings or configurations
- One-by-one column explanation (DDL)
- Description of how the target table is stored using specific tools or libraries other than query or pyspark codes

9. Begin your analysis immediately after these instructions. Provide your entire output within <answer> tags.

Remember to focus on providing valuable insights and practical guidance for future users of the data table. Your analysis should be thorough, clear, and tailored to the specific code provided.
"""

HOT_TO_USE_PROMPT = """
You are tasked with creating example SQL queries or Python scripts using a provided target table, based on given input queries and/or code blocks. Your goal is to demonstrate how to use the target table in SQL queries and Python code (mainly with PySpark), focusing on filtering specific columns and joining with other tables when necessary.

**Input Data**

The target table name is:
<target_table>
{target_table}
</target_table>

This is the source code for the target table:
<target_table_source_code>
{target_table_source_code}
</target_table_source_code>

You will be given a set of Python scripts for downstream tables that use the target table as a source, as well as several data extraction samples for requests. These follow the rules:

- For extract queries,
  - Each extract query is divided by an issue_id (e.g., `==DATA-NNNN==`, where NNNN is a four-digit number like `==DATA-1234==`), originating from a notebook file (.sql or .py notebook file).
  - Some may include request descriptions.

- For Python scripts for downstream tables,
  - Python scripts for downstream tables are divided by table name (e.g., `==we_table==`).

- An extract query or Python script consists of multiple cells, each cell has:
    - `cell_type`: indicates whether the cell is Python or SQL.
    - `cell_title`: (Optional) the cell title.
    - `role`: the role of the cell in the source code (e.g., code, heading, description, etc.).
    - `code`: the source code (a SQL query or a Python script).
    
Here are the input queries and extracts for data requests:
<extract_samples>
{extract_samples}
</extract_samples>

Here are the Python scripts for downstream tables:
<downstream_table_source_code>
{downstream_table_source_code}
</downstream_table_source_code>

Here is a brief explanation and general guidelines for the Weverse Data Warehouse. Please review it and use it as a reference for the data tables.
<general_guide>
{general_guide}
</general_guide>

**Your Task**

Carefully examine the input queries/code blocks and identify common themes, frequently requested information, and potential relationships with other tables. Pay attention to:

1. Specific columns or data points mentioned.
2. Filtering conditions or constraints.
3. Aggregations or calculations requested.
4. Potential joins with other tables.
5. Review the source code of the target table provided (`target_table_source_code`).

Based on your analysis, create 2-4 example SQL queries or Python scripts that demonstrate how to use the target table in both downstream batch tables and data extraction queries. Each example should:

1. Address a specific use case or information request.
2. Show appropriate filtering of columns.
3. Include joins with other tables if relevant.
4. Demonstrate proper SQL syntax and best practices.

* Feel free to create new examples that may not be directly related to the sample queries but showcase interesting use cases based on the target table's structure and available columns.

**Output Format**

Separate your output into two subsections:

- **Downstream Table/View**
  - Examples of how to use the target table when creating downstream tables or views. 
  - These can be used in batch source code or view table creation clauses.
  - Don't generate a complete SQL QUERY! Just give a part of SQL query or pyspark code blocks like how to join the target table with others.

- **Data Extraction**: Examples of how to use the target table for data extraction to fulfill specific requests.

Present your examples in the following markdown-formatted bullet point structure, with the code block indented under its own bullet:
<output_example>
### Downstream Table/View
- [Brief description of the use case]
    - ```sql/py
      [Your SQL query/Python code block here]
      ```
    - ```sql/py
      [Your SQL query/Python code block here]
      ```

### Data Extraction
- [Brief description of the use case]
    - ```sql/py
      [Your SQL query/Python code block here]
      ```
    - ```sql/py
      [Your SQL query/Python code block here]
      ```
</output_example>

The indentation level has to be strictly maintained for proper code execution.

Provide your entire markdown-formatted output within `<answer>` tags. The descriptions should be written in Korean.
한국어 결과는 존댓말이 아닌 단답으로 해줘. (예: "A 컬럼을 파티션 키로 활용합니다." 대신 "A 컬럼을 파티션 키로 활용")
Ensure that your examples cover a range of scenarios and query complexities. Include comments to explain key parts or reasoning behind certain choices.

**Additional Guidelines**

- Use aliases for table and column names when appropriate.
- Exclude all specific data in the query. For instance, substitute a specific artist name with "ARTIST" and a specific date with "2024-01-01".
- Include proper indentation and formatting in your code blocks for readability.
- If you make assumptions about table structures or relationships, briefly explain them in comments.
- Vary the complexity of your examples, from simple queries to more advanced ones involving subqueries or multiple joins.
- Remove overly simple or redundant queries.
- For SQL queries, write them in the style provided below and keep the SELECT statements concise rather than listing many columns on separate lines.
  - ```
    select `column_name1`, `column_name2`, sum(`column_name3`)
    from `catalog_name`.`db_name`.`table_name`
    where `column_name1` = `filter_condition`
    group by `column_name1`, `column_name2`
    order by `column_name1`, `column_name2`
    ```
- You may create new examples based on the structure and columns defined in the target table's source code.
- If there are fewer than 3 samples provided, avoid relying too heavily on them. Instead, focus more on the target table's structure and general data warehouse guidelines to create diverse and relevant examples.

Remember, the goal is to provide clear, practical examples that demonstrate effective use of the target table in SQL queries and Python scripts.
"""


DOWNSTREAM_TABLE_INFO_PROMPT = template ="""
You are tasked with creating a series of SQL queries or Python scripts usecases using a provided target table, based on given input queries and/or code blocks. Your goal is to demonstrate how to use the target table in SQL queries and Python code (mainly with PySpark), focusing on filtering specific columns and joining with other tables when necessary.

**Input Data**

The target table name is:
<target_table>
{target_table}
</target_table>

This is the source code for the target table:
<target_table_source_code>
{target_table_source_code}
</target_table_source_code>

You will be given a set of Python scripts for downstream (view)tables that use the target table as a source, as well as several data extraction samples for requests. These follow the rules:
- For Python scripts for downstream tables,
  - Python scripts for downstream tables are divided by table name (e.g., `==we_table==`).
  - The source code of each downstream table consists of multiple cells, each cell would have PySpark code or SQL code wrapped by PySpark. PySpark native code is used for additional data processing in many cases.

- An extract query or Python script consists of multiple cells, each cell has:
    - `cell_type`: indicates whether the cell is Python or SQL.
    - `cell_title`: (Optional) the cell title.
    - `role`: the role of the cell in the source code (e.g., code, heading, description, etc.).
    - `code`: the source code (a SQL query or a Python script).

Here are the Python scripts for downstream tables:
<downstream_table_source_code>
{downstream_table_source_code}
</downstream_table_source_code>

Here is a brief explanation and general guidelines for the Weverse Data Warehouse. Please review it and use it as a reference for the data tables.
<general_guide>
{general_guide}
</general_guide>

**Your Task**

Carefully examine the input queries/code blocks and identify common themes, frequently requested information, and potential relationships with other tables. Pay attention to:

1. Specific columns or data points mentioned.
2. Filtering conditions or constraints.
3. Aggregations or calculations requested.
4. Potential joins with other tables.
5. Review the source code of the target table provided (`target_table_source_code`).

Based on your analysis, list the downstream tables and view tables with a brief description of how it uses the target table as a source table.

**Output Format**

Separate your output into two subsections:

- **Downstream Table**
  - Breif description of the purpose of the table and how it uses the target table in whichever way.
  - Explains how the future user can use the table for data extraction to fulfill specific requests.

- **Downstream View tables**
  - Brief description of the purpose of the view table and how it uses the target table in whichever way.

You can skip Downstream Table or Downstream View tables if there are no relevant usecases.
Present your usecases in the following markdown-formatted bullet point structure, with the code block indented under its own bullet:
<output_example>
### Downstream Tables
- **[Table name 1]** : [Brief description of the table]
    - [A little bit more specific description and how it uses the target table as a source table]
    - [How the furture user can use the table for data extraction to fulfill specific requests]
- **[Table name 2]** : [Brief description of the table]
    - [A little bit more specific description and how it uses the target table as a source table]
    - [How the furture user can use the table for data extraction to fulfill specific requests]
- ...

### Downstream View Tables
- **[View table name 1]** : [Brief description of the view table]
    - [view table infomations]
- **[View table name 2]** : [Brief description of the view table]
    - [view table infomations]
- ...

</output_example>

The indentation level has to be strictly maintained for proper code execution.

Provide your entire markdown-formatted output within `<answer>` tags. The descriptions should be written in Korean.
한국어 결과는 존댓말이 아닌 단답으로 해줘. (예: "A 컬럼을 파티션 키로 활용합니다." 대신 "A 컬럼을 파티션 키로 활용")
Ensure that your usecases cover a range of scenarios and query complexities. Include comments to explain key parts or reasoning behind certain choices.

**Additional Guidelines**

- Use aliases for table and column names when appropriate.
- Include proper indentation and formatting in your code blocks for readability and markdown compiles.
- If you make assumptions about table structures or relationships, briefly explain them in comments.
- Remove overly simple or redundant queries.
- For SQL queries, write them in the style provided below and keep the SELECT statements concise rather than listing many columns in separate lines.
  - ```
    select `column_name1`, `column_name2`, sum(`column_name3`)
    from `catalog_name`.`db_name`.`table_name`
    where `column_name1` = `filter_condition`
    group by `column_name1`, `column_name2`
    order by `column_name1`, `column_name2`
    ```

Remember, the goal is to provide clear, concise usecases demonstrate effective use of the target table in Python scripts.
"""