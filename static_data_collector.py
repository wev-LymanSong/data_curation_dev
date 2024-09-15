import os
import ast
from markdown_it import MarkdownIt
from mdutils.mdutils import MdUtils
from itertools import zip_longest
from github_repo_connector import *
from databricks_connector import *
from configurations import *

KEY_DIR = "/Users/lymansong/Documents/GitHub/keys"
BASE_DIR = "/Users/lymansong/Documents/GitHub/mtms"
os.chdir(BASE_DIR)
DATA_DIR = os.path.join(BASE_DIR, "data")
SPEC_DIR = os.path.join(DATA_DIR, "specs")
SOURCECODE_DIR = os.path.join(DATA_DIR, "source_codes")

REPO_DIR = '/Users/lymansong/Documents/GitHub/databricks'
CODE_DIR = os.path.join(REPO_DIR, "src/data_analytics")
WE_MART_DIR = os.path.join(CODE_DIR, "mart/we_mart")
WE_META_DIR = os.path.join(CODE_DIR, "meta/we_meta")
WE_STAT_DIR = os.path.join(CODE_DIR, "stats/we_mart")
WI_VIEW_DIR = os.path.join(CODE_DIR, "stats/wi_view")

field2dir_dict =  {
    'we_mart' : WE_MART_DIR, 
    'we_meta' : WE_META_DIR, 
    'we_stats' : WE_STAT_DIR, 
    'wi_view' : WI_VIEW_DIR, 
}
TARGET_FIELD =  'we_mart'
TARGET_DB =     'we_mart'
TARGET_TABLE =  'ws_fc_user_history'
md = MarkdownIt()

gc_databricks = GithubConnector(github_token=os.environ['GITHUB_TOKEN'], repo_name= 'databricks', branch='main')
gc_airflow = GithubConnector(github_token=os.environ['GITHUB_TOKEN'], repo_name='dp-airflow', branch='main')
db_connector = DatabricksConnector(max_hop = 1)

mdFile = MdUtils(file_name=TARGET_TABLE, title=f"{TARGET_DB}.{TARGET_TABLE}")

def safe_exec(code_str):
    local_vars = {}
    global_vars = globals().copy()

    try:
        # 코드 문자열을 AST로 파싱
        tree = ast.parse(code_str)
        
        # AST의 각 노드(문장)를 순회하며 실행
        for node in tree.body:
            try:
                # 노드를 다시 코드로 변환
                stmt = ast.unparse(node)
                exec(stmt, global_vars, local_vars)
            except Exception as e:
                print(f"Error in statement '{stmt}': {e}")
                # 여기서 에러 로깅을 추가할 수 있습니다.
                continue  # 다음 문장으로 계속 진행
    except SyntaxError as e:
        print(f"Syntax error in the entire code: {e}")
        # 전체 코드의 구문 오류 처리

    return local_vars

#==========================================Source Fetching================================
# Fetch and format code blocks from the target database and table
## file load
with open(os.path.join(field2dir_dict[TARGET_FIELD], TARGET_TABLE + ".py"), "rb") as f:
    source_code_cells = "".join([l.decode() for l in f.readlines()])
    source_code_lang = 'PYTHON' if source_code_cells.startswith(NOTEBOOK_PREFIX_PY) else "SQL"
    f.close()

code_blocks = DatabricksConnector.get_formatted_blocks(
    source_code_cells=source_code_cells, 
    source_code_lang=source_code_lang
)
code_df = GithubConnector.to_df(code_blocks=code_blocks)
code_df.to_csv(os.path.join(SOURCECODE_DIR, f"{TARGET_TABLE}.csv"))

# Get basic info data from the fetched code block
basic_info = code_df[code_df['role'] == 'basic_info']['codes'].item()
tokens = md.parse(basic_info)

# Get table, option setting config and variables from code block
settings = code_df[(code_df["codes"].str.contains('dbutils.widgets.get'))]['codes'].item()

## setting 코드 중 'key', 'run_mode', 'slack_token', 'channel'로 시작하는 줄은 제거
settings = "\n".join([l for l in settings.split("\n") if l.startswith(('key', 'run_mode', 'slack_token', 'channel', 'from', 'import')) == False])
settings = "key = None" + settings # key라는 변수를 더미로 넣어주기: 이후 코드에서 key를 가지고 연산 및 처리하는 경우가 많음
settings = safe_exec(settings) # 에러나는 구문은 무시하고 실행, 변수로 저장

# Get file change history for the target table, make it into a DataFrame
change_df = gc_databricks.get_file_change_history(file_path=os.path.join(field2dir_dict[TARGET_FIELD], TARGET_TABLE), verbose = False)
author_counts = change_df['Author'].value_counts().reset_index()

# Get table schema from databricks sql connection
desc_df = db_connector.get_desc_table(catalogName='wev_prod', databaseName=TARGET_DB, tableName = TARGET_TABLE)
part_indices = desc_df[desc_df['col_name'] == "# Partition Information"].index

# Get Airflow Dag-task mapping dataframe
dag_task_df = pd.DataFrame(columns=['owner', 'dag_id', 'batch_interval', 'step', 'field', 'db_name', 'table_name', 'file_path'])
for dag in gc_airflow.get_dag_list():
    owner, dag_id, task_list = gc_airflow.get_task_list(dag)
    if 'test' in dag_id:
        continue
    interval = dag_id.split("_")[-1].upper()
    rows = []
    for step in task_list:
        for task in step['list']:
            cur_field = 'we_' + task.split("/")[-3]
            cur_db = task.split('/')[-2]
            cur_table = task.split("/")[-1]
            rows.append({'owner' : owner, 'dag_id': dag_id, 'batch_interval': interval, 'step': step["step"], 'field' : cur_field, 'db_name': cur_db, 'table_name':cur_table, 'file_path': task})
    dag_task_df = pd.concat([dag_task_df, pd.DataFrame(rows)], ignore_index=True)


#==========================================Template Fill-in============================

## BASIC INFO
mdFile.new_header(level=1, title='BASIC INFO')
ABOUT = " "

dag_items = dag_task_df[(dag_task_df["field"] == TARGET_FIELD) & (dag_task_df["table_name"] == TARGET_TABLE)]
dag_names = dag_items['dag_id'].values.tolist()
PRIORITY = "PRIMARY" if dag_names[0] in PRIMARY_DAG else "SECONDARY"
TABLE_TYPE = f"{TARGET_FIELD.split("_")[-1].upper()} {PRIORITY}"
PARTITIONED_BY = ", ".join([f"`{i}`" for i in desc_df.iloc[part_indices.item() + 2:]['col_name'].to_list()]) if len(part_indices) != 0 else " "
CREATED_BY = COLLABORATOR_DICT[change_df.iloc[len(change_df) - 1]['Author']]
LAST_UPDATED_BY = COLLABORATOR_DICT[change_df.iloc[0]['Author']]
# MANAGED_BY = COLLABORATOR_DICT[change_df['Author'].value_counts().index[0]] # most frequent author 
CREATED_AT = change_df.iloc[len(change_df) - 1]['Date'].split("T")[0]
LAST_UPDATED_AT = change_df.iloc[0]['Date'].split("T")[0]
COLLABORATORS = ', '.join([f"{COLLABORATOR_DICT[author]}[{count}]" for author, count in zip(author_counts['Author'], author_counts['count'])])

header = ['**About**', ABOUT]
rows = [
    ['**Database**', f'**{TARGET_DB}**'],
    ['**Table Type**', TABLE_TYPE],
    ['**Partitioned by**', f'{PARTITIONED_BY}'],
    ['**Created/ Last Updated At**', f'{CREATED_AT} / {LAST_UPDATED_AT}'],
    ['**Created By**', CREATED_BY],
    ['**Last Updated By**', LAST_UPDATED_BY],
    ['**Collaborators**', COLLABORATORS],
]
mdFile.new_table(columns=len(header), rows=len(rows) + 1, text=header + sum(rows, []), text_align='left')

## CHANGE HISTORY
mdFile.new_line("#### Change History")
change_history_header = ['**Date**', '**By**', '**LINK**']
change_df_tmp = change_df[['Date', 'Author', 'URL']]
change_df_tmp['Date'] = change_df_tmp['Date'].apply(lambda x: x.split("T")[0])
change_df_tmp['Author'] = change_df_tmp['Author'].apply(lambda x: COLLABORATOR_DICT[x])
change_df_tmp['URL'] = change_df_tmp['URL'].apply(lambda x: f"[PR]({x})")
change_history_rows = change_df_tmp.sort_values(["Date"]).values.tolist()
mdFile.new_table(columns=len(change_history_header), rows=len(change_history_rows) + 1, text=change_history_header + sum(change_history_rows, []), text_align='left')
mdFile.new_line("  ")

## TALBE NOTICE
mdFile.new_header(level=1, title='TABLE NOTICE')
mdFile.new_line("  ")
mdFile.new_line("---")

## COLUMN INFO
mdFile.new_header(level=1, title='COLUMN INFO')
COLUMNS = desc_df.fillna(" ")
header = ['#', "Column Name", "Data Type", "Comment"]
rows = []
if len(part_indices) > 0:
    for i, r in COLUMNS.iloc[:part_indices.item()].iterrows():
        rows.append([i] + r.to_list())
else:
    for i, r in COLUMNS.iterrows():
        rows.append([i] + r.to_list())
mdFile.new_table(columns=len(header), rows=len(rows) + 1, text=header + sum(rows, []), text_align='left')
mdFile.new_line("  ")
mdFile.new_line("---")

## HOW TO USE
mdFile.new_header(level=1, title='HOW TO USE')
mdFile.new_line("  ")
mdFile.new_line("---")

## PIPELINE INFO
mdFile.new_header(level=1, title='PIPELINE INFO')
## BATCH INFO
mdFile.new_header(level=2, title='⌛️ BATCH')
if len(dag_items) > 1:
    DAG = ", ".join([f"`{dag}`" for dag in dag_names])
    UPDATE_INTERVAL = ", ".join(dag_items['batch_interval'].unique().tolist())
else:
    DAG = f"`{dag_names[0]}`"
    UPDATE_INTERVAL = dag_items['batch_interval'].item()
UPDATE_TYPE = settings['option']['mode'].upper()
mdFile.new_header(level = 3, title = f"DAG: {DAG}")
mdFile.new_header(level = 3, title = f"Update Interval: {UPDATE_INTERVAL}")
mdFile.new_header(level = 3, title = f"Update Type: {UPDATE_TYPE}")

mdFile.new_line("  ")

## LOCATIONs
mdFile.new_header(level=2, title='📍 LINK URLs')
GITHUB = gc_databricks.switch_table_dir_to_url(os.path.join(field2dir_dict[TARGET_FIELD], TARGET_TABLE))
mdFile.new_header(level = 3, title = 'Github: ' + mdFile.new_inline_link(link=GITHUB, text='Source Code')) 

if len(dag_items) > 1: # DAG가 여러개일 경우
    mdFile.new_header(level = 3, title = 'Airflow DAGs')
    dag_urls = [f"[{dag}]({gc_airflow.switch_dag_dir_to_url(dag)})" for dag in dag_items['dag_id'].tolist()]
    mdFile.new_list(dag_urls)
else: # DAG가 하나일 경우
    AIRFLOW = gc_airflow.switch_dag_dir_to_url(dag_items['dag_id'].item())
    mdFile.new_header(level = 3, title = 'Airflow: ' + mdFile.new_inline_link(link=GITHUB, text='DAG')) 

mdFile.new_line("  ")
mdFile.new_line("---")

## DEPENDENCIES
mdFile.new_header(level=1, title='DEPENDENCIES')
mdFile.new_header(level=2, title='👨‍👩‍👧‍👦 Up/Downstream Table List')
batch_table = db_connector.get_batch_table(
    from_date = '2024-08-01',
    to_date = '2024-09-07',
    target_schema = "'we_mart'",
    target_table_type = "'TABLE'"
)
db_connector.get_upstream_table_list(batch_table, hop=0, type='TABLE', target_table=f'wev_prod.{TARGET_DB}.{TARGET_TABLE}')
db_connector.get_downstream_table_list(batch_table, hop=0, type='TABLE', source_table=f'wev_prod.{TARGET_DB}.{TARGET_TABLE}')
UPSTREAM_TABLES = sorted(set(db_connector.upstream_table_list[1]))
DOWNSTREAM_TABLES = sorted(set(db_connector.downstream_table_list[1]))

header = ['Upstream Tables', 'Downstream Tables']
rows = []
for i, j in zip_longest(UPSTREAM_TABLES, DOWNSTREAM_TABLES):
    rows.append([i if i is not None else ' ', j if j is not None else ' '])

mdFile.new_table(columns=len(header), rows=len(rows) + 1, text=header + sum(rows, []), text_align='left')


# ### SOURCE TABLE INFO
# mdFile.new_header(level=2, title='🐔 Upstream Tables Info')
# mdFile.new_line("  ")
### TARGET TABLE INFO
mdFile.new_header(level=2, title='🐤 Downstream Tables Info')
mdFile.new_line("  ")
mdFile.new_line("---")


## RELATED PAGES




# File save
mdFile.new_line("---")
os.chdir(SPEC_DIR)
mdFile.create_md_file()

