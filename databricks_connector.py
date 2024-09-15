from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat
from configurations import *

## 데이터브릭스 코드 레포지토리 파일 포맷 변수들
HEADER_PATTERN = r"^#{1,6}\s.*$"
CELL_SEPARATOR_PY = "\n\n# COMMAND ----------\n\n"
CELL_SEPARATOR_SQL = "\n\n-- COMMAND ----------\n\n"
MAGIC_KEYWORD_PY = "# MAGIC"
MAGIC_KEYWORD_SQL = "-- MAGIC"
NOTEBOOK_PREFIX_PY = "# Databricks notebook source"
NOTEBOOK_PREFIX_SQL = "-- Databricks notebook source"

class DatabricksConnector(object):
    def __init__(self, max_hop = 3):
        self.server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        self.access_token = os.getenv("DATABRICKS_TOKEN")
        self.max_hop = max_hop
        self.downstream_table_list = [[] for _ in range(self.max_hop + 1)]
        self.upstream_table_list = [[] for _ in range(self.max_hop + 1)]
        self.w = WorkspaceClient(
            host = self.server_hostname,
            token = self.access_token
        )
        self.req_dict = {}

    
    def fetch_table(self, query, connection_name="default") -> pd.DataFrame:
        with sql.connect(server_hostname=self.server_hostname, http_path=self.http_path, access_token=self.access_token) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = [l.asDict() for l in cursor.fetchall()]
                cursor.close()
                connection.close()
            return pd.DataFrame(result)
            
    def get_desc_table(self, catalogName = "wev_prod", databaseName = 'default', tableName = 'test'):
        return self.fetch_table(f"DESC {catalogName}.{databaseName}.{tableName}")

    def get_batch_table(self, from_date, to_date, target_schema, target_table_type):
        table_q = f"""
        select distinct target_type, entity_type, source_type, source_table_full_name, source_table_schema, source_table_name, target_table_full_name, target_table_schema, target_table_name
        from system.access.table_lineage
        where 1=1
        and event_date between '{from_date}' and '{to_date}'
        and target_table_schema in ({target_schema})
        and target_type in ({target_table_type})
        """
        return self.fetch_table(table_q)

    def get_downstream_table_list(self, batch_table, hop, type, source_table, parent_node_id = None, verbose = False):
        if verbose:
            print(f"HOP : {hop if hop !=0 else 'BASE'}\t", f"{type}: {source_table}")
        if hop == 0:
            parent_node_id = 'base'
            self.downstream_table_list = [[] for _ in range(self.max_hop + 1)]
            self.downstream_table_list[hop].append(source_table.split(".")[-1])
        if hop > self.max_hop:
            return None
        else:
            downstream_tables = batch_table[batch_table['source_table_full_name'] == source_table]

            if len(downstream_tables) == 0:
                return None
            else:
                for i, row in downstream_tables.iterrows():
                    if hop + 1 <= self.max_hop : # and row['target_table_name'] not in exclude_tables:
                        # G.add_node(f'hop{hop + 1}_{i}', table_name = row['target_table_name'], hop = hop + 1)
                        self.downstream_table_list[hop + 1].append(f"{row['target_table_schema']}.{row['target_table_name']}")
                        # G.add_edge(parent_node_id, f'hop{hop + 1}_{i}')
                        self.get_downstream_table_list(batch_table = batch_table, hop = hop + 1, type=row['target_type'], source_table=row['target_table_full_name'], parent_node_id= f"hop{hop + 1}_{i}")
    
    def get_upstream_table_list(self, batch_table, hop, type, target_table, child_node_id = None, verbose = False):
        if verbose:
            print(f"HOP : {hop if hop !=0 else 'BASE'}\t", f"{type}: {target_table}")
        if hop == 0:
            child_node_id = 'base'
            self.upstream_table_list = [[] for _ in range(self.max_hop + 1)]
            self.upstream_table_list[hop].append(target_table.split(".")[-1])
        if hop > self.max_hop:
            return None
        else:
            upstream_tables = batch_table[batch_table['target_table_full_name'] == target_table]

            if len(upstream_tables) == 0:
                return None
            else:
                for i, row in upstream_tables.iterrows():
                    if hop + 1 <= self.max_hop : # and row['target_table_name'] not in exclude_tables:
                        # G.add_node(f'hop{hop + 1}_{i}', table_name = row['target_table_name'], hop = hop + 1)
                        self.upstream_table_list[hop + 1].append(f"{row['source_table_schema']}.{row['source_table_name']}")
                        # G.add_edge(parent_node_id, f'hop{hop + 1}_{i}')
                        self.get_upstream_table_list(batch_table = batch_table, hop = hop + 1, type=row['target_type'], target_table=row['source_table_full_name'], child_node_id= f"hop{hop + 1}_{i}")
    
    def get_request_queries(self):
        error_files = []
        pattern = r'DATA[-_]\d{4}'
        for i in self.w.workspace.list(f'/data-analytics/100.[JIRA] Source sharing/', recursive=False):
            dir_path = i.path
            if dir_path.split("/")[-1] in ["DATA-4XXX", "DATA-5XXX", "DATA-6XXX"]:
                print(f"Current Dir: {dir_path}")
                
                for notebook in self.w.workspace.list(path = dir_path, recursive=False):
                    # print(notebook.path)
                    notebook_title = notebook.path.split("/")[-1]
                    notebook_lang = notebook.language.name
                    issue_id = re.search(pattern, notebook_title).group().replace("_", "-")
                    try:
                        with self.w.workspace.download(notebook.path) as f:
                            content = f.read()
                            self.req_dict[issue_id] = (notebook_lang, content.decode('utf-8'))
                    except:
                        error_files.append(notebook.path.split("/")[-1]) # 노트북 제목을 포함한 경로상에 화투(, \x08)가 있는 제목은 에러가 남
        return self.req_dict
    
    @classmethod
    def get_formatted_blocks(cls, source_code_cells:str, source_code_lang:str = 'PYTHON', min_lines:int = None) -> dict:
        """
        Databricks 노트북의 소스 코드 셀을 포맷팅된 코드 블록으로 변환

        Args:
            source_code_cells (str): 소스 코드 전문
            source_code_lang (str): 소스 코드의 언어 ('SQL' 또는 'PYTHON')
            min_lines (int): (SQL 셀의 경우) 셀의 "\n"로 구분되는 하한 line 임계값

        Returns:
            dict: 포맷팅된 코드 블록들의 딕셔너리

        Description:
            1. 소스 코드를 셀 단위로 분리
            2. 각 셀에 대해 다음 정보를 추출
                - cell_type: 셀의 타입 (예: 'sql', 'py', 'md')
                - cell_title: 셀의 제목 (있는 경우)
                - role: 셀의 역할 ('basic_info', 'setting', 'code', 'description' 등)
                - codes: 셀의 실제 코드 내용
            3. 추출된 정보를 딕셔너리 형태로 반환
        """
        code_blocks = dict()

        if source_code_lang == 'SQL': # SQL 타입의 노트북: 추출 쿼리 or 인사이트 뷰 생성 쿼리
            default_cell_type = 'sql'
            lines = source_code_cells.split(CELL_SEPARATOR_SQL) #셀 별로 나누기
            lines[0] = lines[0].strip("-- Databricks notebook source")
            title_block = "-- DBTITLE"
            MAGIC_KEYWORD = MAGIC_KEYWORD_SQL
            if lines[0][0] == "\n":
                lines[0] = lines[0][1:]

            if min_lines is not None:
                lines = [l for l in lines if len(l.split("\n")) >= min_lines] ## 해당 셀의 linebreak가 N개 이상, 즉 N줄 이상으로 이루어진 경우에만 참조 예시 코드로 포함시키기

        else : # source_code_lang == 'PYTHON'
            default_cell_type = 'py'
            lines = source_code_cells.split(CELL_SEPARATOR_PY)
            lines[0] = lines[0].strip("# Databricks notebook source")
            title_block = "# DBTITLE"
            MAGIC_KEYWORD = MAGIC_KEYWORD_PY
            if lines[0][0] == "\n":
                lines[0] = lines[0][1:]
            

        for i, cell in enumerate(lines):

            ## CELL TITLE 처리
            if cell.startswith(title_block): # 셀에 title이 있는 경우
                cell = cell.strip(title_block)
                cell_title = ",".join(cell.split(",")[1:]).split("\n")[0]
                # print(cell)
                cell = ",".join(cell.split(",")[1:]).strip(cell_title + "\n")
            else:
                cell_title = None

            ## MAGIC COMMAND 처리
            if cell[:len(MAGIC_KEYWORD)] == MAGIC_KEYWORD: # 메직 커멘드가 적용된 셀 처리
                start_idx = cell.index("%") + 1
                end_idx = cell.index("\n")
                cell_type = cell[start_idx:end_idx] # 언어 값을 파싱
                codes = cell[end_idx + 1:].replace(MAGIC_KEYWORD, "") # 나머지 MAGIC들은 지워줌
            else: # 매직 커멘드가 적용 안된 셀은 기본 언어로 설정
                cell_type = default_cell_type
                codes = cell


            ## BASIC INFO, 기본 파이썬 세팅 등 처리
            if cell_type == 'md':
                codes = "\n".join([line[1:] for line in codes.split("\n")])
                if "BASIC INFO" in codes.upper():
                    role = "basic_info"
                elif re.fullmatch(HEADER_PATTERN, codes):
                    role = "just_heading"
                else:
                    role = "description"
            elif cell_type == 'py':
                if any(code in codes for code in ['run_mode = dbutils.widgets.get("run_mode")', "use catalog"]):
                    role = "setting"
                # elif all([l[0] if l[0] == "#" else None for l in codes.split("\n") if len(l) > 0]):
                #     role = "description"
                else:
                    role = "code"
            elif cell_type == 'sql':
                role = "code"
            else:
                role = "etc"

            ## 파싱된 셀을 code_blocks에 저장(셀타입, 셀 타이틀(있는 경우), 셀 역할, 코드)
            code_blocks[i + 1] = (cell_type, cell_title, role, codes)
        return code_blocks
                    
## TEST
# KEY_DIR = "/Users/lymansong/Documents/GitHub/keys"
# from dotenv import load_dotenv
# print(load_dotenv(dotenv_path= os.path.join(KEY_DIR, ".env")))
# db_connector = DatabricksConnector(max_hop = 2)

# batch_table = db_connector.get_batch_table(
#     from_date = '2024-09-03',
#     to_date = '2024-09-04',
#     target_schema = "'we_mart'",
#     target_table_type = "'TABLE'"
# )

# db_connector.get_downstream_table_list(batch_table, hop=0, type='TABLE', source_table='wev_prod.we_mart.we_user')
# print(db_connector.downstream_table_list[1])
# db_connector.get_downstream_table_list(batch_table, hop=0, type='TABLE', source_table='wev_prod.we_mart.ws_album_sale', verbose = True)
# print(db_connector.downstream_table_list[1])
# db_connector.get_upstream_table_list(batch_table, hop=0, type='TABLE', target_table='wev_prod.we_mart.we_user')


# import pickle
# import os
# from configurations import *
# def load_dictionary(filepath):
#     with open(filepath, 'rb') as file:
#         loaded_data = pickle.load(file)
#     return loaded_data

# _dataset_dict = load_dictionary(os.path.join(REQ_DIR, '_dataset_dict.pkl'))
# dataset = sorted(_dataset_dict['data'], key=lambda x: x['issue_id'], reverse= True)
# # for i, data in  enumerate(dataset):
# #     print(i, data['source_code_lang'], data['issue_id'], data['issue_title'])
    

# from databricks_connector import DatabricksConnector
# sample_idx = 9
# print(dataset[sample_idx]['issue_id'])
# source_code_cells, source_code_lang = dataset[sample_idx]['source_code'], dataset[sample_idx]['source_code_lang']
# code_blocks_2 = DatabricksConnector.get_formatted_blocks(source_code_cells, source_code_lang)
# code_blocks_2