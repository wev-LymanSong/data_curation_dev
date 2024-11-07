from configurations import *
from static_data_collector import StaticDataCollector
from semantic_info_generator import SemanticInfoGenerator
from tools.utils.md_table_generator import MdTableGenerator
from mdutils.mdutils import MdUtils
from itertools import zip_longest
from functools import wraps
from datetime import datetime, timedelta
import time
import warnings
import re

warnings.filterwarnings("ignore")

def convert_dtype(dtype):
    dtype_pattern = r'^(array|map)<(struct<[^>]+>|[^<>]+)>$|^[a-zA-Z]+$'
    match = re.search(dtype_pattern, dtype)
    if match:
        result = match.group(0)
        if 'struct' in result:
            result = result.split('<')[0] + '<struct>'
        return result
    else:
        return dtype

def safe_access(default_value=""):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"An error occurred: {e}")  # 디버깅을 위해 에러 출력 (필요 없으면 제거)
                return default_value
        return wrapper
    return decorator

def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            error_message = str(e)
            print(f"Error in {func.__name__}{'(' + args[1] + ')' if len(args) > 1 else ''}")
            print(f"\t{error_message}")
            return "None"
        end = time.time()
        elapsed_time = end - start
        # print(args)
        if len(args) > 1: 
            print(f"{func.__name__}({args[1]}) 실행 시간: {elapsed_time:.2f}초")
        else:
            print(f"{func.__name__} 실행 시간: {elapsed_time:.2f}초")
        return result
    
    return wrapper

class SpecificationBuilder(object):
    
    #class variables
    dag_task_df = StaticDataCollector.get_dag_task_df()
    from_date = (datetime.today() - timedelta(days = 90)).strftime('%Y-%m-%d')
    to_date = datetime.today().strftime('%Y-%m-%d')

    batch_df = StaticDataCollector.db_connector.get_batch_table(
        from_date = from_date,
        to_date = to_date,
        target_schema = "'we_mart', 'we_meta'",
        target_table_type = "'TABLE', 'VIEW'"
    )

    def __init__(self, target_table):
        
        dag_rows = SpecificationBuilder.dag_task_df[SpecificationBuilder.dag_task_df['table_name'] == target_table]
        batch_row = SpecificationBuilder.batch_df[SpecificationBuilder.batch_df['target_table_name'] == target_table]
        
        if dag_rows.empty and batch_row.empty:
            raise ValueError(f"No information found for the target table: {target_table}")
        elif dag_rows.empty: # No DAG for the target table
            target_table_info = batch_row.iloc[0] # take the first row in case of multiple tasks
            self.TARGET_DB = target_table_info['target_table_schema']
            if self.TARGET_DB == 'we_mart':
                self.TARGET_FIELD = 'we_stat' if target_table.startswith("stats") else 'we_mart'
            else:
                self.TARGET_FIELD = self.TARGET_DB
        
        else:
            target_table_info = dag_rows.iloc[0] # take the first row in case of multiple tasks
            self.TARGET_FIELD = target_table_info['field']
            self.TARGET_DB    = target_table_info['db_name']
        
        self.TARGET_TABLE = target_table
        self.mdFile = None
        
        # Static DataFrames and info dicts: Get static data associated with the target table from StaticDataCollector.
        self.sdc = StaticDataCollector(self.TARGET_FIELD, self.TARGET_DB, self.TARGET_TABLE)
        self.code_df = self.sdc.get_code_blocks()
        self.basic_info, self.settings = self.sdc.get_basic_info_settings(code_df=self.code_df)
        self.change_df, self.author_counts = self.sdc.get_file_change_history()
        self.desc_df, self.part_indices = self.sdc.get_table_schema()

        # Semantic DataFrames and info dicts: Get semantic data associated with the target table from SemanticInfoGenerator.
        self.sig = SemanticInfoGenerator(
            target_table=self.TARGET_TABLE,
            dag_task_df = SpecificationBuilder.dag_task_df,
            batch_df = SpecificationBuilder.batch_df,
            gc = StaticDataCollector.gc_databricks
        )

        # Initiate sepcification Features
        self.basic_info = None
        self.change_history = None
        self.table_notice = "No content."
        self.column_info = None
        self.how_to_use = "No content."
        self.batch_info = None
        self.locations = None
        self.dep_table_list = None
        # self.dep_down_table_info = "No content."
    
    @timer
    def collect_static_data(self):
        # BASIC INFO
        dag_items = SpecificationBuilder.dag_task_df[(SpecificationBuilder.dag_task_df["field"] == self.TARGET_FIELD) & (SpecificationBuilder.dag_task_df["table_name"] == self.TARGET_TABLE)]

        if dag_items.empty:
            dag_names = ["No DAG"]
            PRIORITY = 'VIEW TABLE or NOT SCHEDULED'
        else:
            dag_names = dag_items['dag_id'].values.tolist()
            PRIORITY = "PRIMARY" if dag_names[0] in PRIMARY_DAG else "SECONDARY"


        TABLE_TYPE = f"{self.TARGET_FIELD.split('_')[-1].upper()} {PRIORITY}"
        PARTITIONED_BY = ", ".join([f"`{i}`" for i in self.desc_df.iloc[self.part_indices.item() + 2:]['col_name'].to_list()]) if len(self.part_indices) != 0 else " "
        CREATED_BY = COLLABORATOR_DICT[self.change_df.iloc[len(self.change_df) - 1]['Author']]
        LAST_UPDATED_BY = COLLABORATOR_DICT[self.change_df.iloc[0]['Author']]
        CREATED_AT = self.change_df.iloc[len(self.change_df) - 1]['Date'].split("T")[0]
        LAST_UPDATED_AT = self.change_df.iloc[0]['Date'].split("T")[0]
        COLLABORATORS = ', '.join([f"{COLLABORATOR_DICT[author]}[{count}]" for author, count in zip(self.author_counts['Author'], self.author_counts['count'])])

        basic_info_header = ['**About**', " 담당자 수기 입력 필요 "]
        basic_info_rows = [
            ['**Database**', f'**{self.TARGET_DB}**'],
            ['**Table Type**', TABLE_TYPE],
            ['**Partitioned by**', f'{PARTITIONED_BY}'],
            ['**Created/ Last Updated At**', f'{CREATED_AT} / {LAST_UPDATED_AT}'],
            ['**Created By**', CREATED_BY],
            ['**Last Updated By**', LAST_UPDATED_BY],
            ['**Collaborators**', COLLABORATORS],
        ]

        self.basic_info = (basic_info_header, basic_info_rows)

        ## CHANGE HISTORY
        change_history_header = ['**Date**', '**By**', '**LINK**']
        change_df_tmp = self.change_df[['Date', 'Author', 'URL']]
        change_df_tmp['Date'] = change_df_tmp['Date'].apply(lambda x: x.split("T")[0])
        change_df_tmp['Author'] = change_df_tmp['Author'].apply(lambda x: COLLABORATOR_DICT[x])
        change_df_tmp['URL'] = change_df_tmp['URL'].apply(lambda x: f"[PR]({x})")
        change_history_rows = change_df_tmp.sort_values(["Date"]).values.tolist()
        self.change_history = (change_history_header, change_history_rows)

        ## COLUMN INFO
        COLUMNS = self.desc_df.fillna(" ")
        COLUMNS['data_type'] = COLUMNS['data_type'].apply(convert_dtype)
        column_info_header = ['#', "Column Name", "Data Type", "Comment"]
        column_info_rows = []
        if len(self.part_indices) > 0:
            for i, r in COLUMNS.iloc[:self.part_indices.item()].iterrows():
                column_info_rows.append([i] + r.to_list())
        else:
            for i, r in COLUMNS.iterrows():
                column_info_rows.append([i] + r.to_list())
        self.column_info = (column_info_header, column_info_rows)

        ## PIPELINE INFO
        ### BATCH INFO
        if dag_items.empty:
            DAG = "`No DAG`"
            UPDATE_INTERVAL = "N/A"
            UPDATE_TYPE = 'N/A'
        elif len(dag_items) > 1:
            DAG = ", ".join([f"`{dag}`" for dag in dag_names])
            UPDATE_INTERVAL = ", ".join(dag_items['batch_interval'].unique().tolist())
            UPDATE_TYPE = self.settings['option']['mode'].upper() if self.settings is not None else " "
        else:
            DAG = f"`{dag_names[0]}`"
            UPDATE_INTERVAL = dag_items['batch_interval'].item()
            UPDATE_TYPE = self.settings['option']['mode'].upper() if self.settings is not None else " "
        self.batch_info = (DAG, UPDATE_INTERVAL, UPDATE_TYPE)
        ### LOCATIONs
        try:
            target_path = field2dir_dict[self.TARGET_FIELD]
        except:
            target_path = os.path.join(CODE_DIR, self.TARGET_FIELD)
        GITHUB = StaticDataCollector.gc_databricks.switch_table_dir_to_url(os.path.join(target_path, self.TARGET_TABLE))
        
        if dag_items.empty:
            AIRFLOW = " "
        elif len(dag_items) > 1: # DAG가 여러개일 경우
            AIRFLOW = [f"[{dag}]({StaticDataCollector.gc_airflow.switch_dag_dir_to_url(dag)})" for dag in dag_items['dag_id'].tolist()]
        else: # DAG가 하나일 경우
            AIRFLOW = [StaticDataCollector.gc_airflow.switch_dag_dir_to_url(dag_items['dag_id'].item())]
        self.locations = (GITHUB, AIRFLOW)

        ## DEPENDENCIES
        StaticDataCollector.db_connector.get_upstream_table_list(SpecificationBuilder.batch_df, hop=0, type='TABLE', target_table=f'wev_prod.{self.TARGET_DB}.{self.TARGET_TABLE}')
        StaticDataCollector.db_connector.get_downstream_table_list(SpecificationBuilder.batch_df, hop=0, type='TABLE', source_table=f'wev_prod.{self.TARGET_DB}.{self.TARGET_TABLE}')
        UPSTREAM_TABLES = sorted(set(StaticDataCollector.db_connector.upstream_table_list[1]))
        DOWNSTREAM_TABLES = sorted(set(StaticDataCollector.db_connector.downstream_table_list[1]))
        # DOWNSTREAM_VIEWS = sorted(set(db_connector.downstream_table_list[2]))
        dep_tl_header = ['Upstream Tables', 'Downstream Tables']
        dep_tl_rows = []
        for i, j in zip_longest(UPSTREAM_TABLES, DOWNSTREAM_TABLES):
            dep_tl_rows.append([i if i is not None else ' ', j if j is not None else ' '])
        self.dep_table_list = (dep_tl_header, dep_tl_rows)
    
    @timer
    def generate_semantic_data(self, target_section):
        if target_section == "TABLE_NOTICE":
            self.table_notice = self.sig.get_table_notice()
        elif target_section == 'HOW_TO_USE':
            self.how_to_use = self.sig.get_how_to_use()
        # elif target_section == 'DOWNSTREAM_TABLE_INFO':
        #     self.dep_down_table_info = self.sig.get_downstream_table_info()
            
    
    @timer
    def save_mdfile(self, target_dir):
        self.mdFile = MdUtils(file_name=self.TARGET_TABLE, title=f"{self.TARGET_DB}.{self.TARGET_TABLE}")
        ## BASIC INFO
        self.mdFile.new_header(level=1, title='BASIC INFO')
        basic_info_header, basic_info_rows = self.basic_info
        self.mdFile.new_table(columns=len(basic_info_header), rows=len(basic_info_rows) + 1, text=basic_info_header + sum(basic_info_rows, []), text_align='left')

        #### CHANGE HISTORY
        self.mdFile.new_line("#### Change History")
        change_history_header, change_history_rows = self.change_history
        self.mdFile.new_table(columns=len(change_history_header), rows=len(change_history_rows) + 1, text=change_history_header + sum(change_history_rows, []), text_align='left')
        self.mdFile.new_line("  ")
        
        ## TALBE NOTICE
        self.mdFile.new_header(level=1, title='TABLE NOTICE')
        self.mdFile.new_line(self.table_notice)
        self.mdFile.new_line("---")

        ## COLUMN INFO
        self.mdFile.new_header(level=1, title='COLUMN INFO')
        column_info_header, column_info_rows = self.column_info
        self.mdFile.new_table(columns=len(column_info_header), rows=len(column_info_rows) + 1, text=column_info_header + sum(column_info_rows, []), text_align='left')
        self.mdFile.new_line("  ")
        self.mdFile.new_line("---")

        ## HOW TO USE
        self.mdFile.new_header(level=1, title='HOW TO USE')
        self.mdFile.new_line(self.how_to_use)
        self.mdFile.new_line("---")

        ## PIPELINE INFO
        self.mdFile.new_header(level=1, title='PIPELINE INFO')
        ### BATCH INFO
        self.mdFile.new_header(level=2, title='⌛️ BATCH')
        batch_info_dag, batch_info_update_interval, batch_info_update_type = self.batch_info
        self.mdFile.new_header(level = 3, title = f"DAG: {batch_info_dag}")
        self.mdFile.new_header(level = 3, title = f"Update Interval: {batch_info_update_interval}")
        self.mdFile.new_header(level = 3, title = f"Update Type: {batch_info_update_type}")

        ### LOCATIONs
        self.mdFile.new_header(level=2, title='📍 LINK URLs')
        source_code, aiflow_dag = self.locations
        self.mdFile.new_header(level = 3, title = 'Github: ' + self.mdFile.new_inline_link(link=source_code, text='Source Code')) 

        if len(aiflow_dag) > 1: # DAG가 여러개일 경우
            self.mdFile.new_header(level = 3, title = 'Airflow DAGs')
            self.mdFile.new_list(aiflow_dag)
        else: # DAG가 하나일 경우
            self.mdFile.new_header(level = 3, title = 'Airflow: ' + self.mdFile.new_inline_link(link=aiflow_dag[0], text='DAG')) 
        self.mdFile.new_line("  ")
        self.mdFile.new_line("---")

        ## DEPENDENCIES
        self.mdFile.new_header(level=1, title='DEPENDENCIES')
        self.mdFile.new_header(level=2, title='👨‍👩‍👧‍👦 Up/Downstream Table List')
        dep_tl_header, dep_tl_rows = self.dep_table_list
        self.mdFile.new_table(columns=len(dep_tl_header), rows=len(dep_tl_rows) + 1, text=dep_tl_header + sum(dep_tl_rows, []), text_align='left')

        ### DOWN TABLE INFO
        # self.mdFile.new_header(level=2, title='🐤 Downstream Tables Info')
        # self.mdFile.new_line(self.dep_down_table_info)
        # self.mdFile.new_line("---")

        # File save
        os.chdir(target_dir)
        self.mdFile.create_md_file()
        os.chdir(BASE_DIR)

    
    def print_table_info(self):
        print(f"Current Target Table:({self.self.TARGET_FIELD.upper()}) {self.TARGET_DB}.{self.TARGET_TABLE:<20}")
    
    @timer
    def read_mdfile(self, source_dir):
        target_table_dir = os.path.join(source_dir, self.TARGET_TABLE + ".md").replace("\\", "/")

        with open(target_table_dir, "r", encoding="utf-8") as file:
            content = file.read()
        sections = {
            'BASIC INFO': None,
            'Change History': None,
            'TABLE NOTICE': " ",
            'COLUMN INFO': None,
            'HOW TO USE': " ",
            'PIPELINE INFO': None,
            'DEPENDENCIES': None
        }

        current_section = None
        section_content = []

        for line in content.split('\n'):
            if line.startswith('# ') or line.strip() == "#### Change History":
                if current_section:
                    sections[current_section] = '\n'.join(section_content).strip()
                current_section = line.split("# ")[-1].strip()
                section_content = []
            elif current_section:
                if line == "---":
                    continue
                section_content.append(line)

        if current_section:
            sections[current_section] = '\n'.join(section_content).strip()

        self.basic_info = MdTableGenerator.parse_markdown_table(sections['BASIC INFO'])
        self.change_history = MdTableGenerator.parse_markdown_table(sections['Change History'])
        self.table_notice = sections['TABLE NOTICE']
        column_info = MdTableGenerator.parse_markdown_table(sections['COLUMN INFO'])
        rows = [[int(row[0])] + row[1:] for row in column_info[1]]

        for row in rows:
            if len(row) < 4:
                row.append("")
                
        self.column_info = (column_info[0], rows)
        self.how_to_use = sections['HOW TO USE']
        batch_info = sections['PIPELINE INFO']

        # DEPENDENCIES 섹션 파싱
        dependencies = sections['DEPENDENCIES']
        dep_table_list = None
        # dep_down_table_info = " "

        if dependencies:
            dep_sections = re.split(r'##\s+', dependencies)
            for section in dep_sections:
                if section.startswith('👨‍👩‍👧‍👦 Up/Downstream Table List'):
                    dep_table_list = section.split('\n', 1)[1].strip()
                # elif section.startswith('🐤 Downstream Tables Info'):
                #     try:
                #         dep_down_table_info = section.split('\n', 1)[1].strip()
                #     except:
                #         dep_down_table_info = " "

        # PIPELINE INFO 섹션에서 locations 정보 추출
        locations = None
        
        if batch_info:
            location_match = re.search(r'## 📍 LINK URLs\n\n(.*?)\n\n', batch_info, re.DOTALL)
            if location_match:
                locations = location_match.group(1)
        ls = [l.strip("\n").strip() for l in batch_info.split("##") if l != '']
        
        batch_infos = None
        link_pattern = r'\[(.*?)\]\((.*?)\)'
        for i, l in enumerate(ls):
            if l.startswith("#") == False:
                if i == 0:
                    1==1
                else:
                    batch_infos = keywords
                keywords = []
            else:
                if batch_infos is None:
                    keywords.append(''.join(l.split(":")[1:]).strip())
                else:
                    matches = re.findall(link_pattern, l)
                    for match in matches:
                        keywords.append(match)
        
        self.batch_info = batch_infos
        if len(keywords) == 2:
            self.locations = (keywords[0][1], [k[1] for k in keywords[1:]])
        else:
            self.locations = (keywords[0][1], [f"[{k[0]}]({k[1]})" for k in keywords[1:]])
        self.dep_table_list = MdTableGenerator.parse_markdown_table(dep_table_list)
        # self.dep_down_table_info = dep_down_table_info

# sb = SpecificationBuilder("wa_album")
# sb.collect_static_data()
# sb.generate_semantic_data("TABLE_NOTICE")
# sb.generate_semantic_data("HOW_TO_USE")
# sb.generate_semantic_data("DOWNSTREAM_TABLE_INFO")
# sb.build_mdfile()