import os
import ast
from tools.connectors.github_repo_connector import *
from tools.connectors.databricks_connector import *
from configurations import *
def error_to_none(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Error occurred: {e}")
            return None
    return wrapper

class StaticDataCollector(object): 
    """
    A class for collecting static data from various sources.

    This class provides methods to interact with GitHub repositories and Databricks,
    retrieve DAG and task information, fetch code blocks, and gather table schemas.

    Class Attributes:
        gc_databricks (GithubConnector): Connector for the databricks GitHub repository.
        gc_airflow (GithubConnector): Connector for the dp-airflow GitHub repository.
        db_connector (DatabricksConnector): Connector for Databricks.

    Methods:
        safe_exec: Safely executes a given code string and returns local variables.
        get_dag_task_df: Retrieves a DataFrame containing Airflow DAG and task mapping information.
        get_code_blocks: Fetches code blocks from the target table file.
        get_basic_info_settings: Processes basic information and settings from code blocks.
        get_file_change_history: Fetches and summarizes file change history.
        get_table_schema: Retrieves table schema from Databricks SQL connection.
    """

    # Instantiate Connectors (class variables)
    gc_databricks = GithubConnector(github_token=os.environ['GITHUB_TOKEN'], repo_name= REPO_NAME, branch='main', owner = OWNER)
    gc_airflow = GithubConnector(github_token=os.environ['GITHUB_TOKEN'], repo_name='dp-airflow', branch='main')
    db_connector = DatabricksConnector(max_hop = 1)

    @staticmethod
    def safe_exec(code_str):
    # This function safely executes a given code string and returns local variables

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
    
    @classmethod
    def get_dag_task_df(cls):
        """
        Retrieves a DataFrame containing Airflow DAG and task mapping information.

        This method iterates through all DAGs in the Airflow repository, excluding test DAGs,
        and collects information about each task within the DAGs. It creates a DataFrame
        with columns for owner, DAG ID, batch interval, step, field, database name,
        table name, and file path.

        Returns:
            pandas.DataFrame: A DataFrame containing the DAG and task mapping information
            with the following columns:
            - owner: The owner of the DAG
            - dag_id: The ID of the DAG
            - batch_interval: The batch interval extracted from the DAG ID
            - step: The step number of the task
            - field: The field name derived from the task path
            - db_name: The database name derived from the task path
            - table_name: The table name derived from the task path
            - file_path: The full file path of the task
        """
        dag_task_df = pd.DataFrame(columns=['owner', 'dag_id', 'batch_interval', 'step', 'field', 'db_name', 'table_name', 'file_path'])
        for dag in cls.gc_airflow.get_dag_list():
            owner, dag_id, task_list = cls.gc_airflow.get_task_list(dag)
            if 'test' in dag_id:
                continue
            interval = dag_id.split("_")[-1].upper()
            rows = []
            for step in task_list:
                for task in step['list']:
                    task = task.split("/data_analytics/")[-1] # data_analytics 하위만 보기
                    if len(task.split("/")) == 2: # 'etc/{table}' , 'survey/{table}'과 같은 구조
                        cur_field = task.split("/")[0]
                        cur_db = 'we_mart'
                        cur_table = task.split("/")[-1]
                    elif len(task.split("/")) > 2:
                        cur_field = 'we_' + task.split("/")[-3] # 'stats/we_mart/{table}'과 같은 보통의 구조 z
                        cur_db = task.split('/')[-2]
                        cur_table = task.split("/")[-1]
                    rows.append(
                        {
                            'owner' : owner,
                            'dag_id': dag_id,
                            'batch_interval': interval,
                            'step': step["step"],
                            'field' : cur_field,
                            'db_name': cur_db,
                            'table_name':cur_table,
                            'file_path': task
                        }
                    )
            
            dag_task_df = pd.concat([dag_task_df, pd.DataFrame(rows)], ignore_index=True)
        return dag_task_df

    def __init__(self, target_field, target_db, target_table) -> None:
        self.target_field = target_field
        self.target_db = target_db
        self.target_table = target_table

    def get_code_blocks(self):
        """
        Fetches the code blocks from the target table file using Databricks Github Connector.

        Returns:
            pandas.DataFrame: A DataFrame containing the code blocks with columns for role and codes.
        """
        
        try:
            target_path = field2dir_dict[self.target_field]
        except:
            target_path = os.path.join(CODE_DIR, self.target_field)
        with open(os.path.join(target_path, self.target_table + ".py"), "rb") as f:
            source_code_cells = "".join([l.decode() for l in f.readlines()])
            if "\r" in source_code_cells:
                source_code_cells = source_code_cells.replace("\r", "")
            source_code_lang = 'PYTHON' if source_code_cells.startswith(NOTEBOOK_PREFIX_PY) else "SQL"
            f.close()

        code_blocks = DatabricksConnector.get_formatted_blocks(
            source_code_cells=source_code_cells, 
            source_code_lang=source_code_lang
        )
        code_df = GithubConnector.to_df(code_blocks=code_blocks)
        code_df.to_csv(os.path.join(SOURCECODE_DIR, f"{self.target_table}.csv"))
        return code_df
    
    def get_basic_info_settings(self, code_df):
        """
        Fetches and processes basic information and settings from the code DataFrame.

        Args:
            code_df (pandas.DataFrame): DataFrame containing code blocks.

        Returns:
            tuple: A tuple containing two elements:
                - basic_info (str): The basic information extracted from the code.
                - settings (dict): A dictionary of settings extracted and executed from the code.
        """

        basic_info = code_df[code_df['role'] == 'basic_info']['codes'].item()
        try:
            settings = code_df[(code_df["codes"].str.contains('dbutils.widgets.get'))]['codes'].item()
            settings = "\n".join([l for l in settings.split("\n") if not l.startswith(('key', 'run_mode', 'slack_token', 'channel', 'from', 'import'))])
            settings = "key = None\n" + settings
            settings = StaticDataCollector.safe_exec(settings)
        except:
            settings = None
        
        return basic_info, settings


    def get_file_change_history(self):
        """
        Fetches and summarizes the file change history for the target table.
        Returns:
            tuple: (change_df, author_counts)
                change_df: DataFrame with full change history
                author_counts: DataFrame with author contribution summary
        """
        try:
            target_path = field2dir_dict[self.target_field]
        except:
            target_path = os.path.join(CODE_DIR, self.target_field)

        file_path = os.path.join(target_path, self.target_table)
        change_df = StaticDataCollector.gc_databricks.get_file_change_history(file_path=file_path, verbose=False)
        author_counts = change_df['Author'].value_counts().reset_index()

        return change_df, author_counts
    
    def get_table_schema(self):
        """
        Retrieves the table schema from Databricks SQL connection.

        Returns:
            tuple: A tuple containing two elements:
                - desc_df (pandas.DataFrame): Description of the table schema.
                - part_indices (pandas.Index): Indices of partition information in the schema.
        """
        desc_df = StaticDataCollector.db_connector.get_desc_table(
            catalogName='wev_prod', 
            databaseName=self.target_db, 
            tableName = self.target_table
        )
        part_indices = desc_df[desc_df['col_name'] == "# Partition Information"].index
        return desc_df, part_indices