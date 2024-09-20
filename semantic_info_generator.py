from configurations import *
from gedi_wev.utils import table_utils as tu
from static_data_collector import StaticDataCollector
from prompt_templates import *
from gedi_wev.connectors.github_repo_connector import GithubConnector
from gedi_wev.connectors.databricks_connector import DatabricksConnector, NOTEBOOK_PREFIX_PY, NOTEBOOK_PREFIX_SQL


# from langchain_openai import ChatOpenAI, OpenAIEmbeddings
# import langchain_google_genai as genai
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAI
from langchain_google_genai import GoogleGenerativeAIEmbeddings 

# from langchain_anthropic import ChatAnthropic
from langchain.prompts import PromptTemplate
from langchain_core.documents import Document
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_core.runnables import ConfigurableField
from langchain_community.vectorstores.faiss import FAISS


from langchain_community.retrievers import BM25Retriever
from langchain.retrievers import EnsembleRetriever
# from FaissMetaVectorStore import FaissMetaVectorStore # Custom 제작 메타 데이터 벡터스토어

class SemanticInfoGenerator(object):

    # ============ Class Variables: 전체 재사용 가능한 데이터들
    dag_task_df = StaticDataCollector.get_dag_task_df()
    gc = GithubConnector(github_token=os.environ['GITHUB_TOKEN'], repo_name='databricks', branch='main', owner='benxcorp')
    llm = GoogleGenerativeAI(model="gemini-1.5-flash", api_key=os.getenv("GOOGLE_AI_API_KEY"))
    answer_pattern = r'<answer>(.*?)</answer>'
    req_ext_dataset_dict = tu.load_dictionary(os.path.join(REQ_DIR, '_dataset_dict.pkl'))
    req_ext_dataset = sorted(req_ext_dataset_dict['data'], key=lambda x: x['issue_id'], reverse= True)
    req_ext_docs = []

    for issue in req_ext_dataset:
        if issue['issue_id'].startswith('DATA') == False:
            continue
        code_block = DatabricksConnector.get_formatted_blocks(issue['source_code'], issue['source_code_lang'])
        # print(f"{issue['issue_id']}[{issue['source_code_lang']}]: {len(code_block)}, {code_block}")

        if issue['issue_id'].startswith('DATA-6001'):
            break    

        code_df = GithubConnector.to_df(code_blocks=code_block)

        req_ext_docs.append(
            Document(
                page_content = tu.df2str(code_df),
                metadata = {
                    'issue_id': issue['issue_id'] , 
                    'issue_title': issue['issue_title'] , 
                    'request': issue['request'] ,
                    'file_path': os.path.join(REQ_DIR, issue['issue_id'] + ".json")
                }
            )
        )
    
    def __init__(self, target_table):
        target_table_info = SemanticInfoGenerator.dag_task_df[SemanticInfoGenerator.dag_task_df['table_name'] == target_table].iloc[0] # take the first row in case of multiple tasks
        self.TARGET_FIELD = target_table_info['field']
        self.TARGET_DB    = target_table_info['db_name']
        self.TARGET_TABLE = target_table

        # ================== Data for context ========================
        ## df_str: TARGET_TABLE 의 소스코드 데이터
        
        tmp_df = pd.read_csv(os.path.join(SOURCECODE_DIR, f"{self.TARGET_TABLE}.csv"))
        tmp_df = tmp_df.iloc[tmp_df[tmp_df['role'] == "setting"].index.values[0] + 1 : ] # 노트북에서 `setting` 헤더 이후 코드블록만 가져오기
        self.df_str = tu.df2str(tmp_df)

    @classmethod
    def run(cls, prompt_template, params):
        prompt = PromptTemplate(
            input_variables=params.keys(),
            template=prompt_template
        )

        prompt = prompt.format(**params)
        result = cls.llm.invoke(prompt) 
        return re.search(cls.answer_pattern, result, re.DOTALL).group(1).strip()

    def get_table_notice(self):
        table_notice = SemanticInfoGenerator.run(
            prompt_template = TABLE_NOTICE_PROMPT,
            params = {
                "notebook_code" : self.df_str,
                "data_specification_documents" : "No related tables",
                "general_guide" : GENERAL_GUIDE,
                "template_document" : field2table_notice_template[self.TARGET_FIELD] 
            }
        )
        return table_notice

    def get_how_to_use(self):
        #### 추출들 중 예시가 있는 건들만 필터링(코사인 유사도 기반)
        codes_df = tu.documents_to_dataframe(SemanticInfoGenerator.req_ext_docs)
        top_n_extracts = tu.get_top_n_docs(codes_df, 'page_content', self.TARGET_TABLE)
        ext_samples = ""
        for i, r in top_n_extracts.iterrows():
            ext_samples += f"\n\n=={r['issue_id']}==\n\n"
            ext_samples += r['page_content']

        #### Downstream 테이블들 중 몇가지만 가져오기
        ds_tables = SemanticInfoGenerator.gc.search_tables(target_table_name=self.TARGET_TABLE)
        ds_tables = ds_tables[
            (ds_tables['table_name'].str != self.TARGET_TABLE + ".py") &
            (ds_tables['file_path'].str.split("/").str[-2].isin(["we_mart", "we_meta", "wi_view"]))
        ]

        sampled_ds_tables = tu.get_top_n_words(ds_tables, target_column = "table_name", query = self.TARGET_TABLE + ".py", exclude_self_reference = True)
        ds_tables_samples = ""
        for i, r in sampled_ds_tables.iterrows():
            ds_tables_samples += f"\n\n=={r['table_name'].split(".")[0]}==\n\n"
            with open(os.path.join(REPO_DIR, r['file_path']), "rb") as f:
                source_code_cells = "".join([l.decode() for l in f.readlines()])
                source_code_lang = 'PYTHON' if source_code_cells.startswith(NOTEBOOK_PREFIX_PY) else NOTEBOOK_PREFIX_SQL
                f.close()
            code_blocks = DatabricksConnector.get_formatted_blocks(
                source_code_cells=source_code_cells, 
                source_code_lang=source_code_lang
            )
            code_df = GithubConnector.to_df(code_blocks=code_blocks)

            ds_tables_samples += tu.df2str(code_df)

        how_to_use = SemanticInfoGenerator.run(
            prompt_template = HOT_TO_USE_PROMPT,
            params = {
                "target_table" : self.TARGET_TABLE,
                "target_table_source_code" : self.df_str,
                "extract_samples" : ext_samples,
                "downstream_table_source_code" : ds_tables_samples,
                "general_guide" : GENERAL_GUIDE
            }
        )

        return how_to_use
    def get_downstream_table_info(self):
        #### Downstream 테이블들 중 몇가지만 가져오기
        ds_tables = SemanticInfoGenerator.gc.search_tables(target_table_name=self.TARGET_TABLE)
        ds_tables = ds_tables[
            (ds_tables['table_name'].str != self.TARGET_TABLE + ".py") &
            (ds_tables['file_path'].str.split("/").str[-2].isin(["we_mart", "we_meta", "wi_view"]))
        ]

        sampled_ds_tables = tu.get_top_n_words(ds_tables, target_column = "table_name", query = self.TARGET_TABLE + ".py", exclude_self_reference = True)
        ds_tables_samples = ""
        for i, r in sampled_ds_tables.iterrows():
            ds_tables_samples += f"\n\n=={r['table_name'].split(".")[0]}==\n\n"
            with open(os.path.join(REPO_DIR, r['file_path']), "rb") as f:
                source_code_cells = "".join([l.decode() for l in f.readlines()])
                source_code_lang = 'PYTHON' if source_code_cells.startswith(NOTEBOOK_PREFIX_PY) else NOTEBOOK_PREFIX_SQL
                f.close()
            code_blocks = DatabricksConnector.get_formatted_blocks(
                source_code_cells=source_code_cells, 
                source_code_lang=source_code_lang
            )
            code_df = GithubConnector.to_df(code_blocks=code_blocks)

            ds_tables_samples += tu.df2str(code_df)

        downstream_table_info = SemanticInfoGenerator.run(
            prompt_template = DOWNSTREAM_TABLE_INFO_PROMPT,
            params = {
                "target_table" : self.TARGET_TABLE,
                "target_table_source_code" : self.df_str,
                "downstream_table_source_code" : ds_tables_samples,
                "general_guide" : GENERAL_GUIDE
            }
        )

        return downstream_table_info
#=====================