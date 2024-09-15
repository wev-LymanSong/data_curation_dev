import os
import re
import requests
import pandas as pd
from datetime import datetime, timedelta
from configurations import * 

## 각종 디렉토리 설정

class GithubConnector(object):
    def __init__(self, github_token, repo_name, branch, owner = 'benxcorp'):
        self.github_token = github_token
        self.repo_name = repo_name
        self.branch = branch
        self.owner = owner
        self.REPO_DIR = os.path.join(ROOT_DIR, self.repo_name)
        # Headers for authentication
        self.headers = {
            "Authorization": f"token {self.github_token}"
        }
        ## 데이터브릭스 코드 레포지토리 main 업데이트
        os.chdir(self.REPO_DIR)
        os.system(f"git pull origin {self.branch}") 
        os.chdir(BASE_DIR)
    
    ## databricks용 Method 정의

    def get_collaborator_list(self, permission = 'pull', page = 1):
        """
        Get a list of collaborators for the repository.

        Args:
            permission (str): The permission level to filter collaborators. Must be one of: 'pull', 'triage', 'push', 'maintain', 'admin'.
            page (int): The page number for paginated results.
        Returns:
            list: A list of dictionaries containing collaborator information.
        Raises:
            AssertionError: If an invalid permission type is provided.        
        """

        assert permission in ["pull", 'triage', "push", "maintain", "admin"], "Invalid permission type, must be one of: pull, triage, push, maintain, admin"

        url = f"https://api.github.com/repos/{self.owner}/{self.repo_name}/collaborators"
        params = {
            "permission": permission,
            "page": page
        }
        response = requests.get(url, headers=self.headers, params=params)
        people = response.json()    

    def get_update_files(self, since_date:str, time_delta_dates: int = 1, verbose: bool = False):
        """
        기간을 설정한 뒤 해당 기간 내에 업데이트된 파일들을 반환
        Args:
            since_date (str): 기간 시작 날짜
            time_delta_dates (int): 시작 날짜에서 다음 기간 까지의 일수, 기본값은 하루(1)
            verbose (bool): 상세 로그 출력 여부
        Returns:
            list: 업데이트된 파일들의 경로
        """
        
        # 기간 계산, since_date에서 time_delta_dates 만큼 지난 날짜까지 계산
        since_date_kst = datetime.strptime(f'{since_date}T00:00:00+09:00', '%Y-%m-%dT%H:%M:%S%z')
        until_date_kst = since_date_kst + timedelta(days=time_delta_dates)

        if verbose:
            print(f"Since_date(KST): {since_date_kst}")
            print(f"Until_date(KST): {until_date_kst}")
        
        # timezone 변경 (KST to UTC)
        since_date_utc = since_date_kst - timedelta(hours=9)
        until_date_utc = until_date_kst - timedelta(hours=9)

        # 기간 내 커밋 조회
        commits_url = f"https://api.github.com/repos/{self.owner}/{self.repo_name}/commits?sha={self.branch}&since={since_date_utc}&until={until_date_utc}"
        

        commit_response = requests.get(commits_url, headers=self.headers)
        commit_response.raise_for_status()  # Raise an error for bad status codes

        # Commit들에게서 sha 추출
        commits = commit_response.json()
        commit_shas = [c['sha'] for c in commits]

        # to return
        update_files = []

        for sha in commit_shas:
            # Sha로 커밋 내용 요청
            commit_url = f"https://api.github.com/repos/{self.owner}/{self.repo_name}/commits/{sha}"
            sha_response = requests.get(commit_url, headers=self.headers)
            sha_response.raise_for_status()  # Raise an error for bad status codes
            
            response_data = sha_response.json()

            
            if response_data['committer'] is None:
                pass
            # 만약 커밋 내용이 'web-flow'의 자동 코드 업데이트라면 continue
            elif response_data['committer']['login'] == 'web-flow':
                continue
            
            if verbose:
                print('--------------------------------')
                print(f"sha_id  : {response_data['sha']}")
                print(f"datetime: {response_data['commit']['author']['date']}")
                print(f"name    : {response_data['commit']['author']['name']}")
                print(f"message : {response_data['commit']['message']}")
            for file in response_data['files']:
                update_files.append(file['filename'])
                if verbose:
                    print(f"\tfile_name: {file['filename']}")
        
        return update_files
    
    @staticmethod
    def get_formatted_blocks2(table_base_dir, table_name):
        """
        타겟 파일을 읽어서 포맷팅된 코드 블록들을 반환
        Args:
            table_base_dir (str): 타겟 파일이 있는 디렉토리
            table_name (str): 타겟 파일 이름
        Returns:
            dict: 포맷팅된 코드 블록들

        Description:
            1. 로컬 디렉토리에서 타겟 파일 읽기
            2. 코드 블록 포맷팅
                - 코드 블록의 field들은 각각 cell_type, role, codes로 구성
                - cell_type은 셀의 타입을 의미
                - role은 셀의 역할을 의미
                    - cell_type에 따라 역할이 달라짐
                    - cell_type이 'py', 'sql'이라면 role은 'code'로 설정, 다만 dataflow를 위한 설정 코드블록은 'setting'으로 설정
                    - cell_type이 'md'라면 role은 Basic Info 블록일 경우 'basic_info', 설명 블록일 경우 'description', 그냥 헤더일 경우 'just_heading'
                - codes는 셀의 내용을 그대로 담고 있음
        """

        # 파일 읽기
        with open(os.path.join(table_base_dir, table_name + ".py")) as file:
            ls = file.readlines()
            if ls[0] == '# Databricks notebook source\n': ## 데이터브릭스 타입의 노트북이라면(보통의 경우)
                code = "".join(ls[1:])

            code_blocks = dict()
            for i, cell in enumerate(code.split(CELL_SEPARATOR_PY)):
                if cell[:7] == '# MAGIC':
                    start_idx = cell.index("%") + 1
                    end_idx = cell.index("\n")
                    cell_type = cell[start_idx:end_idx]
                    codes = cell[end_idx + 1:].replace(MAGIC_KEYWORD, "")
                else:
                    cell_type = 'py'
                    codes = cell

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
                code_blocks[i + 1] = (cell_type, role, codes)
        
        return code_blocks

    def get_file_change_history(self, file_path, verbose: bool = False) -> pd.DataFrame:
        """
        특정 파일의 변경 이력을 조회

        Args:
            file_path (str): 조회할 파일의 경로
            verbose (bool, optional): 상세 로그 출력 여부. 기본값은 False

        Returns:
            pd.DataFrame: 파일 변경 이력이 담긴 DataFrame. 각 행은 하나의 커밋을 나타냄
            컬럼: 'Commit', 'Author', 'Date', 'Message', 'URL'

        Raises:
            None

        Description:
            1. GitHub API를 사용하여 특정 파일의 커밋 이력을 조회
            2. 각 커밋에 대한 정보(커밋 해시, 작성자, 날짜, 메시지, URL)를 추출
            3. 추출된 정보를 DataFrame으로 구성하여 반환
            4. API 요청이 실패할 경우 에러 메시지를 출력, None을 반환
        """
        
        repo_file_path = "src" + file_path.split("src")[-1].replace("\\", "/") + ".py"
        url = f"https://api.github.com/repos/{self.owner}/{self.repo_name}/commits"
        params = {
            "path": repo_file_path,
            "per_page": 100
        }
        response = requests.get(url, headers=self.headers, params=params)
        if response.status_code == 200:
            df = pd.DataFrame(columns=['Commit', 'Author', 'Date', 'Message', 'URL'])
            commits = response.json()
            for commit in commits:
                commit_sha = commit['sha']
                author_name = commit['commit']['author']['name']
                commit_date = commit['commit']['author']['date']
                commit_message = commit['commit']['message']
                commit_url = commit['html_url']
                if verbose:
                    print(f"Commit: {commit_sha}")
                    print(f"Author: {author_name}")
                    print(f"Date: {commit_date}")
                    print(f"Message: {commit_message}")
                    print("--------------------")

                # Create a dictionary for easy DataFrame creation
                commit_data = {
                    'Commit': [commit_sha],
                    'Author': [author_name],
                    'Date': [commit_date],
                    'Message': [commit_message],
                    'URL': [commit_url]
                }

                df = pd.concat([df, pd.DataFrame(commit_data)], ignore_index=True)
            return df
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            return None
        


    ## dp-airflow용 Method 정의
    def get_dag_list(self):
        cur_dag_dir = os.path.join(self.REPO_DIR, DAG_DIR)
        return [f for f in os.listdir(cur_dag_dir) if f.startswith('analytics')]
    
    def get_task_list(self, dag_name):
        if dag_name.endswith('.py'):
            dag_path = os.path.join(self.REPO_DIR, DAG_DIR, dag_name)
        else:
            dag_path = os.path.join(self.REPO_DIR, DAG_DIR, dag_name + '.py')
        with open(dag_path, 'r', encoding='utf-8') as file:
            code = file.read()
    
        local_vars = {}
        exec(code, globals(), local_vars)
        owner = local_vars.get('owner')
        dag_id = local_vars.get('dag_id')
        dag_task_list = local_vars.get('dag_task_list')
        return owner, dag_id, dag_task_list
    

    ## static or 클래스 공통 Method 들 정의

    def search_tables(self, target_table_name: str, verbose = False) -> pd.DataFrame:
        """
        테이블 이름을 기반으로 GitHub 저장소에서 관련 파일을 검색 후 반환

        Args:
            target_table_name (str): 검색할 테이블 이름
            verbose (bool, optional): 상세 로그 출력 여부. 기본값은 False

        Returns:
            pd.DataFrame: 검색 결과를 담은 DataFrame. 각 행은 하나의 검색 결과를 나타냄
            컬럼: 'table_name', 'file_path', 'score'

        Raises:
            None

        Description:
            1. GitHub API의 코드 검색 기능을 사용하여 지정된 테이블 이름과 관련된 파일을 검색
            2. 검색 결과에서 파일 이름, 경로, 관련성 점수를 추출
            3. 추출된 정보를 DataFrame으로 구성하여 반환
            4. API 요청이 실패할 경우 에러 메시지를 출력, None을 반환
        """
        
        url = "https://api.github.com/search/code"
        df = pd.DataFrame(columns=['table_name', 'file_path', 'score'])
        full_query = f'repo:{self.owner}/{self.repo_name} {target_table_name}'
        # full_qeury = "ws_album_sale+in:file+language:py+repo:benxcorp/databricks"
        params = {
            "q": full_query,
            "per_page": 100
        }
        response = requests.get(url, headers=self.headers, params=params)
        if response.status_code == 200:
            results = response.json()
            for result in results['items']:
                result_data = {
                    'table_name': result['name'], 
                    'file_path': result['path'],
                    'score': result['score']
                }
                if verbose:
                    print(result_data)
                df = pd.concat([df, pd.DataFrame([result_data])], ignore_index=True)
            return df
        else:
            print(response.text)
            return None
        
    @staticmethod
    def to_df(code_blocks, columns:dict = None):
        if columns is None:
            columns = {0: "cell_type", 1: "cell_title", 2: "role", 3: "codes"}
        return pd.DataFrame(code_blocks).T.rename(columns=columns)
    
    @staticmethod
    def export(df, file_name = "tmp"):
        df.to_csv(os.path.join(DATA_DIR, file_name + ".csv"))
        return df
    def switch_table_dir_to_url(self, table_dir):
        url = f"https://github.com/{self.owner}/{self.repo_name}/blob/{self.branch}/src/" + table_dir.split("/src/")[-1]
        if url.endswith(".py"):
            return url
        else:
            return url + ".py"
    def switch_dag_dir_to_url(self, dag_id):
        url = os.path.join(f"https://github.com/{self.owner}/{self.repo_name}/blob/{self.branch}", DAG_DIR, dag_id)
        if url.endswith(".py"):
            return url
        else:
            return url + ".py"    
    def parse_table_name_from_dir(self, table_dir, verbose = False):
        return table_dir.split("/")[-1].split(".")[0]


## test run - databricks
gc = GithubConnector(github_token=os.environ['GITHUB_TOKEN'], repo_name= 'databricks', branch='main')
upd_files = gc.get_update_files(since_date='2024-08-23', time_delta_dates=1, verbose=False)


# table_name = gc.parse_table_name_from_dir(upd_files[3])
# with open(os.path.join(WE_MART_DIR, table_name + ".py")) as file:
#     code = "".join(file.readlines())
        
# code_blocks = gc.get_formatted_blocks(code, 'PYTHON')
# df = GithubConnector.to_df(code_blocks=code_blocks)
# # df = GithubConnector.export(df, table_name)
# # 특정 파일의 커밋 히스토리 반환
# gc.get_file_change_history(file_path = os.path.join(WE_MART_DIR, 'ws_album_sale'), verbose=True)
# gc.search_tables(target_table_name="ws_album_sale", verbose=True)





## test run - dp-airflow
# gc_air = GithubConnector(github_token=os.environ['GITHUB_TOKEN'], repo_name='dp-airflow', branch='main')
# dags = gc_air.get_dag_list()
# df = pd.DataFrame(columns=['owner', 'dag_id', 'step', 'task'])

# for dag in dags:
#     owner, dag_id, task_list = gc_air.get_task_list(dag)
#     rows = []
#     for step in task_list:
#         for task in step['list']:
#             rows.append(
#                 {
#                 'owner' : owner,
#                 'dag_id': dag_id,
#                 'step': step["step"],
#                 'task': task
#                 } 
#             )
#     df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)



## CUSTOM TEST
# owner = 'benxcorp'
# repo = 'databricks'
# query = "ws_album_sale"
# df = pd.DataFrame(columns=['table_name', 'file_path', 'score'])
# headers = {
#     "Accept": "application/vnd.github+json",
#     "Authorization": f"token {os.environ['GITHUB_TOKEN']}"
# }
# url = "https://api.github.com/search/code"
# full_query = f"repo:{owner}/{repo} {query}"
# # full_qeury = "ws_album_sale+in:file+language:py+repo:benxcorp/databricks"
# params = {
#     "q": full_query,
#     "per_page": 100
# }
# response = requests.get(url, headers=headers, params=params)
# if response.status_code == 200:
#     results = response.json()
#     for result in results['items']:
#         result_data = {
#             'table_name': result['name'], 
#             'file_path': result['path'],
#             'score': result['score']
#         }
#         if True:
#             print(result_data)
#         df = pd.concat([df, pd.DataFrame([result_data])], ignore_index=False)
# else:

#     print(response.text)