from configurations import * 
import requests
import pandas as pd
from datetime import datetime, timedelta
import base64
from typing import List
import subprocess

## 각종 디렉토리 설정

class GithubConnector(object):
    def __init__(self, github_token, repo_name, branch, owner = 'benxcorp'):
        self.github_token = github_token
        self.repo_name = repo_name
        self.branch = branch
        self.owner = owner
        self.base_url = "https://api.github.com"
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
    def api_request(self, method, url, data=None):
        response = requests.request(method, url, headers=self.headers, json=data)
        if response.status_code not in [200, 201]:
            print(f"Error: {response.status_code}")
            print(response.text)
            return None
        return response.json()
    
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

        return self.api_request("GET", url, data=params)
           

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
        commits = self.api_request("GET", url = commits_url)        
        
        # Commit들에게서 sha 추출
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

        df = pd.DataFrame(columns=['Commit', 'Author', 'Date', 'Message', 'URL'])

        # response = requests.get(url, headers=self.headers, params=params)
        
        response = requests.get(url, headers=self.headers, params=params)
        if response.status_code != 200:
            return None
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
 
    def create_pull_request(self, branch_name:str, title:str, body:str, files:List[str], base_branch:str = 'main'):
        """
        Create a pull request with the specified changes.

        Args:
            branch_name (str): Name of the new branch to create
            title (str): Title of the pull request
            body (str): Description of the pull request
            files (List[str]): List of files to be included in the pull request
            base_branch (str, optional): Base branch for the pull request. Defaults to 'main'

        Returns:
            str: URL of the created pull request

        Process:
        1. Get the latest commit SHA of the base branch
        2. Create a list of files to be changed
        3. Create new blobs for each changed file
        4. Create a new tree with the changed files
        5. Create a new commit with the new tree
        6. Create a new branch with the new commit
        7. Create a pull request from the new branch to the base branch
        """

        # 1. 현재 main 브랜치의 최신 커밋 SHA 가져오기
        main_branch = self.api_request("GET", f"{self.base_url}/repos/{self.owner}/{self.repo_name}/git/ref/heads/{base_branch}")
        if not main_branch:
            print("Failed to get main branch information")
            exit(1)

        base_sha = main_branch["object"]["sha"]

        # 2. 변경된 파일 목록 (로컬 파일 경로와 GitHub 상의 경로)
        # files = os.listdir(os.path.join(CODE_DIR, "specs"))
        files_to_change = []
        for f in files:
            print(os.path.join(SPEC_REPO_DIR, f))
            files_to_change.append((os.path.join(SPEC_REPO_DIR, f), f"src/data_analytics/specs/{f}"))

        # 3. 각 파일에 대해 변경사항 생성
        new_tree = []
        for local_path, github_path in files_to_change:
            # 파일 내용 읽기
            with open(local_path, "rb") as file:
                content = file.read()
            
            # Base64로 인코딩
            content_encoded = base64.b64encode(content).decode("utf-8")
            
            # GitHub에 새 blob 생성
            blob = self.api_request("POST", f"{self.base_url}/repos/{self.owner}/{self.repo_name}/git/blobs", {
                "content": content_encoded,
                "encoding": "base64"
            })
            if not blob:
                print(f"Failed to create blob for {github_path}")
                exit(1)
            
            # 새 트리에 추가
            new_tree.append({
                "path": github_path,
                "mode": "100644",
                "type": "blob",
                "sha": blob["sha"]
            })

        # 4. 새 트리 생성
        tree = self.api_request("POST", f"{self.base_url}/repos/{self.owner}/{self.repo_name}/git/trees", {
            "base_tree": base_sha,
            "tree": new_tree
        })
        if not tree:
            print("Failed to create new tree")
            exit(1)

        # 5. 새 커밋 생성
        commit = self.api_request("POST", f"{self.base_url}/repos/{self.owner}/{self.repo_name}/git/commits", {
            "message": body,
            "tree": tree["sha"],
            "parents": [base_sha]
        })
        if not commit:
            print("Failed to create new commit")
            exit(1)

        # 6. 새 브랜치 생성
        new_branch = self.api_request("POST", f"{self.base_url}/repos/{self.owner}/{self.repo_name}/git/refs", {
            "ref": f"refs/heads/{branch_name}",
            "sha": commit["sha"]
        })
        if not new_branch:
            print("Failed to create new branch")
            exit(1)
        # 7. PR 생성
        pr_data = {
            "title": title,
            "body": body,
            "head": branch_name,
            "base": "main"
        }
        pr = self.api_request("POST", f"{self.base_url}/repos/{self.owner}/{self.repo_name}/pulls", pr_data)
        if not pr:
            print("Failed to create PR")
            exit(1)

        print(f"Successfully created PR: {pr['html_url']}")
        return pr['html_url']

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
    
    def get_back_to_main_branch(self):
        """
        Switch back to the main branch and synchronize with the remote repository.

        This function performs the following steps:
        1. Changes the current directory to the repository directory.
        2. Checks the current branch.
        3. If not on the main branch, switches to the main branch.
        4. Fetches the latest information from the remote repository.
        5. Resets the local main branch to match the remote main branch.

        If any error occurs during the process, it will be caught and printed.

        메인 브랜치로 전환하고 원격 저장소와 동기화합니다.

        이 함수는 다음 단계를 수행합니다:
        1. 현재 디렉토리를 저장소 디렉토리로 변경합니다.
        2. 현재 브랜치를 확인합니다.
        3. 메인 브랜치가 아닌 경우, 메인 브랜치로 전환합니다.
        4. 원격 저장소에서 최신 정보를 가져옵니다.
        5. 로컬 메인 브랜치를 원격 메인 브랜치와 일치하도록 리셋합니다.

        프로세스 중 오류가 발생하면 해당 오류를 캐치하여 출력합니다.
        """

        try:
            os.chdir(self.REPO_DIR)
            # 현재 브랜치 확인
            current_branch = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], 
                                            capture_output=True, text=True, check=True)       
            if current_branch.stdout.strip() != "main":
                # main 브랜치로 전환
                subprocess.run(["git", "checkout", "main"], check=True)
            # 원격 저장소에서 최신 정보 가져오기
            subprocess.run(["git", "fetch", "origin"], check=True)
            # 로컬 main 브랜치를 원격 main과 동기화
            subprocess.run(["git", "reset", "--hard", "origin/main"], check=True)
            print("Successfully synchronized with remote main branch.")
        except subprocess.CalledProcessError as e:
            print(f"An error occurred: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        finally:
            os.chdir(BASE_DIR)
    
    def switch_table_dir_to_url(self, table_dir):
        table_dir = table_dir.replace("\\", "/") # Windows path compatibility
        url = f"https://github.com/{self.owner}/{self.repo_name}/blob/{self.branch}/src/" + table_dir.split("/src/")[-1]
        if url.endswith(".py"):
            return url
        else:
            return url + ".py"
    def switch_dag_dir_to_url(self, dag_id):
        url = os.path.join(f"https://github.com/{self.owner}/{self.repo_name}/blob/{self.branch}", DAG_DIR, dag_id)
        url = url.replace("\\", "/")  # Windows path compatibility
        if url.endswith(".py"):
            return url
        else:
            return url + ".py"    
    def parse_table_name_from_dir(self, table_dir, verbose = False):
        return table_dir.split("/")[-1].split(".")[0]


## test run - databricks
# gc = GithubConnector(github_token=os.environ['GITHUB_TOKEN'], repo_name= 'databricks', branch='main')
# upd_files = gc.get_update_files(since_date='2024-08-23', time_delta_dates=1, verbose=False)


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