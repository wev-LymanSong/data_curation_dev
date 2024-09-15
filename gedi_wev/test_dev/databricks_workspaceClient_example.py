# Authenticate as described above
from databricks.sdk import WorkspaceClient
from configurations import *

server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")

"""
REFERENCE:
https://databricks-sdk-py.readthedocs.io/en/latest/workspace/workspace/workspace.html

"""
import io
import time
import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat

w = WorkspaceClient(
    host = server_hostname,
    token = access_token
)
error_files = []
req_dict =  {}
pattern = r'DATA[-_]\d{4}' # DATA-1234 또는 DATA_1234 추출

for i in w.workspace.list(f'/data-analytics/100.[JIRA] Source sharing/', recursive=False):
    dir_path = i.path
    if dir_path.split("/")[-1] in ["DATA-4XXX", "DATA-5XXX", "DATA-6XXX"]:
        print(f"Current Dir: {dir_path}")
        
        for notebook in w.workspace.list(path = dir_path, recursive=False):
            print(notebook.path)
            notebook_title = notebook.path.split("/")[-1]
            issue_id = re.search(pattern, notebook_title).group().replace("_", "-")
            try:
                with w.workspace.download(notebook.path) as f:
                    content = f.read()
                    req_dict[issue_id] = content.decode('utf-8')
            except:
                error_files.append(notebook.path.split("/")[-1])
                #  (\x08)이 있는 제목은 에러가 남


example_nb = "/data-analytics/100.[JIRA] Source sharing/DATA-5XXX/DATA-5889 TXT 멤버십 가입자 분석"

with w.workspace.download(example_nb) as f:
    content = f.read()
    content
    f.close()
print(content)
