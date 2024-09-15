import os
from configurations import *
from databricks_connector import DatabricksConnector
from jira_connector import JiraConnector
import json
import pickle


def read_query_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data['request'], data['source_code']
def save_dictionary(filepath, target_dict = None):
    with open(filepath, 'wb') as file:
        pickle.dump(target_dict, file)
        file.close()
def load_dictionary(filepath):
    with open(filepath, 'rb') as file:
        loaded_data = pickle.load(file)
    return loaded_data

def save_query_file(file_path, issue_id, issue_title, request, code_lang, source_code):
    data = {
        "issue_id": issue_id,
        "issue_title": issue_title,
        "request": request,
        "source_code_lang": code_lang,
        "source_code": source_code
    }
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    return data

# 필요 커넥터들 init
db_connector = DatabricksConnector()
jira_connector = JiraConnector()

"""
추출 소스코드 가져오기
"""
# 데이터브릭스에서 요청에 대한 추출 쿼리 코드 가져오기
# req_dict = db_connector.get_request_queries()
# 로컬 폴더에 저장
# save_dictionary(os.path.join(REQ_DIR, 'sourcecode_dictionary.pkl'), target_dict = req_dict)
# 로컬 폴터에서 가져오기
req_dict = load_dictionary(os.path.join(REQ_DIR, '_sourcecode_dictionary.pkl'))


"""
추출 요청 가져오기 & 추출 소스코드와 함께 로컬에 저장
"""
req_dataset = []

for (issue_id, (lang, code)) in sorted(req_dict.items()): # 각 요청에 대해서
    try:
        # JiraConnector로 지라에서 요청 정보 가져오기, 스트링으로 저장
        title, issue_content = jira_connector.get_issue_content(issue_id)
        request = "\n".join(JiraConnector.extract_text_from_dict(issue_content, 'text'))
        print(issue_id)
        # 이슈별로 JSON으로 변환 후 로컬에 저장, 데이터셋 딕셔너리에도 append
        req_dataset.append(
            save_query_file(
                file_path =  os.path.join(REQ_DIR, f"{issue_id}.json"), 
                issue_id = issue_id, 
                issue_title = title, 
                request = request, 
                code_lang = lang, 
                source_code = code
            )
        )
    except:
        print(f"Error issue : {issue_id}")

_dataset_dict = {"dataset_name": "request_extact", "data" : req_dataset}
# 데이터셋 딕셔너리도 통으로 로컬에 저장
save_dictionary(os.path.join(REQ_DIR, "_dataset_dict.pkl"), target_dict=_dataset_dict)



