import os
os.chdir("/Users/lymansong/Documents/GitHub/data_curation_dev")

from configurations import *
from tools.connectors.databricks_connector import DatabricksConnector
from tools.connectors.jira_connector import JiraConnector
import json
import pickle
from google.cloud import storage
from semantic_info_generator import SemanticInfoGenerator
from prompt_templates import *

## TEMP

import yaml
from google.cloud import secretmanager
import subprocess
# import functions_framework

def get_secret_yaml(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    secret_yaml = response.payload.data.decode("UTF-8")
    return yaml.safe_load(secret_yaml)

project_id = PROJECT_ID
secret_id = "gedi"
os.chdir(BASE_DIR)
secrets = get_secret_yaml(project_id, secret_id)
for key, value in secrets.items():
    os.environ[key] = value

## TEMP END


def load_dictionary(filepath):
    with open(filepath, 'rb') as file:
        loaded_data = pickle.load(file)
    return loaded_data

class RequestDatasetManager(object):
    def __init__(self, bucket):
        self.storage_client = storage.Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        assert bucket in [b.name for b in self.storage_client.list_buckets()], f'A Bucket named [{bucket}] is not in the remote Cloud Storage. Please create one before use.'
        self.bucket = self.storage_client.bucket(bucket)
        self.db_connector = DatabricksConnector()
        self.jira_connector = JiraConnector()

    def download_pkl_dataset_from_cloudstorage(self, dataset_name, ext = '.pkl', delete_file = False):
        if ext.startswith(".") == False:
            ext = "." + ext

        if dataset_name.endswith(ext) == False:
            dataset_name = dataset_name + ext

        assert dataset_name in [b.name for b in self.bucket.list_blobs()], f'A File named [{dataset_name}] is not in {self.bucket.name} bucket. Please create or upload one before use.'
        if os.path.exists(REQ_DIR) == False:
            os.mkdir(REQ_DIR)

        file_path = os.path.join(REQ_DIR, dataset_name)

        blob = self.bucket.blob(dataset_name)
        blob.download_to_filename(file_path)

        to_return = load_dictionary(file_path)
        if delete_file:
            os.remove(file_path)

        return to_return
    
    def upload_file_to_cloudstorage(
            self, 
            dataset_name, 
            ext = '.pkl', 
            local_folder = ".",
            bucket_folder = ".", 
            delete_file = False
        ):
        if ext.startswith(".") == False:
            ext = "." + ext

        if dataset_name.endswith(ext) == False:
            dataset_name = dataset_name + ext

        if os.path.exists(REQ_DIR) == False:
            raise FileExistsError(f"No directory exists. ({REQ_DIR})")

        assert dataset_name in os.listdir(REQ_DIR if local_folder == "." else os.path.join(REQ_DIR, local_folder))\
            , f'A File named [{dataset_name}] is not in {REQ_DIR}. Please create or download one before upload.'
    
        file_path = os.path.join(REQ_DIR, dataset_name) if local_folder == "." else os.path.join(REQ_DIR, local_folder, dataset_name)
        upload_path = bucket_folder + "/" + dataset_name if bucket_folder != "." else dataset_name
        blob = self.bucket.blob(upload_path)
        blob.upload_from_filename(file_path)
        blob = self.bucket.get_blob(upload_path)

        if delete_file:
            os.remove(file_path)

        return f"""
        Successfully uploaded file {dataset_name} to {self.bucket.name} bucket.
        == File Info ==
            - id: {blob.id}
            - size: {blob.size}
        """
    
    def create_request_instance(self, issue_id, req_item, save_file = True):
        # JiraConnector로 지라에서 요청 정보 가져오기, 스트링으로 저장
        title, issue_content = self.jira_connector.get_issue_content(issue_id)
        request_text = "\n".join(JiraConnector.extract_text_from_dict(issue_content, 'text'))

        data = {
            "issue_id": issue_id,
            "issue_title": title,
            "request": request_text,
            "source_code_lang": req_item[0],
            "source_code": req_item[-1]
        }

        if save_file:
            temp_file_path = os.path.join(REQ_DIR, ".tmp", f"{issue_id}.json")
            
            # 데이터를 JSON 파일로 로컬 폴더에 저장
            with open(temp_file_path, 'w', encoding='utf-8') as file:
                json.dump(data, file, ensure_ascii=False, indent=2)

            # 피클 파일을 클라우드 스토리지에 업로드
            result = self.upload_file_to_cloudstorage(
                f"{issue_id}", 
                ext=".json",
                local_folder=".tmp", 
                bucket_folder= "REQUESTS"
            )
        else:
            result = None

        return result, data


    def run_request_instance(self, data):
        processed_data_request = SemanticInfoGenerator.run(
            prompt_template = DATA_EXTRACT_REQUEST_PROMPT,
            params = {
                "data_request_title" : data['issue_title'],
                "data_request" : data['request'],
                "extract_code" : data['source_code'],
                "general_guidelines" : GENERAL_GUIDE,
            }
        )
        return processed_data_request
    
    @staticmethod
    def convert_to_json(text):
        # 정규 표현식을 사용하여 태그와 내용을 추출
        pattern = r'<(\w+)>([\s\S]*?)</\1>'
        matches = re.findall(pattern, text, re.DOTALL)
        
        # 추출된 데이터를 딕셔너리로 변환
        result = {}
        for tag, content in matches:
            # 내용의 앞뒤 공백 제거
            content = content.strip()
            result[tag] = content
        
        # 딕셔너리를 JSON 문자열로 변환
        json_result = json.dumps(result, ensure_ascii=False, indent=2)
        
        return json_result
    
    @staticmethod
    def save_json_file(file_path, data:dict):
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=2)

    @staticmethod
    def save_dictionary(filepath, target_dict = None):
        with open(filepath, 'wb') as file:
            pickle.dump(target_dict, file)
            file.close()
   
## TEST CODE
tmp = RequestDatasetManager(bucket= "gedi_dc_dataset")
ext_dict = tmp.download_pkl_dataset_from_cloudstorage(dataset_name="sourcecode_dictionary")

req_dicts = []
processed_req_dicts = []
error_reqs = []
ll = ['DATA-4025', 'DATA-4038', 'DATA-4044', 'DATA-4048', 'DATA-4049']
ll = ['DATA-4048']
for issue_id in ext_dict.keys():
    try:
        # request dict
        _, data_dict = tmp.create_request_instance(issue_id = issue_id, req_item = ext_dict[issue_id])
        # print(_)
        req_dicts.append(data_dict)
        
        # processed request dict
    
        res = tmp.run_request_instance(data_dict)

        processed_data_dict = data_dict | json.loads(tmp.convert_to_json(res))
        print(json.dumps(processed_data_dict, ensure_ascii=False, indent=2))

        output_path = os.path.join(REQ_DIR, ".tmp", "REQUESTS_PROCESSED", f"{issue_id}.json")
        tmp.save_json_file(output_path, processed_data_dict)

        if 'error' in processed_data_dict.keys():
            error_reqs.append((issue_id, processed_data_dict['error']))
        else:
            processed_req_dicts.append(processed_data_dict)
        _ = tmp.upload_file_to_cloudstorage(f"{issue_id}.json", ext = '.json', local_folder=".tmp/REQUESTS_PROCESSED", bucket_folder= 'REQUESTS_PROCESSED')
        # print(_)
    except Exception as e:
        error_reqs.append((issue_id, e))


output_path = os.path.join(REQ_DIR, "processed_request_dataset_dict.pkl")
tmp.save_dictionary(output_path, processed_req_dicts)

output_path = os.path.join(REQ_DIR, "request_dataset_dict.pkl")
tmp.save_dictionary(output_path, req_dicts)

output_path = os.path.join(REQ_DIR, "error_requests.txt")
error_dict = {}
for (k, v) in error_reqs:
    error_dict[k] = v
with open(output_path, "w", encoding='utf-8') as f:
    f.write(str(error_dict))

_ = tmp.upload_file_to_cloudstorage(f"processed_request_dataset_dict.pkl", ext = '.pkl', local_folder=".", bucket_folder= '.')
print(_)
_ = tmp.upload_file_to_cloudstorage(f"request_dataset_dict.pkl", ext = '.pkl', local_folder=".", bucket_folder= '.')
print(_)
_ = tmp.upload_file_to_cloudstorage(f"error_requests.txt", ext = '.txt', local_folder=".", bucket_folder= '.')
print(_)


try:
    tmp.create_request_instance(issue_id = issue_id, req_item = ext_dict[issue_id])
except Exception as e:
    print(e)
    