from configurations import *
import flask
import pytz
from datetime import datetime
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

from specification_builder import *

## RUN FUNCTION


## PARAMETERS
target_table_ext = 'ws_sess_daily.py'
overwrite = True


## PROCESS
StaticDataCollector.gc_databricks.get_back_to_main_branch()

try:
    assert target_table_ext.endswith(".sql") or target_table_ext.endswith(".py")
except:
    to_return = f"""
        Error: Invalid file format for {target_table_ext}
        Please provide a file name ending with '.sql' or '.py'
    """
    print(to_return)
    ## slack notice 구현 필요

print(f"======== TARGET TABLE: {target_table_ext} =========")
target_table_ext
target_table = target_table_ext.split(".")[0] # 확장자 제거

try:
    sb = SpecificationBuilder(target_table)
except ValueError as e:
    print(f"ValueError: {str(e)}")
except Exception as e:
    print(f"An unexpected error occurred: {type(e).__name__}: {str(e)}")

prev_file_exists = False
if target_table + ".md" in os.listdir(SPEC_REPO_DIR):
    sb.read_mdfile(source_dir = SPEC_REPO_DIR)
    prev_file_exists = True

# 1. static 데이터 수집, 기존 명세서 유무와 상관 없이 새로 수집하기
sb.collect_static_data()


# 2. semantic 데이터 생성
if prev_file_exists and overwrite == False:
    pass
else:
    sb.generate_semantic_data("TABLE_NOTICE")
    sb.generate_semantic_data("HOW_TO_USE")
    # sb.generate_semantic_data("DOWNSTREAM_TABLE_INFO")


# 3. 마크다운 파일 생성
sb.save_mdfile(target_dir = SPEC_REPO_DIR)


# 4. PR 생성
    # 현재 시간을 한국 시간대로 가져오기
korea_timezone = pytz.timezone('Asia/Seoul')
current_time = datetime.now(korea_timezone)

    # target_table 기반으로 브랜치, 제목, 설명 생성
branch_name = datetime.strftime(current_time, "%y%m%d") + f"_{target_table}_specs_update"
title = f"[{datetime.strftime(current_time, '%y%m%d')}] {target_table} " + "Spec Update"
description = f"{target_table} Specifiacation Update"
    # PR 생성
created_pr = sb.sig.gc.create_pull_request(
    branch_name=branch_name,
    title=title,
    body=description,
    files=[target_table + ".md"],
    base_branch='main'
)
to_return = f"""
    * Status: SUCCESS
    * Target Table SourceCode: {sb.sig.gc.switch_table_dir_to_url(table_dir= os.path.join(field2dir_dict.get(sb.TARGET_FIELD), sb.TARGET_TABLE))}
    * Created PR: {created_pr}
"""
print(to_return)
