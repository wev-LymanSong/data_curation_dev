import os
import pandas as pd
import numpy as np
import re
import json

# ============================ 세팅 대상 ============================

ROOT_DIR = "/Users/lymansong/Documents/GitHub"
BASE_DIR = os.path.join(ROOT_DIR, "data_curation_dev")

REPO_NAME = 'databricks_test'
OWNER = 'benxcorp'

# =================================================================

DATA_DIR = os.path.join(BASE_DIR, 'data')
REQ_DIR = os.path.join(DATA_DIR, 'request_extraction')
SPEC_QUEUE_DIR = os.path.join(DATA_DIR, "specs_queue")
SPEC_PROD_DIR = os.path.join(DATA_DIR, "specs_prod")
SOURCECODE_DIR = os.path.join(DATA_DIR, "table_source_codes")


REPO_DIR = os.path.join(ROOT_DIR, REPO_NAME)
DAG_DIR = "dags/utils/dynamic_dag/wev/task_list"

CODE_DIR = os.path.join(REPO_DIR, "src/data_analytics")
SPEC_REPO_DIR = os.path.join(CODE_DIR, 'specs')

WE_MART_DIR = os.path.join(CODE_DIR, "mart/we_mart")
WE_META_DIR = os.path.join(CODE_DIR, "meta/we_meta")
WE_STAT_DIR = os.path.join(CODE_DIR, "stats/we_mart")
WI_VIEW_DIR = os.path.join(CODE_DIR, "stats/wi_view")

#=============== Vertex AI configs
PROJECT_ID = "wev-dev-analytics"
PROJECT_NUMBER = 7783704998
LOCATION = "asia-northeast3"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "wev-dev-analytics-e27b2e077f8c.json"

#=============== Notion page ids
MASTER_PAGE_ID = "b43871505e034988ab04e78b72875a40"
WE_META_DB = 'f547d15c6d3643b5ba9110d7e33c8b13'
WE_MART_DB = 'a66fa8206321482783b3d405b457ca31'
WE_STAT_DB = '2bab57ee96364f8a8a2a9b7ff57a8127'

WE_MART_TEMPLATE_PAGE_ID = '10006aff62a880649b36da4c4d992406'
WE_META_TEMPLATE_PAGE_ID = '10206aff62a88022a849fc900506d5af'


field2dir_dict =  {
    'we_mart' : WE_MART_DIR, 
    'we_meta' : WE_META_DIR, 
    'we_stats' : WE_STAT_DIR, 
    'wi_view' : WI_VIEW_DIR,
}

db2notion_id_dict =  {
    'we_mart' : WE_MART_DB, 
    'we_meta' : WE_META_DB, 
    'we_stats': WE_STAT_DB, 
    'wi_view' : " "#WI_VIEW_DB, 
}

field2table_notice_template = {
    'we_mart' : WE_MART_DIR, 
    'we_meta' : WE_META_DIR, 
    'we_stats' : WE_STAT_DIR, 
    'wi_view' : WI_VIEW_DIR, 
}

# from dotenv import load_dotenv
# print(load_dotenv(dotenv_path= os.path.join(KEY_DIR, ".env")))
# os.chdir(BASE_DIR)


COLLABORATOR_DICT = {
    # DATA INSIGHT
      "hyunjjin": "이현지"
    , "sanghyoon": "윤상혁"
    , "jemmalim11": "임지연"
    , "sangminwev": "박상민"
    , "Park-Shinyoung": "박신영"
    , "wev-LymanSong": "송재영"
    , "lymanstudio": "송재영"
    , "hyunji@weversecompany.com": "이현지"
    , "sanghyoon@weversecompany.com": "윤상혁"
    , "jemmalim@weversecompany.com": "임지연"
    , "sangmin@weversecompany.com": "박상민"
    , "shinyoung.park@weversecompany.com": "박신영"
    , "lymansong@weversecompany.com": "송재영"

    # DATA ASSETIZATION
    , "hyekyunglim": "임혜경"
    , "hyeinseo-wev": "서혜인"
    , "sakongjh": "사공재현"

    , "hyekyung.lim@weversecompany.com": "임혜경"
    , 'lhk2502@naver.com': "임혜경"
    , "hyein.seo@weversecompany.com": "서혜인"
    , "sakongjh@weversecompany.com": "사공재현"

    # DATA PLATFORM
    , "min.koo": "구민서"
    , "minseo-koo": "구민서"
    , "hayoung0927": "정하영"
    , "kitae00": "김기태"
    , "jaina3066": "김민정"
    , "jangsjang": "장준호"

    #UNKNOWN USERS
    
}

SLACK_USER_ID_DICT = {
    # DATA INSIGHT
      "U028QEU54UQ": "이현지"
    , "W019B9VB2PP": "윤상혁"
    , "U01CY6GEPLK": "임지연"
    , "U029DGZ5E2F": "박상민"
    , "U029AA829NH": "박신영"
    , "U029KH2P6AY": "송재영"

    # DATA ASSETIZATION
    , "U05JFH50R60": "임혜경"
    , "U05CWSGV34G": "서혜인"
    , "U03F0B9528L": "사공재현"

    # DATA PLATFORM
    , "W0195SCFUTY": "구민서"
    , "U02LV0L3CQ4": "정하영"
    , "U02S5E4N2Q4": "김기태"
    , "U03363UUYLD": "김민정"
    , "W0195RDMMTQ": "장준호"
    , "U0622URV170": "전이섭"
}

PRIMARY_DAG = [
    "analytics_meta_daily", 
    "analytics_mart_daily", 
    "analytics_ws_mart_daily", 
    "analytics_wv_mart_daily", 
    "analytics_we_mart_daily",
    "analytics_log_daily",
    "analytics_wv_stats_daily",
    "analytics_we_mart_priority_daily",
    "analytics_ws_stats_daily",
    "analytics_we_stats_priority_daily",
    "analytics_we_stats_daily",
    "analytics_we_mart_hourly",
    "analytics_we_mart_monthly",
    "analytics_we_mart_weekly",
]
SECONDARY_DAG = [
    "analytics_we_stats_late_daily",
    "analytics_we_mart_late_daily",
    "analytics_mvp_collection_daily",
]
