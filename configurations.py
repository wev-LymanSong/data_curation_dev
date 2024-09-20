import os
import pandas as pd
import numpy as np
import re
import json

ROOT_DIR = "/Users/lymansong/Documents/GitHub"
KEY_DIR = "/Users/lymansong/Documents/GitHub/keys"
BASE_DIR = "/Users/lymansong/Documents/GitHub/gedi_dev"
DATA_DIR = os.path.join(BASE_DIR, 'data')
REQ_DIR = os.path.join(DATA_DIR, 'requests_extraction')
SPEC_DIR = os.path.join(DATA_DIR, "specs")
SOURCECODE_DIR = os.path.join(DATA_DIR, "source_codes")

REPO_DIR = '/Users/lymansong/Documents/GitHub/databricks'
DAG_DIR = "dags/utils/dynamic_dag/wev/task_list"

CODE_DIR = os.path.join(REPO_DIR, "src/data_analytics")
WE_MART_DIR = os.path.join(CODE_DIR, "mart/we_mart")
WE_META_DIR = os.path.join(CODE_DIR, "meta/we_meta")
WE_STAT_DIR = os.path.join(CODE_DIR, "stats/we_mart")
WI_VIEW_DIR = os.path.join(CODE_DIR, "stats/wi_view")

WE_MART_TEMPLATE_PAGE_ID = '10006aff62a880649b36da4c4d992406'
WE_META_TEMPLATE_PAGE_ID = '10206aff62a88022a849fc900506d5af'


field2dir_dict =  {
    'we_mart' : WE_MART_DIR, 
    'we_meta' : WE_META_DIR, 
    'we_stats' : WE_STAT_DIR, 
    'wi_view' : WI_VIEW_DIR, 
}

field2table_notice_template = {
    'we_mart' : WE_MART_DIR, 
    'we_meta' : WE_META_DIR, 
    'we_stats' : WE_STAT_DIR, 
    'wi_view' : WI_VIEW_DIR, 
}

from dotenv import load_dotenv
print(load_dotenv(dotenv_path= os.path.join(KEY_DIR, ".env")))
os.chdir(BASE_DIR)


COLLABORATOR_DICT = {
    # DATA INSIGHT
      "hyunjjin": "이현지"
    , "sanghyoon": "윤상혁"
    , "jemmalim11": "임지연"
    , "sangminwev": "박상민"
    , "Park-Shinyoung": "박신영"
    , "wev-LymanSong": "송재영"

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
