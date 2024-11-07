import os

from tools.utils.md2notion_uploader import *
from tools.connectors.notion_connector import NotionConnector
from configurations import *
# MASTER_PAGE_ID = "b43871505e034988ab04e78b72875a40"
# WE_META_DB = 'f547d15c6d3643b5ba9110d7e33c8b13'
# WE_MART_DB = 'a66fa8206321482783b3d405b457ca31'
# WE_STAT_DB = '2bab57ee96364f8a8a2a9b7ff57a8127'



notionConnector = NotionConnector(notion_api_key = os.getenv("NOTION_API_KEY"))
# md2notion = Md2NotionUploader(notion_api_key="secret_jnUpyC7BqRV1CEF3LmeEJ2sQPSFqKuiWsWtdnV2KIER")
target_table = "we_artist"
target_db = db2notion_id_dict['we_mart']
md_file_path = os.path.join(SPEC_PROD_DIR, target_table + '.md')
notionConnector.upload_mdfile2page(target_db, target_table, md_file_path=md_file_path)
