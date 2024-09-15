import os

from gedi_dev.codes.utils.md2notion_uploader import *
from gedi_dev.codes.connectors.notion_connector import NotionConnector
from configurations import *
MASTER_PAGE_ID = "b43871505e034988ab04e78b72875a40"
WE_META_DB = 'f547d15c6d3643b5ba9110d7e33c8b13'
WE_MART_DB = 'a66fa8206321482783b3d405b457ca31'
WE_STAT_DB = '2bab57ee96364f8a8a2a9b7ff57a8127'

TARGET_TABLE = "ws_fc_user_history"

notionConnector = NotionConnector(notion_api_key="secret_jnUpyC7BqRV1CEF3LmeEJ2sQPSFqKuiWsWtdnV2KIER")
md2notion = Md2NotionUploader(notion_api_key="secret_jnUpyC7BqRV1CEF3LmeEJ2sQPSFqKuiWsWtdnV2KIER")



## get pages in WE_META database
page_responses, page_df = notionConnector.get_database_children(database_id = WE_MART_DB)
tmp_page = page_df[page_df['page_title'] == TARGET_TABLE]
if len(tmp_page) == 1:
    target_page_uid = tmp_page['page_uid'].iloc[0].replace("-", '')
else:
    target_page_uid = None


## Delete and Create
if target_page_uid is not None:
    notionConnector.delete_page(page_id=target_page_uid)
if (target_page_uid :=notionConnector.create_page(database_id = WE_MART_DB, page_title = TARGET_TABLE)):
    print(target_page_uid)

params = md2notion.run(
    markdown_file_path=f"/Users/lymansong/Documents/GitHub/mtms/data/specs/{TARGET_TABLE.lower()}.md",
    PAGE_ID=target_page_uid.replace("-", ""),
    toc_construct = True
)

# params = md2notion.run(
#     markdown_file_path="/Users/lymansong/Documents/GitHub/mtms/data/specs/template.md",

#     PAGE_ID="c4468600980145bf8afb36ff29be9d9d"
# )


