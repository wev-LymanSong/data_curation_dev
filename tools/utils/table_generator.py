
import requests
from typing import List, Dict, Tuple
from notion_client import Client
from gedi_wev.utils.parser_utils import *
import pandas as pd
from io import StringIO

class TableGenerator(object):
    def __init__(self, notion_api_key:str):
        self.notion_api_key = notion_api_key
        self.notion = Client(auth=self.notion_api_key)
        self.headers = {
            "Authorization": f"{self.notion_api_key}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }

    @staticmethod
    def get_a_table_row(content_list:List[Tuple]) -> Dict:
        cell_list = []
        
        for content in content_list:
            link_obj = {"url": content[1]} if content[1] else None
            cell_list.append(
                [
                    {
                        "type":"text",
                        "text":{
                            "content":content[0],
                            "link":link_obj
                        },
                        "annotations":content[2],
                        "plain_text":content[0],
                        "href":content[1]
                    }
                ]
            )
        row = {
            "object" : "block",
            "type" : "table_row",
            "table_row" : {"cells" : cell_list}
        }
        return row
    
    @staticmethod
    def get_a_table(table_str:str, has_column_header, has_row_header):
        df = (
            pd.read_csv(StringIO(table_str), sep="|", header=None)
            .iloc[0:, 1:-1]
            .drop(1)
            .reset_index(drop=True)
        )

        n_row = len(df)
        n_col = len(df.columns)
        
        table_rows = []
        for i, r in df.iterrows():
            print(r)
            row = TableGenerator.get_a_table_row([parse_content(c.strip()) for c in r.values])
            table_rows.append(row)
    
        table_obj = {
            "table_width":n_col,
            "has_column_header":has_column_header,
            "has_row_header":has_row_header,
        }
        table_obj['children'] = table_rows

        return {
            "object" : "block",
            "type" : "table",
            "table" : table_obj
        }
    
    @staticmethod
    def parse_markdown_table(markdown_table):
        # 줄 단위로 분리
        lines = markdown_table.strip().split('\n')
        
        # 헤더 행 추출 및 처리
        header_row = [cell for cell in lines[0].split('|') if cell != '']
        
        # 본문 행 추출 및 처리
        body_rows = []
        for line in lines[2:]:  # 구분선 (두 번째 줄) 건너뛰기
            cells = [cell for cell in line.split('|') if cell != '']
            if cells:  # 빈 줄 무시
                body_rows.append(cells)
        
        return header_row, body_rows
    
    def table_patch(self, page_id:str, table:Dict):
        table = {
            "children": [table]
        }
        url = f"https://api.notion.com/v1/blocks/{page_id}/children"
        response = requests.patch(url, headers=self.headers, json=table)
        return response


sample_table_1 = f"""
| **About**          |           위버스샵 판매 앨범 메타 정보 |
|----------------|----------------------------------------|
| **Database** | **we_meta**                            |
| **Table Category** | META PRIMARY |
| **Key Columns** | `sale_id`                              |
| **Partitioned by** | `part_date`                            |
| **Created / Last Updated At** | 2023-01-18 / 2024-08-01                 |
| **Collaborators** | 송재영 |
| **LINKS** | [link](https://www.notion.so/618b7a7d0b744cf3845741ee944c0562) |
"""

sample_table_2 = f"""
| Upstream Tables | Downstream Tables |
| --------------- | ----------------- |
| we_meta.ws_album_not_null_list   | we_mart.ws_album_sale|
| we_meta.ws_album_id              | we_mart.stats_ws_d_bulk_user_sales|
| we_mart.we_artist                | we_mart.stats_ws_d_album_sale_scm|
| weverseshop.goods                | we_mart.stats_ws_d_album_cross_ord_by_art|
| weverseshop.goods_stock          | we_mart.stats_ws_d_album_retention|
| weverseshop.stock                | we_mart.stats_wv_d_ops_sales|
| weverseshop.goods_option         | we_mart.stats_ws_d_album_sale|
| weverseshop.goods_option_group   | we_mart.stats_wa_d_album_reg_smry|
| weverseshop.sale                 |                                  |        
| weverseshop.goods_goods_category |                                  |        
| weverseshop.goods_category       |                                  |        
| weverseshop.sale_stock           |                                  |        
| weverseshop.goods_translation    |                                  |        
"""

# tg = TableGenerator(
#     notion_api_key="secret_jnUpyC7BqRV1CEF3LmeEJ2sQPSFqKuiWsWtdnV2KIER"
# )

# PAGE_ID = "618b7a7d-0b74-4cf3-8457-41ee944c0562".replace("-", "")
# response = tg.table_patch(PAGE_ID, tg.get_a_table(sample_table_2, False, False))
# # 응답 확인
# if response.status_code == 200:
#     print("테이블이 성공적으로 추가되었습니다!")
# else:
#     print(f"추가 실패: {response.status_code}, {response.json()}")