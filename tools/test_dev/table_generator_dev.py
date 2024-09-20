sample_table = f"""
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

print(sample_table)

import os
import requests
import re
from typing import List, Dict, Tuple


from notion_client import Client
os.environ['NOTION_API_KEY'] = "secret_jnUpyC7BqRV1CEF3LmeEJ2sQPSFqKuiWsWtdnV2KIER"
notion = Client(auth=os.environ["NOTION_API_KEY"])

headers = {
    "Authorization": f"{os.environ['NOTION_API_KEY']}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

#  ========================================================
import pandas as pd
from io import StringIO
def extract_link(content: str):
    # 정규 표현식을 사용하여 링크와 URL을 추출
    match = re.match(r'\[(.*?)\]\((.*?)\)', content)
    if match:
        return match.group(1), match.group(2)
    else:
        return match.group(1), None
    
def is_content_bold(content:str) -> bool:
    return content.startswith("**") and content.endswith("**")
def is_inline_code(content:str) -> bool:
    return content.startswith("`") and content.endswith("`")
def is_code_block(content:str) -> bool:
    if content.startswith("```") and content.endswith("```"):
        lines = content.split('\n')
        if len(lines) > 1 and lines[0].startswith("```"):
            language = lines[0][3:].strip()
            return True, language
        return True, None
    return False, None

df = (# 첫 번째 열과 마지막 열은 불필요한 공백 제거, 1번 row는 삭제
    pd.read_csv(StringIO(sample_table), sep="|", header=None)
    .iloc[0:, 1:-1]
    .drop(1)
    .reset_index(drop=True)
)
n_row = len(df)
n_col = len(df.columns)

def parse_content(c:str):
    is_bold = False
    link = None
    is_code = False
    is_italic = False
    is_strikethrough = False
    is_underline = False
    is_color = "default"

    if c.startswith("**"):
        is_bold = is_content_bold(c)
        c = c.strip("**")
    if c.startswith("```"):
        is_code, language = is_code_block(c)
    elif c.endswith("`"):
        is_code = is_inline_code(c)
        c = c.strip("`")
    elif c.startswith("["):
        c, link = extract_link(c)
    return c, link, {
        "bold":is_bold,
        "italic":is_italic,
        "strikethrough":is_strikethrough,
        "underline":is_underline,
        "code":is_code,
        "color":is_color
    }

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

def get_a_table(table_width, has_column_header, has_row_header, table_rows:List[Dict]):
    table_obj = {
        "table_width":table_width,
        "has_column_header":has_column_header,
        "has_row_header":has_row_header,
    }
    table_obj['children'] = table_rows

    table = {
        "object" : "block",
        "type" : "table",
        "table" : table_obj
    }

    return {"children" : [table]}

rows = []
for i, r in df.iterrows():
    row = get_a_table_row([parse_content(c.strip()) for c in r.values])
    rows.append(row)

table = get_a_table(n_col, False, False, rows)

PAGE_ID = "618b7a7d-0b74-4cf3-8457-41ee944c0562".replace("-", "")
url = f"https://api.notion.com/v1/blocks/{PAGE_ID}/children"
response = requests.patch(url, headers=headers, json=table)

# 응답 확인
if response.status_code == 200:
    print("테이블이 성공적으로 추가되었습니다!")
else:
    print(f"추가 실패: {response.status_code}, {response.json()}")




