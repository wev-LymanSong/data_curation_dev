import requests
from typing import List, Dict, Tuple
from notion_client import Client
import os
from markdown_it import MarkdownIt
from parser_utils import parse_content, slice_content
from table_generator import TableGenerator

os.environ['NOTION_API_KEY'] = "secret_jnUpyC7BqRV1CEF3LmeEJ2sQPSFqKuiWsWtdnV2KIER"
notion = Client(auth=os.environ["NOTION_API_KEY"])
table_generator = TableGenerator(notion_api_key=os.environ["NOTION_API_KEY"])

headers = {
    "Authorization": f"{os.environ['NOTION_API_KEY']}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}
divider_block = {
    "object": "block",
    "type": "divider",
    "divider": {}  # Divider는 속성 없이 빈 객체로 정의됩니다.
}

# Markdown-it-py 파서 인스턴스 생성
md = MarkdownIt()

# Markdown 파일 읽기
markdown_file_path = "/Users/lymansong/Documents/GitHub/mtms/data/specs/ws_album.md"
with open(markdown_file_path, "r", encoding="utf-8") as file:
    markdown_content = file.read()

# Markdown 내용 파싱
tokens = md.parse(markdown_content)

# 파싱된 토큰 출력
for token in tokens:
    print(f"Token Type: {token.type}, Tag: {token.tag}, Content: {token.content}")

def parse_text(content_string:str):
    content_list = slice_content(content_string)
    rich_texts = []
    for content in content_list:
        content, link, annotations = parse_content(content)
        rich_texts.append(
            {
                "type": "text",
                "text": {
                    "content": content,
                    "link": {"url": link} if link else None
                },
                "annotations": annotations,
                "plain_text": content,
                "href": link
            }
        )
    return rich_texts

def get_heading(level:int, content_string:str):
    rich_texts = parse_text(content_string)
    return {
        "object": "block",
        "type": f"heading_{level}",  # heading_1, heading_2, heading_3 중 선택 가능
        f"heading_{level}": {  # 헤딩 유형에 맞게 key 변경 (heading_1, heading_2, heading_3)
            "rich_text": rich_texts
        }
    }

def get_item(content_string:str):
    rich_texts = parse_text(content_string)
    return {
        "object": "block",
        "type": f"bulleted_list_item",  # heading_1, heading_2, heading_3 중 선택 가능
        f"bulleted_list_item": {  # 헤딩 유형에 맞게 key 변경 (heading_1, heading_2, heading_3)
            "rich_text": rich_texts
        }
    }
    

PAGE_ID = "618b7a7d-0b74-4cf3-8457-41ee944c0562".replace("-", "")
url = f"https://api.notion.com/v1/blocks/{PAGE_ID}/children"

is_heading = False
is_plain_paragraph = False

children = []
ordered_list_level = 0
bullet_list_level = 0
cur_level = 0
cur_list_type = None

for token in tokens:
    if token.type == "heading_open":
        is_heading = True
        h_level = token.tag[1] ## h2에서 2만 추출, 헤딩 레벨 값
        continue
    if is_heading:
        if token.type != "heading_close":
            children.append(get_heading(h_level, token.content))
            # 응답 확인
            continue
        else:
            is_heading = False
    
    if token.type == "hr":
        children.append(divider_block)
        continue

    if token.type == "paragraph_open" and bullet_list_level == 0 and ordered_list_level == 0:
        is_plain_paragraph = True
        continue

    if is_plain_paragraph:
        if token.type != "paragraph_close" and token.tag == '':
            if token.content.startswith("|") and token.content.endswith("|"): # 테이블 형식인 경우
                children.append(table_generator.get_a_table(token.content, False, False))
            else: # 일반 텍스트인 경우
                children.append(get_item(token.content))
                continue
        else:
            is_plain_paragraph = False
    
    if token.type == "bullet_list_open":
        bullet_list_level += 1
        cur_level += 1
        cur_list_type = "bullet"
        continue
    if token.type == "bullet_list_close":
        bullet_list_level -= 1
        cur_level -= 1
        continue
    if token.type == "ordered_list_open":
        ordered_list_level += 1
        cur_level += 1
        cur_list_type = "ordered"
        continue
    if token.type == "ordered_list_close":
        ordered_list_level -= 1
        cur_level -= 1
        continue

    # if cur_level > 0:

blocks = {
    "children": children
}
response = requests.patch(url, headers=headers, json=blocks)

    
