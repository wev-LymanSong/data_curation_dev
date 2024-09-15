import requests
# from typing import List, Dict, Tuple
from notion_client import Client
import os
from markdown_it import MarkdownIt
from gedi_dev.codes.utils.parser_utils import parse_text
from gedi_dev.codes.utils.table_generator import TableGenerator

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
    "divider": {}  # Divider는 속성 없이 빈 객체로 정의
}

# Markdown-it-py 파서 인스턴스 생성
md = MarkdownIt()

# Markdown 파일 읽기
markdown_file_path = "/Users/lymansong/Documents/GitHub/mtms/data/specs/ws_album_sale.md"
with open(markdown_file_path, "r", encoding="utf-8") as file:
    markdown_content = file.read()

# Markdown 내용 파싱
tokens = md.parse(markdown_content)

# 파싱된 토큰 출력
for token in tokens:
    print(f"Token Type: {token.type}, Tag: {token.tag}, Content: {token.content if token.content == '' or token.content[0] != '|' else 'table'}")

def get_heading(level:int, content_string:str):
    rich_texts = parse_text(content_string)
    return {
        "object": "block",
        "type": f"heading_{level}",  # heading_1, heading_2, heading_3 중 선택 가능
        f"heading_{level}": {  # 헤딩 유형에 맞게 key 변경 (heading_1, heading_2, heading_3)
            "rich_text": rich_texts
        }
    }

def get_text_block(content_string:str, block_type:str = "paragraph", language:str = None):
    rich_texts = parse_text(content_string)
    to_return = {
        "object": "block",
    }
    to_return['type'] = block_type
    to_return[block_type] = {}
    if language is not None:
        to_return[block_type]['caption'] = []
    to_return[block_type]['rich_text'] = rich_texts
    if language is not None:
        to_return[block_type]["language"] = language
    
    return to_return
    

PAGE_ID = "6b0a3fd1ca78482998f890a22894041f".replace("-", "")
url = f"https://api.notion.com/v1/blocks/{PAGE_ID}/children"

is_heading = False
is_plain_paragraph = False

children = []
list_children = []
item_indent_level = 0
max_indent_level = 0
list_type = None
cur_depth = 0

def other_type_parser(start_idx, block_type): ## blockquote_open 처리
    cur_idx = start_idx
    print(cur_idx)
    item = None
    while(cur_idx < len(tokens)):
        token = tokens[cur_idx]
        if token.type == "paragraph_open" or token.type == "paragraph_close" or token.type == "hr":
            cur_idx += 1
            continue
        elif token.type == "inline":
            item = get_text_block(token.content, block_type)
            cur_idx += 1
            continue
        elif token.type == "blockquote_close":
            return cur_idx, item
    return cur_idx, item

def list_item_parser(start_idx, cur_indent_level, cur_list_type, is_list_item = False):
    global cur_depth
    cur_depth += 1
    cur_idx = start_idx
    print(cur_idx)
    global max_indent_level
    while(cur_idx < len(tokens)):
        token = tokens[cur_idx]
        if "두 테이블" in token.content:
            print("dd")
        if token.type == "list_item_close":
            cur_depth -= 1
            return cur_idx, cur_indent_level
        elif token.type == "list_item_open":
            cur_idx, cur_indent_level = list_item_parser(cur_idx + 1, cur_indent_level, cur_list_type, is_list_item = True)
            cur_idx += 1
        elif token.type == "paragraph_open" or token.type == "paragraph_close" or token.type == "hr":
            cur_idx += 1
            continue
        elif token.type == "inline":
            if is_list_item:
                block_type = "bulleted_list_item" if cur_list_type == "bulleted_list_item" else "numbered_list_item"
            else:
                block_type = "paragraph"
            item = get_text_block(token.content, block_type)
            list_children.append((cur_indent_level, cur_list_type, item))
            if cur_indent_level == 0:
                children.append(item)
            cur_idx += 1
            is_list_item = False    
            continue
        elif token.type == "bullet_list_open":
            cur_indent_level += 1
            max_indent_level = max(max_indent_level, cur_indent_level)
            cur_idx, cur_indent_level = list_item_parser(cur_idx + 1, cur_indent_level, cur_list_type = "bulleted_list_item", is_list_item = True)
            # cur_idx += 1
            continue
        elif token.type == "bullet_list_close":
            cur_depth -= 1  
            cur_indent_level -= 1
            cur_idx += 1
            return cur_idx, cur_indent_level
        elif token.type == "ordered_list_open":
            cur_indent_level += 1
            max_indent_level = max(max_indent_level, cur_indent_level)
            cur_idx, cur_indent_level = list_item_parser(cur_idx + 1, cur_indent_level, cur_list_type = "ordered_list_item", is_list_item = True)
            # cur_idx += 1
            continue
        elif token.type == "ordered_list_close":
            cur_depth -= 1  
            cur_indent_level -= 1
            cur_idx += 1
            return cur_idx, cur_indent_level
        elif token.type in ["blockquote_open"]:
            block_type = "quote"
            cur_idx, item = other_type_parser(cur_idx + 1, block_type = block_type)
            list_children.append((
                cur_indent_level, 
                cur_list_type, 
                item
            ))
            if cur_indent_level == 1:
                children.append(item)
            cur_idx += 1
            is_list_item = False
            continue
        elif token.type == "fence" and token.tag == "code":
            code_block = token.content[:-1] #문자열 마지막에 들어가있는 newline 제거
            item = get_text_block(code_block, token.tag, language=token.info)
            list_children.append((
                cur_indent_level, 
                cur_list_type, 
                item
            ))
            if cur_indent_level == 0:
                children.append(item)
            cur_idx += 1
            is_list_item = False
            continue
        else:
            cur_depth -= 1  
            return cur_idx, cur_indent_level
    cur_depth -= 1  
    return cur_idx, cur_indent_level

idx = 0
while(idx < len(tokens)):
    token = tokens[idx]
    if idx == 108:
        print("dd")
    print(idx)
    
    if token.type == "heading_open":
        is_heading = True
        h_level = token.tag[1] ## h2에서 2만 추출, 헤딩 레벨 값
        idx += 1
        continue
    if is_heading:
        if token.type != "heading_close":
            children.append(get_heading(h_level, token.content))
            # 응답 확인
            idx += 1
            continue
        else:
            is_heading = False
            idx += 1
            continue
    if token.type == "hr":
        children.append(divider_block)
        idx += 1
        continue

    if token.type == "paragraph_open" and bullet_list_level == 0 and ordered_list_level == 0:
        is_plain_paragraph = True
        idx += 1
        continue

    if is_plain_paragraph:
        if token.type != "paragraph_close" and token.tag == '':
            if token.content.startswith("|") and token.content.endswith("|"): # 테이블 형식인 경우
                children.append(table_generator.get_a_table(token.content, False, False))
                idx += 1
                continue
            else: # 일반 텍스트인 경우
                children.append(get_text_block(token.content))
                idx += 1    
                continue
        else:
            is_plain_paragraph = False
            idx += 1
            continue
    
    if token.type == "ordered_list_open" or token.type == "bullet_list_open":
        list_type = "numbered_list_item" if token.type == "ordered_list_open" else "bulleted_list_item"
        idx, item_indent_level = list_item_parser(idx + 1, item_indent_level, list_type)
        item_indent_level = 0
        continue
    if token.type in ["fence", "code_block"] and token.tag == "code":
        children.append(get_text_block(token.content, token.tag, language=token.info))
        idx += 1
        continue

    if token.type in ["blockquote_open"]:
        block_type = "quote"
        idx, item = other_type_parser(idx + 1, block_type = block_type)
        children.append(item)
        idx += 1
        continue




## PATCHING 영역

def patch_children(p_level, c_level, parent_block_dict):
    reverse_dict = {value['plain_text']: key for key, value in parent_block_dict.items()}
    c_blocks = []
    cur_blocks = [b for b in list_children if b[0] in [p_level, c_level]]
    for i, b in enumerate(cur_blocks):
        if b[0] == p_level:
            if i != 0 and len(c_blocks) > 0:
                list_url = f"https://api.notion.com/v1/blocks/{cur_parent_block_id}/children"
                tmp_block = {
                    "children": c_blocks
                }
                response = requests.patch(list_url, headers=headers, json=tmp_block)
                if response.status_code == 200:
                    print("블록이 성공적으로 추가되었습니다!")
                else:
                    print(f"추가 실패: {response.status_code}, {response.json()}")
                c_blocks = []

            cur_plain_text = "".join([t['plain_text'] for t in b[2][b[2]['type']]['rich_text']])
            cur_parent_block_id = reverse_dict[cur_plain_text]
            continue
        else:
            block_item = b[-1]
            block_item["parent"] = dict()
            block_item["parent"]["block_id"] = cur_parent_block_id
            c_blocks.append(block_item)
            






blocks = {
    "children": children
}
response = requests.patch(url, headers=headers, json=blocks)
if response.status_code == 200:
    blocks = response.json().get("results", [])
else:
    print(f"블록을 가져오지 못했습니다: {response.status_code}, {response.json()}")

# # children patch one by one
# for c in children:
#     print(c)
#     tmp_block = {
#         "children": [c]
#     }
#     url = f"https://api.notion.com/v1/blocks/{PAGE_ID}/children"
#     response = requests.patch(url, headers=headers, json=tmp_block)




for idx, i in enumerate(list_children):
    indent_level, list_type, item = i
    indent = "\t" * indent_level
    list_type = "B" if list_type == "bulleted_list_item" else "N"
    print(f"{indent}{list_type}{idx} {item[item['type']]['rich_text'][0]['plain_text']}")

parent_blocks = []
parent_block_dict = {}
for i in range(0, max_indent_level):
    if i == 0:
        get_response = requests.get(url, headers=headers)
            # 응답 확인 및 블록 출력
        if get_response.status_code == 200:
            blocks = get_response.json().get("results", [])
        else:
            print(f"블록을 가져오지 못했습니다: {get_response.status_code}, {get_response.json()}")
            break
        for b in blocks:
            if b['type'] in ['bulleted_list_item', 'numbered_list_item']:
                parent_blocks.append(b)
    else:
        for pp_id in pre_parent_block_dict.keys():
            parent_url = f"https://api.notion.com/v1/blocks/{pp_id}/children"
            get_response = requests.get(parent_url, headers=headers)
            # 응답 확인 및 블록 출력
            if get_response.status_code == 200:
                blocks = get_response.json().get("results", [])
                parent_blocks.extend(blocks)
        
    for p in parent_blocks:
        parent_block_dict[p['id']] = {
            "block_id": p['id'],
            "plain_text": "".join([t['plain_text'] for t in p[p['type']]['rich_text']])
        }   
    patch_children(p_level = i, c_level = i + 1, parent_block_dict=parent_block_dict)
    
    pre_parent_block_dict = parent_block_dict
    parent_blocks = []
    parent_block_dict = {}

"""
240828 11시 48분
기본적인 헤딩, 테이블, 불렛 포인트 등 파싱 완료
현재 'blockquote_open'에서 처리 필요하기에 멈춤 (idx == 239)
이후 코드 블록까지 파싱해서 children에 넣고 부르면 됨
list_children들은 indent를 구현하려면 레벨 별로 넣어줘야 하는데 어떻게 구현 할지 고민임
현재 list_children의 각 아이템은 튜플로(indent_level, list_type, item) 꼴로 구성되어 있음

240831 16시 28분
전체 파싱 로직 구현 완료, 테스트 파일(ws_album.md) 잘 돌아감, 이후 다른 파일 작성 후 테스트 필요
"""


