import requests
import os
import sys
print(os.getcwd())
from notion_client import Client
from markdown_it import MarkdownIt
from gedi_wev.utils.parser_utils import get_heading, get_text_block, get_divider_block 
from gedi_wev.utils.table_generator import TableGenerator
import pandas as pd


class Md2NotionUploader(object):
    """
    A class for uploading Markdown content to Notion pages.

    This class provides functionality to parse Markdown files, convert them into Notion block structures,
    and upload the content to specified Notion pages. It handles various Markdown elements including
    headings, lists, code blocks, blockquotes, and tables.
    
    Args:
        notion_api_key (str): The API key for authenticating with the Notion API.

    Attributes:
        notion_api_key (str): The Notion API key.
        notion (Client): An instance of the Notion client.
        table_generator (TableGenerator): An instance of TableGenerator for handling table conversions.
        headers (dict): HTTP headers for Notion API requests.
        md (MarkdownIt): An instance of MarkdownIt for parsing Markdown.

    Methods:
        get_databases_children: Retrieves children of a Notion database.
        other_type_parser: Parses special block types like blockquotes.
        list_item_parser: Parses list items in the Markdown content.
        patch_children: Updates child blocks in Notion.
        get_children: Extracts and processes child elements from Markdown tokens.
        get_md_tokens: Reads and tokenizes a Markdown file.
        run: Main method to process a Markdown file and upload to Notion.
        print_listitems: Static method to print list items for debugging.
    """
    def __init__(self, notion_api_key: str):
        self.notion_api_key = notion_api_key
        self.notion = Client(auth=self.notion_api_key)
        self.table_generator = TableGenerator(notion_api_key=self.notion_api_key)
        self.headers = {
            "Authorization": f"{self.notion_api_key}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        self.md = MarkdownIt()
        
    
    def get_databases_children(self, database_id = None):
        notion_api_url = f"https://api.notion.com/v1/databases/{database_id}/query"
        df = pd.DataFrame(columns=['page_title', 'created_at', 'page_uid'])
        # 페이지 요청 (필터나 정렬이 없는 기본 쿼리)
        response = requests.post(notion_api_url, headers=self.headers, json={})
        # 응답 처리
        if response.status_code == 200:
            pages = response.json().get('results', [])
            print("Page IDs in the database:")
            for page in pages:
                page_id = page.get('id')
                try:
                    page_title = page.get('properties').get('Name').get('title')[0].get('plain_text')
                except:
                    page_title = page.get('properties').get('이름').get('title')[0].get('plain_text')
                page_created_at = page.get('created_time')
                cur_row = {
                    'page_title' : page_title, 
                    'created_at' : page_created_at, 
                    'page_uid' : page_id
                }
                df = pd.concat([df, pd.DataFrame([cur_row])], ignore_index=True)

                print(f"{page_title:30}\t{page_created_at}\t{page_id}")

            return pages, df
        else:
            print(f"database를 가져오지 못했습니다: {response.status_code}, {response.json()}")
            return notion_api_url, None
        
    def other_type_parser(self, tokens, start_idx, block_type): ## blockquote_open 등 처리
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
    def list_item_parser(self, params, start_idx, cur_indent_level, cur_list_type, is_list_item = False):
        params["cur_depth"] += 1
        cur_idx = start_idx
        print(cur_idx)
        while(cur_idx < len(params["tokens"])):
            token = params["tokens"][cur_idx]
            if token.type == "list_item_close":
                params["cur_depth"] -= 1
                return params, cur_idx, cur_indent_level
            elif token.type == "list_item_open":
                params, cur_idx, cur_indent_level = self.list_item_parser(params, cur_idx + 1, cur_indent_level, cur_list_type, is_list_item = True)
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
                params["list_children"].append((cur_indent_level, cur_list_type, item))
                if cur_indent_level == 0:
                    params["children"].append(item)
                cur_idx += 1
                is_list_item = False    
                continue
            elif token.type == "bullet_list_open":
                cur_indent_level += 1
                params["max_indent_level"] = max(params["max_indent_level"], cur_indent_level)
                params, cur_idx, cur_indent_level = self.list_item_parser(params, cur_idx + 1, cur_indent_level, cur_list_type = "bulleted_list_item", is_list_item = True)
                # cur_idx += 1
                continue
            elif token.type == "bullet_list_close":
                params["cur_depth"] -= 1  
                cur_indent_level -= 1
                cur_idx += 1
                return params, cur_idx, cur_indent_level
            elif token.type == "ordered_list_open":
                cur_indent_level += 1
                params["max_indent_level"] = max(params["max_indent_level"], cur_indent_level)
                params, cur_idx, cur_indent_level = self.list_item_parser(params, cur_idx + 1, cur_indent_level, cur_list_type = "ordered_list_item", is_list_item = True)
                continue
            elif token.type == "ordered_list_close":
                params["cur_depth"] -= 1  
                cur_indent_level -= 1
                cur_idx += 1
                return params, cur_idx, cur_indent_level
            elif token.type in ["blockquote_open"]:
                block_type = "quote"
                cur_idx, cur_indent_level = self.other_type_parser(params["tokens"], cur_idx + 1, block_type = block_type)
                params["list_children"].append((
                    cur_indent_level, 
                    cur_list_type, 
                    item
                ))
                if cur_indent_level == 1:
                    params["children"].append(item)
                cur_idx += 1
                is_list_item = False
                continue
            elif token.type == "fence" and token.tag == "code":
                code_block = token.content[:-1] #문자열 마지막에 들어가있는 newline 제거
                item = get_text_block(code_block, token.tag, language=token.info)
                params["list_children"].append((
                    cur_indent_level, 
                    cur_list_type, 
                    item
                ))
                if cur_indent_level == 0:
                    params["children"].append(item)
                cur_idx += 1
                is_list_item = False
                continue
            else:
                params["cur_depth"] -= 1  
                return params, cur_idx, cur_indent_level
        params["cur_depth"] -= 1  
        return params, cur_idx, cur_indent_level
    
    def patch_children(self, list_children, p_level, c_level, parent_block_dict):
        reverse_dict = {value['plain_text']: key for key, value in parent_block_dict.items()}
        c_blocks = []
        cur_blocks = [b for b in list_children if b[0] in [p_level, c_level]]
        for i, b in enumerate(cur_blocks):
            if b[0] == p_level:
                if b[2]['type'] in ['heading_1', 'heading_2']:
                    if b[2][b[2]['type']]['is_toggleable'] == True:
                        1 == 1
                elif b[2]['type'] not in ['bulleted_list_item', 'numbered_list_item', 'toggle']: # 현재 parent가 children 을 가질 수 없는 타입이라면 (e.g., table) 무시하고 넘어가기
                    continue

                if i != 0 and len(c_blocks) > 0:
                    list_url = f"https://api.notion.com/v1/blocks/{cur_parent_block_id}/children"
                    tmp_block = {
                        "children": c_blocks
                    }
                    response = requests.patch(list_url, headers=self.headers, json=tmp_block)
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
                if i == len(cur_blocks) - 1:
                    list_url = f"https://api.notion.com/v1/blocks/{cur_parent_block_id}/children"
                    tmp_block = {
                        "children": c_blocks
                    }
                    response = requests.patch(list_url, headers=self.headers, json=tmp_block)
                    if response.status_code == 200:
                        print("블록이 성공적으로 추가되었습니다!")
                    else:
                        print(f"추가 실패: {response.status_code}, {response.json()}")

    def get_children(self, params, tokens):
        is_heading = False
        is_plain_paragraph = False
        idx = 0

        while(idx < len(tokens)):
            token = tokens[idx]
            print(idx, token.content)
            if token.type == "heading_open":
                is_heading = True
                h_level = token.tag[1] ## h2에서 2만 추출, 헤딩 레벨 값
                idx += 1
                continue
            if is_heading:
                if token.type != "heading_close":
                    if token.content in ['Change History']:
                        params["children"].append(get_text_block(content_string = 'Change History', block_type = 'toggle'))
                        params["list_children"].append((
                            0, 
                            "toggle", 
                            get_text_block(content_string = token.content, block_type = 'toggle')
                        ))
                    elif token.content in ['👨‍👩‍👧‍👦 Up/Downstream Table List', 'COLUMN INFO']:
                        is_toggleable = True
                        params["children"].append(get_heading(h_level, token.content, is_toggleable))
                        params["list_children"].append((
                            0, 
                            "heading", 
                            get_heading(h_level, token.content, is_toggleable)
                        ))
                    else:
                        is_toggleable = False
                        params["children"].append(get_heading(h_level, token.content, is_toggleable))
                        
                    # 응답 확인
                    idx += 1
                    continue
                else:
                    is_heading = False
                    idx += 1
                    continue
            if token.type == "hr":
                params["children"].append(get_divider_block())
                idx += 1
                continue

            if token.type == "paragraph_open":
                is_plain_paragraph = True
                idx += 1
                continue

            if is_plain_paragraph:
                if token.type != "paragraph_close" and token.tag == '':
                    if token.content.startswith("|") and token.content.endswith("|"): # 테이블 형식인 경우
                        is_row_header = False if "Created/ Last Updated At" in token.content else True # Basic info 테이블인 경우에는 row header = False
                        is_column_header = False if "|Upstream Tables|Downstream Tables|" in token.content else True # Dependency 테이블인 경우에는 row column = False
                        # Change History 테이블, 디펜던시 테이블인 경우에는 토글의 children으로 넣기
                        if "|**Date**|**By**|**LINK**|" in token.content or "|#|Column Name|Data Type" in token.content or "|Upstream Tables|Downstream Tables|" in token.content: 
                            params["max_indent_level"] = max(params["max_indent_level"], 1)
                            params["list_children"].append((
                                1, 
                                "table", 
                                self.table_generator.get_a_table(token.content, is_row_header, is_column_header)
                            ))
                        else:
                            params["children"].append(self.table_generator.get_a_table(token.content, is_row_header, is_column_header))
                        idx += 1
                        continue
                    else: # 일반 텍스트인 경우
                        params["children"].append(get_text_block(token.content))
                        idx += 1    
                        continue
                else:
                    is_plain_paragraph = False
                    idx += 1
                    continue
            
            if token.type == "ordered_list_open" or token.type == "bullet_list_open":
                list_type = "numbered_list_item" if token.type == "ordered_list_open" else "bulleted_list_item"
                params, idx, _ = self.list_item_parser(params, idx + 1, params["item_indent_level"], list_type)
                params["item_indent_level"] = 0
                continue
            if token.type in ["fence", "code_block"] and token.tag == "code":
                params["children"].append(get_text_block(token.content, token.tag, language=token.info))
                idx += 1
                continue

            if token.type in ["blockquote_open"]:
                block_type = "quote"
                idx, item = self.other_type_parser(tokens, idx + 1, block_type = block_type)
                params["children"].append(item)
                idx += 1
                continue

        return params

    def get_md_tokens(self, markdown_file_path: str):
        with open(markdown_file_path, "r", encoding="utf-8") as file:
            markdown_content = file.read()
        tokens = self.md.parse(markdown_content)
        return tokens
        
    def run(self, markdown_file_path: str, PAGE_ID: str, toc_construct = False):
        tokens = self.get_md_tokens(markdown_file_path)

        params = {
            "tokens": tokens,
            "children": [],
            "list_children": [],
            "item_indent_level": 0,
            "max_indent_level": 0,
            "list_type": None,
            "cur_depth": 0
        }
        
        params = self.get_children(params,tokens)

        if toc_construct:
            params["children"][0]= {
                'object': 'block',
                'type': 'table_of_contents',
                'table_of_contents' : {
                    "color" : "gray_background"
                }
            }
        else:
            params['children'] = params['children'][1:]


        blocks = {
            "children": params["children"]
        }
        
        page_url = f"https://api.notion.com/v1/blocks/{PAGE_ID}/children"
        response = requests.patch(url = page_url, headers=self.headers, json=blocks)
        if response.status_code == 200:
            blocks = response.json().get("results", [])
        else:
            print(f"블록을 가져오지 못했습니다: {response.status_code}, {response.json()}")
        
        parent_blocks = []
        parent_block_dict = {}

        for i in range(0, params["max_indent_level"]):
            if i == 0:
                get_response = requests.get(page_url, headers=self.headers)
                    
                if get_response.status_code == 200:
                    blocks = get_response.json().get("results", [])
                else:
                    print(f"블록을 가져오지 못했습니다: {get_response.status_code}, {get_response.json()}")
                    break
                for b in blocks:
                    if b['type'] in ['bulleted_list_item', 'numbered_list_item', 'toggle']:
                        parent_blocks.append(b)
                    elif b['type'] in ["heading_1", "heading_2", "heading_3"]:
                        if b[b['type']]['is_toggleable'] == True:
                            parent_blocks.append(b)
                    else:
                        continue
            else:
                for pp_id in pre_parent_block_dict.keys():
                    block_url = f"https://api.notion.com/v1/blocks/{pp_id}/children"
                    get_response = requests.get(block_url, headers=self.headers)
                    
                    if get_response.status_code == 200:
                        blocks = get_response.json().get("results", [])
                        for b in blocks:
                            if b['type'] in ['bulleted_list_item', 'numbered_list_item', 'toggle'] or b[b['type']]['is_toggleable'] == True:
                                parent_blocks.append(b)
                
            for p in parent_blocks:
                parent_block_dict[p['id']] = {
                    "block_id": p['id'],
                    "plain_text": "".join([t['plain_text'] for t in p[p['type']]['rich_text']])
                }   
            
            self.patch_children(list_children = params['list_children'], p_level = i, c_level = i + 1, parent_block_dict=parent_block_dict)
            
            pre_parent_block_dict = parent_block_dict
            parent_blocks = []
            parent_block_dict = {}

        return params
    
    @staticmethod
    def print_listitems(list_children):
        for idx, i in enumerate(list_children):
            indent_level, list_type, item = i
            indent = "\t" * indent_level
            list_type = "B" if list_type == "bulleted_list_item" else "N"
            print(f"{indent}{list_type}{idx} {item[item['type']]['rich_text'][0]['plain_text']}")