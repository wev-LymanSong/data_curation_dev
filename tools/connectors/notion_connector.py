import requests
from notion_client import Client
from tools.utils.md2notion_uploader import *
import pandas as pd
import os

class NotionConnector(object):
    """
    A class to interact with the Notion API.

    This class provides methods to perform various operations on Notion databases and pages,
    such as retrieving database contents, creating, deleting, and restoring pages.

    Attributes:
        notion_api_key (str): The API key for authenticating with the Notion API.
        notion (Client): An instance of the Notion client.
        headers (dict): HTTP headers used for API requests.

    Methods:
        get_databases_children(database_id): Retrieve child pages of a database.
        delete_page(page_id): Archive (soft delete) a page.
        restore_page(page_id): Restore an archived page.
        create_page(database_id, page_title): Create a new page in a database.
    """
    

    def __init__(self, notion_api_key):
        self.notion_api_key = notion_api_key
        self.md_uploader = Md2NotionUploader(notion_api_key=notion_api_key)
        self.notion = Client(auth=self.notion_api_key)
        self.headers = {
            "Authorization": f"{self.notion_api_key}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        
        
    def get_database_children(self, database_id = None):
        """
        지정된 데이터베이스의 하위 페이지들을 검색합니다.

        이 메서드는 Notion API에 요청을 보내 지정된 데이터베이스 내의 페이지들을 가져오고,
        결과와 함께 페이지 제목, 생성 시간, 페이지 ID를 포함하는 Pandas DataFrame을 반환합니다.

        인자:
            database_id (str): 하위 항목을 검색할 데이터베이스의 ID.

        반환:
            tuple: 다음을 포함하는 튜플:
                - list: 데이터베이스에서 검색된 페이지 객체 리스트.
                - pd.DataFrame: 페이지의 제목, 생성 시간, ID를 포함하는 DataFrame.
        """
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
                    page_title = page.get('properties').get('테이블 명').get('title')[0].get('plain_text')
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
        
    def delete_page(self, page_id = None):
        """
        https://developers.notion.com/reference/archive-a-page
        실제로는 완전 삭제가 아니고 archiving, 즉 휴지통으로 가는 것. 휴지통 이동 뒤 30일 지나면 삭제된다고 함(삭제한 편집자는 현재 API의 커넥션 ID)
        삭제되기 전엔 아래 restore 메서드로 살리기 가능
        """
        delete_url = f'https://api.notion.com/v1/pages/{page_id}'

        data = {
            "archived": True
        }
        response = requests.patch(delete_url, headers=self.headers, json=data)
        if response.status_code == 200:
            print(f"Page delete complete: page_id: {page_id}")
            return page_id, True
        else:
            print(f"Page delete failed: {response.status_code}, {response.json()}")
            return None, False
        
    def restore_page(self, page_id = None):
        delete_url = f'https://api.notion.com/v1/pages/{page_id}'
        data = {
            "archived": False
        }
        response = requests.patch(delete_url, headers=self.headers, json=data)
        if response.status_code == 200:
            print(f"Page restore complete: page_id: {page_id}")
            return page_id, True
        else:
            print(f"Page restore failed: {response.status_code}, {response.json()}")
            return None, False
        
    def create_page(self, database_id = None, page_title = None, description = "테스트 Description"):
        """
        data는 JSON으로 들어가는 페이지 속성(properties) 값이며 database의 하위 페이지일 경우 database의 properties에 있는 필드들이 원소로 들어가야 함.
        ex) 만약 상위 database의 속성 중 페이지의 제목 property 필드명이 Name이 아니고 '이름'이라면 JSON 내부에도 '이름'이라고 적어줘야 함.
        """
        create_url = 'https://api.notion.com/v1/pages'
        data = {
            "parent": { "database_id": database_id },
            "properties": {
                "테이블 명": {
                    "title": [
                        {
                            "text": {
                                "content": page_title
                            }
                        }
                    ]
                },
                "설명": {
                    "rich_text": [
                        {
                            "text": {
                                "content": description
                            }
                        }
                    ]
                }
            }
        }
        create_response = requests.post(create_url, headers=self.headers, json=data)
        if create_response.status_code == 200:
            print(f"Page {page_title} has been created successfully.")
            return create_response.json().get('id')
        else:
            print(f"Failed to create a new page: {create_response.text}")
            return None
        
    def duplicate_page(self, database_id = None, template_page_id = None, page_title = None):
        """
        Duplicates a page in a Notion database.

        This method creates a new page in the specified database by copying the content
        and properties of a template page. The new page is given a specified title.

        Args:
            database_id (str): The ID of the database where the new page will be created.
            template_page_id (str): The ID of the page to use as a template.
            page_title (str): The title for the new page.

        Returns:
            str: The ID of the newly created page if successful, None otherwise.
        """
        
        #TBD
        return None
    

    def upload_mdfile2page(self, target_db, target_table, md_file_path = None):
        
        page_responses, page_df = self.get_database_children(database_id = target_db)
        
        tmp_page = page_df[page_df['page_title'] == target_table]
        if len(tmp_page) == 1:
            target_page_uid = tmp_page['page_uid'].iloc[0].replace("-", '')
        else:
            target_page_uid = None
        
        ## Delete and Create
        if target_page_uid is not None:
            self.delete_page(page_id=target_page_uid)
        if (target_page_uid :=self.create_page(database_id = target_db, page_title = target_table)):
            print(target_page_uid)

        params = self.md_uploader.run(
            markdown_file_path=md_file_path,
            PAGE_ID=target_page_uid.replace("-", ""),
            toc_construct = True
        )
