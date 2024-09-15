import os
from notion_client import Client
os.environ['NOTION_TOKEN'] = "secret_jnUpyC7BqRV1CEF3LmeEJ2sQPSFqKuiWsWtdnV2KIER"
# os.environ['NOTION_TOKEN'] = "secret_DKyEKgj3nS66NDMLcXdGEsqR8Q1tiwkDvxiXDMwvuA"
notion = Client(auth=os.environ["NOTION_TOKEN"])


# https://www.notion.so/weversecompany/3-2-Data-Mart-cb24c78490bf4341837040e56baca915?pvs=4
import requests



# 데이터베이스 ID 및 Notion API URL
database_id = "907646de285c46b6bfdca72b26bc6e47"
notion_api_url = f"https://api.notion.com/v1/databases/{database_id}/query"

# 헤더 설정
headers = {
    "Authorization": os.environ["NOTION_TOKEN"],
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

# 페이지 요청 (필터나 정렬이 없는 기본 쿼리)
response = requests.post(notion_api_url, headers=headers, json={})
# 응답 처리
if response.status_code == 200:
    pages = response.json().get('results', [])
    print("Page IDs in the database:")
    for page in pages:
        page_id = page.get('id')
        print(page_id)
else:
    print("Error:", response.status_code, response.json())


# Notion API URL 및 페이지 ID
page_id = "02299f5a-f8c9-4b04-85a6-5fc634fc7ab9"
page_id_re = page_id.replace("-", "")
notion_api_url = f"https://api.notion.com/v1/pages/{page_id_re}"

# 헤더 설정
headers = {
    "Authorization": os.environ["NOTION_TOKEN"],
    "Notion-Version": "2022-06-28"
}
response = requests.get(notion_api_url, headers=headers)

# 응답 확인
if response.status_code == 200:
    page_properties = response.json().get('properties', {})
    print("Page Properties:")
    for prop_name, prop_value in page_properties.items():
        print(f"{prop_name}: {prop_value}")
else:
    print("Error:", response.status_code, response.json())


# Page Properties 요청 보내기
property_id = response.json()['properties']['Name']['id']
notion_api_url = f"https://api.notion.com/v1/pages/{page_id_re}/properties/{property_id}"

# 헤더 설정
headers = {
    "Authorization": os.environ["NOTION_TOKEN"],
    "Notion-Version": "2022-06-28",
}

# 요청 보내기
response = requests.get(notion_api_url, headers=headers)
# https://www.notion.so/weversecompany/we_meta-we_digital_product-02299f5af8c94b0485a65fc634fc7ab9?pvs=4#edb15aef85774784a6b03cd189791718
# https://www.notion.so/weversecompany/we_meta-we_digital_product-02299f5af8c94b0485a65fc634fc7ab9?pvs=4#35e33f03334145cc8e4aab3e1291f891
block_id = "12129556-810b-45de-9af5-af876a9cc5ab".replace("-", "")
notion_api_url = f"https://api.notion.com/v1/blocks/{block_id}"

# 헤더 설정
headers = {
    "Authorization": os.environ["NOTION_TOKEN"],
    "Notion-Version": "2022-06-28"
}
response = requests.get(notion_api_url, headers=headers)
response.json()


page_id = "02299f5af8c94b0485a65fc634fc7ab9"
notion_api_url = f"https://api.notion.com/v1/blocks/{page_id}/children"

# 헤더 설정
headers = {
    "Authorization": os.environ["NOTION_TOKEN"],
    "Notion-Version": "2022-06-28"
}

# 블록 요청
response = requests.get(notion_api_url, headers=headers)

# 응답 처리
if response.status_code == 200:
    blocks = response.json().get('results', [])
    print("Blocks in the page:")
    for block in blocks:
        block_id = block.get('id')
        block_type = block.get('type')
        print(f"Block ID: {block_id}, Type: {block_type}")
else:
    print("Error:", response.status_code, response.json())