import requests
import base64
import os
from configurations import * 

# GitHub API 설정
base_url = "https://api.github.com"
owner = "benxcorp"
repo = "databricks"
headers = {
    "Authorization": f"token {os.getenv('GITHUB_TOKEN')}",
    "Accept": "application/vnd.github.v3+json"
}

def github_api_request(method, url, data=None):
    response = requests.request(method, url, headers=headers, json=data)
    if response.status_code not in [200, 201]:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None
    return response.json()

# 1. 현재 main 브랜치의 최신 커밋 SHA 가져오기
main_branch = github_api_request("GET", f"{base_url}/repos/{owner}/{repo}/git/ref/heads/main")
if not main_branch:
    print("Failed to get main branch information")
    exit(1)

base_sha = main_branch["object"]["sha"]

# 2. 변경된 파일 목록 (로컬 파일 경로와 GitHub 상의 경로)
files = os.listdir(os.path.join(CODE_DIR, "specs"))
files_to_change = []
for f in files:
    print(os.path.join(CODE_DIR, "specs", f))
    files_to_change.append((os.path.join(CODE_DIR, "specs", f), f"src/data_analytics/specs/{f}"))

# files_to_change = [
#     ("path/to/new_file.txt", "new_file.txt"),
#     ("path/to/modified_file.py", "existing/modified_file.py"),
# ]

# 3. 각 파일에 대해 변경사항 생성
new_tree = []
for local_path, github_path in files_to_change:
    # 파일 내용 읽기
    with open(local_path, "rb") as file:
        content = file.read()
    
    # Base64로 인코딩
    content_encoded = base64.b64encode(content).decode("utf-8")
    
    # GitHub에 새 blob 생성
    blob = github_api_request("POST", f"{base_url}/repos/{owner}/{repo}/git/blobs", {
        "content": content_encoded,
        "encoding": "base64"
    })
    if not blob:
        print(f"Failed to create blob for {github_path}")
        exit(1)
    
    # 새 트리에 추가
    new_tree.append({
        "path": github_path,
        "mode": "100644",
        "type": "blob",
        "sha": blob["sha"]
    })

# 4. 새 트리 생성
tree = github_api_request("POST", f"{base_url}/repos/{owner}/{repo}/git/trees", {
    "base_tree": base_sha,
    "tree": new_tree
})
if not tree:
    print("Failed to create new tree")
    exit(1)

# 5. 새 커밋 생성
commit = github_api_request("POST", f"{base_url}/repos/{owner}/{repo}/git/commits", {
    "message": "Add new file and modify existing file",
    "tree": tree["sha"],
    "parents": [base_sha]
})
if not commit:
    print("Failed to create new commit")
    exit(1)

# 6. 새 브랜치 생성
new_branch_name = "20241027_test_pr"
new_branch = github_api_request("POST", f"{base_url}/repos/{owner}/{repo}/git/refs", {
    "ref": f"refs/heads/{new_branch_name}",
    "sha": commit["sha"]
})
if not new_branch:
    print("Failed to create new branch")
    exit(1)

# 7. PR 생성
pr_data = {
    "title": "명세서 업데이트 PR 테스트",
    "body": "- 명세서 업데이트",
    "head": new_branch_name,
    "base": "main"
}
pr = github_api_request("POST", f"{base_url}/repos/{owner}/{repo}/pulls", pr_data)
if not pr:
    print("Failed to create PR")
    exit(1)

print(f"Successfully created PR: {pr['html_url']}")
