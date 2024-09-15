from configurations import *
import requests


class JiraConnector(object):

    def __init__(self):
        self.user_name = os.getenv("JIRA_USER_NAME")
        self.access_token = os.getenv("JIRA_ACCESS_TOKEN")
        
    def get_issue_content(self, issue_id):
        url = f"https://bighitcorp.atlassian.net/rest/api/3/issue/{issue_id}"
        headers = {
            "X-Atlassian-Token": "no-check"
        }

        response = requests.get(
            url = url,
            headers=headers,
            auth = (self.user_name, self.access_token)
        )

        if response.status_code == 200:
            issue_data = response.json()
            return issue_data['fields']['summary'], issue_data['fields']['description']
        else:
            raise Exception(f"Failed to fetch issue content: {response.status_code} - {response.text}")

    @staticmethod
    def extract_text_from_dict(data, target_field):
        """
        중첩된 딕셔너리 또는 리스트 구조에서 모든 target_field 값을 재귀적으로 추출
        """
        texts = []
        if isinstance(data, dict):
            for key, value in data.items():
                if key == target_field:
                    texts.append(value)
                else:
                    texts.extend(JiraConnector.extract_text_from_dict(value, target_field))
        
        # 리스트인 경우, 리스트의 각 요소를 순회하며 탐색
        elif isinstance(data, list):
            for item in data:
                texts.extend(JiraConnector.extract_text_from_dict(item, target_field))

        return texts 

# TEST CODE
# jc = JiraConnector()
# title, issue_content = jc.get_issue_content("DATA-6398")
# print("\n".join(JiraConnector.extract_text_from_dict(issue_content, 'text')))