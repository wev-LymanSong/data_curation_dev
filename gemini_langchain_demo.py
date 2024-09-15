import os

os.environ["GOOGLE_API_KEY"] = "AIzaSyABEy2PPzLAgkNbH6jgeW3Re9UhDF-DEP0"


from langchain_google_genai import ChatGoogleGenerativeAI

base_dir = os.getcwd()
key_dir = os.path.join(base_dir, 'keys')
if not os.path.exists(key_dir):
    os.makedirs(key_dir)
    

from dotenv import load_dotenv
print(load_dotenv(dotenv_path= os.path.join(key_dir, ".env")))

llm = ChatGoogleGenerativeAI(
    model="gemini-1.5-pro",
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
    api_key=os.environ['GOOGLE_AI_API_KEY']
    # other params...
)
