import os
KEY_DIR = "/Users/lymansong/Documents/GitHub/keys"
BASE_DIR = "/Users/lymansong/Documents/GitHub/mtms"
os.chdir(BASE_DIR)
DATA_DIR = os.path.join(BASE_DIR, "data")
SPEC_DIR = os.path.join(DATA_DIR, "specs")
SOURCECODE_DIR = os.path.join(DATA_DIR, "source_codes")

os.environ["GOOGLE_API_KEY"] = "AIzaSyABEy2PPzLAgkNbH6jgeW3Re9UhDF-DEP0"


from llama_index.llms.gemini import Gemini

llm = Gemini(model="models/gemini-1.5-pro")
# resp = llm.complete("Write a poem about a magic backpack")
# print(resp)
from llama_index.core import PromptTemplate



from llama_index.core import VectorStoreIndex, SimpleDirectoryReader, Settings
from llama_index.embeddings.gemini import GeminiEmbedding
Settings.embed_model = GeminiEmbedding(model_name="models/embedding-001")
Settings.llm = Gemini(model="models/gemini-1.5-pro")

documents = SimpleDirectoryReader(SOURCECODE_DIR).load_data()
index = VectorStoreIndex.from_documents(documents)
query_engine = index.as_query_engine()
response = query_engine.query("What is the key columns for ws_order?")
print(response)