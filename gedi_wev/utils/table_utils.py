import pickle
import os
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity



def load_dictionary(filepath):
    with open(filepath, 'rb') as file:
        loaded_data = pickle.load(file)
    return loaded_data

def df2str(df: pd.DataFrame) -> str:
    to_return = []
    for i, r in df.iterrows():
        if r["cell_type"] == 'md':
            to_return.append(f"cell_type : {r['cell_type']}\nrole: {r['role']}\ncontent: {r['codes']}\n\n")
        else:
            if pd.isna(r['cell_title']) == False:
                to_return.append(f"cell_type : {r['cell_type']}\ncell_title : {r['cell_title']}\nrole: {r['role']}\ncode: {r['codes']}\n\n")    
            else:
                to_return.append(f"cell_type : {r['cell_type']}\nrole: {r['role']}\ncode: {r['codes']}\n\n")
    return ''.join(to_return)

def documents_to_dataframe(docs):
    data = []
    for doc in docs:
        row = {'page_content': doc.page_content}
        row.update(doc.metadata)
        data.append(row)
    
    return pd.DataFrame(data)

def get_top_n_docs(df:pd.DataFrame, target_column:str, query:str, N:int = 5, exclude_self_reference:bool = True) -> pd.DataFrame:
    # TF-IDF 벡터화
    vectorizer = TfidfVectorizer()
    if exclude_self_reference:
        df = df[df[target_column] != query]
    tfidf_matrix = vectorizer.fit_transform(df[target_column])
    query_vec = vectorizer.transform([query])

    # 코사인 유사도 계산
    cosine_similarities = cosine_similarity(query_vec, tfidf_matrix).flatten()
    positive_indices = np.where(cosine_similarities > 0)[0]
    sorted_indices = positive_indices[np.argsort(cosine_similarities[positive_indices])[::-1]]
    
    # 상위 5개 또는 양수 유사도를 가진 모든 결과 선택 (둘 중 작은 값)
    top_n = min(N, len(sorted_indices))
    top_n_indices = sorted_indices[:top_n]

    top_n = df.iloc[top_n_indices]
    return top_n
def get_top_n_words(
    df:pd.DataFrame, 
    target_column:str, 
    query:str, 
    N:int = 5, 
    method = 'jaccard',
    exclude_self_reference:bool = True) -> pd.DataFrame:
    from Levenshtein import distance
    def levenshtein_similarity(word1, word2):
        max_len = max(len(word1), len(word2))
        return 1 - (distance(word1, word2) / max_len)
    def jaccard_similarity(word1, word2):
        set1 = set(word1.lower())
        set2 = set(word2.lower())
        return len(set1 & set2) / len(set1 | set2)
    if exclude_self_reference:
        df = df[df[target_column] != query]
        df = df.reset_index(drop = True)[['table_name', "file_path"]]
    
    sim_func = jaccard_similarity if method == 'jaccard' else levenshtein_similarity
    similarities = df[target_column].apply(lambda x: sim_func(x.lower(), query.lower()))
    df['similarity'] = similarities

    positive_indices = np.where(similarities > 0)[0]
    sorted_indices = positive_indices[np.argsort(similarities[positive_indices])[::-1]]
    
    # 상위 5개 또는 양수 유사도를 가진 모든 결과 선택 (둘 중 작은 값)
    top_n = min(N, len(sorted_indices))
    top_n_indices = sorted_indices[:top_n]

    top_n = df.iloc[top_n_indices]
    return top_n