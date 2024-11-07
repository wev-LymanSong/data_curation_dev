import streamlit as st
from streamlit_ace import st_ace, KEYBINDINGS, LANGUAGES, THEMES
from specification_builder import SpecificationBuilder
from tools.utils.md2notion_uploader import *
from configurations import *
from tools.connectors.notion_connector import NotionConnector
import os
import warnings
warnings.filterwarnings("ignore")

def init_target_table():
    return os.listdir("./data/specs_prod")[0].split(".")[0]

def init_sb():
    return SpecificationBuilder(st.session_state.target_table)

# st.session_state.target_table을 초기화합니다.
if "target_table" not in st.session_state:
    st.session_state.target_table = init_target_table()
if 'previous_target_table' not in st.session_state:
    st.session_state.previous_target_table = st.session_state.target_table
if "sb" not in st.session_state:
    st.session_state.sb = init_sb()
if 'martdown_text' not in st.session_state:
    st.session_state.martdown_text = "NULL"
if 'notion_connector' not in st.session_state:
    st.session_state.notion_connector = NotionConnector(notion_api_key=os.getenv("NOTION_API_KEY"))

@st.dialog("Save Markdown file as")
def save_cur_state(file_name):
    st.write(f"Current File name: {file_name}")
    if st.button("Save"):
        with open(f"./data/specs_prod/{file_name}", "w", encoding='utf-8') as f:
            cleaned_content = '\n'.join(line.rstrip() for line in st.session_state.default_content.splitlines())
            _ = f.write(cleaned_content)
            st.write("MD file saved.")

        with open(f"./data/specs_queue/{file_name}", "w", encoding='utf-8') as f:
            cleaned_content = '\n'.join(line.rstrip() for line in st.session_state.default_content.splitlines())
            _ = f.write(cleaned_content)

    ## NOTION UPLOAD
    if st.button("Upload to Notion"):
        with st.spinner('Uploading to Notion...'):
            try:
                st.session_state.notion_connector.upload_mdfile2page(
                    target_db = db2notion_id_dict[st.session_state.sb.TARGET_DB],
                    target_table = st.session_state.sb.TARGET_TABLE,
                    md_file_path = f"./data/specs_prod/{file_name}"
                )
                st.write("Upload success")
            except:
                st.write("Upload faild")

def delete_cur_md(file_name):
    st.write(f"Current File name: {file_name}")
    if st.button("Delete"):
        print("delete")
        try:
            file_path = f"./data/specs_queue/{file_name}"
            
            if os.path.exists(file_path):
                os.remove(file_path)
                st.success(f"File {file_name} deleted successfully from specs_queue folder.")
            else:
                st.warning(f"File {file_name} does not exist in specs_queue folder.")
        except Exception as e:
            st.error(f"Error deleting file: {str(e)}")

def regen_all(file_name): 
    with st.spinner('Reconstruct the whole file...'):
        st.session_state.sb.collect_static_data()
        st.session_state.sb.generate_semantic_data("TABLE_NOTICE")
        st.session_state.sb.generate_semantic_data("HOW_TO_USE")
        st.session_state.sb.generate_semantic_data("DOWNSTREAM_TABLE_INFO")
        st.session_state.sb.build_mdfile()
        with open(f"./data/specs_queue/{st.session_state.target_table}", "rb") as f:
            file = f.read().decode('utf-8')
        st.session_state.default_content = file
        return True

def regen_component(file_name, component_name):
    with st.spinner(f'Regenarate {component_name}...'):
        st.session_state.sb.read_mdfile()
        st.session_state.sb.generate_semantic_data(component_name)
        st.session_state.sb.build_mdfile()
        with open(f"./data/specs_queue/{st.session_state.target_table}", "rb") as f:
            file = f.read().decode('utf-8')
        st.session_state.default_content = file
        return True
st.set_page_config(page_title="테이블 명세서 편집기", layout="wide", page_icon="💡")
markdown_garammar = """
    | 요소 예시 | 문법                             |
    |-----------------|----------------------------------|
    |헤더 생성         | `# Heading 1` `## Heading 2`        |
    |*italic*, **bold**, ~~취소선~~ | `*italic*` `**bold**` `~~취소선~~`    |
    |리스트 생성| `- 항목1`(bullet items) <br> `    1. 첫 번째 항목`(ordered items)            |
    |[링크 삽입](https://examplecom)         | `[제목](https://example.com)`   |
    |텍스트 인용       | `> 인용 내용`                     |
    |코드 표시         | `python인라인 코드` |
    |구분선 삽입       | `---`                            |
    | HTML 요소 사용     | `<div>HTML 요소</div>`            |
    | 위아래 내용 분리 | `분리하려는 내용 사이에 <br> 입력`    |
    """

markdwon_table_example = """
    ```markdown
    | 헤더1 | 헤더2 | 헤더3 |
    |-------|:-----:|------:|
    |표 안에 리스트 생성 금지 |||
    | 텍스트   | 텍스트   | 텍스트  |
    """ 

if "default_content" not in st.session_state:
    with open("./data/spec_template.md", "rb") as f:
        file = f.read().decode('utf-8')
        st.session_state.default_content = file

with st.sidebar:
    
    filetree, upload, setup_section, md_ref = st.tabs(["명세서 리스트", "업로드", "편집기 세팅", "가이드"])
    with filetree:
        st.subheader(":blue[명세서 리스트]", divider='grey')
        
        # 선택 박스를 사용하여 queue 또는 prod 선택
        list_type = st.selectbox("리스트 선택", ["작성 대기중", "작성 완료"])
        
        if list_type == "작성 대기중":
            files = sorted([f for f in os.listdir("./data/specs_queue") if f.endswith(".md")])
            folder = "./data/specs_queue" 
        else:  # "작성 완료"
            files = sorted([f for f in os.listdir("./data/specs_prod") if f.endswith(".md")])
            folder = "./data/specs_prod"
        
        if files:
            st.session_state.target_table = st.radio(
                label='※ 파일 선택 후 raw code 확인',
                options=files,
                key=f'{list_type}_radio'
            )
        else:
            st.write("해당 폴더에 파일이 없습니다.")
            st.session_state.target_table = None

        if st.session_state.target_table == "spec_template.md":
            1==1
        elif st.session_state.target_table and st.session_state.target_table != st.session_state.get('previous_target_table'):
            st.session_state.sb = SpecificationBuilder(st.session_state.target_table.split(".")[0])
            print("SpecificationBuilder reinitialized")
            st.session_state.sb.read_mdfile()
            st.session_state.previous_target_table = st.session_state.target_table
            print(folder)
            with open(f"{folder}/{st.session_state.target_table}", "rb") as f:
                file = f.read().decode('utf-8')
            st.session_state.default_content = file
        else:
            1==1


        if st.session_state.target_table != "spec_template.md":
            st.subheader(":blue[재생성 하기]", divider = 'grey')
            if st.button("🎲 전체 파일 재생성"):
                regen_all(st.session_state.target_table)
            if st.button("🎲 TABLE NOTICE 재생성"):
                regen_component(st.session_state.target_table, "TABLE_NOTICE")
            if st.button("🎲 HOW TO USE 재생성"):
                regen_component(st.session_state.target_table, "HOW_TO_USE")
            if st.button("🎲 DOWNSTREAM TABLE INFO 재생성"):
                regen_component(st.session_state.target_table, "DOWNSTREAM_TABLE_INFO")

        else:
            st.warning("템플릿 파일은 수정할 수 없습니다.")

        on = st.toggle(label = 'See the raw code')
        if on:
            st.code(st.session_state.default_content, language="markdown")
    with upload:
        if st.session_state.target_table != "spec_template.md":
            st.subheader(f":blue[현재 파일: {st.session_state.sb.TARGET_TABLE}]", divider='grey')
            if st.button("Save Current State as Prod"):
                st.write("Saving current state...")
                save_cur_state(file_name = st.session_state.target_table)
            if st.button(label = 'Delete in the Queue'):
                st.write("Deleting from the queue...")
                delete_cur_md(file_name = st.session_state.target_table)
            if st.button("Notion Upload"):
                st.write("Uploading to Notion...")
                notion_upload(st.session_state.target_table)
        else:
            st.warning("템플릿 파일은 수정할 수 없습니다.")
    with setup_section:
        st.subheader(":blue[편집기 파라미터 세팅]", divider = 'grey')
        theme = st.selectbox("Theme", options=THEMES, index=35)
        font_size = st.slider("Font size", 5, 24, 14)
        tab_size = st.number_input("tab size", 1, 8, 4, step = 1)
        wrap = st.checkbox("Wrap enabled", value=False)
        auto_update = st.checkbox("Auto update", value=True)
    
    with md_ref:
        st.subheader(":blue[Markdown 문법 가이드]", divider = 'grey')
        st.markdown(markdown_garammar, unsafe_allow_html=True)
        st.subheader(":blue[Markdown 테이블 예시]", divider = 'grey')
        st.markdown(markdwon_table_example, unsafe_allow_html=True)


# 두 개의 병렬 열 생성
c1, c2, c3 = st.columns([2,0.1,2])

# 1급 및 2급 제목의 스타일과 코드 블록 배경색을 변경하기 위한 전역 CSS 추가
st.markdown(
    """
    <style>
    h1{
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-radius: 5px;
        color: white !important;
        padding: 10px;
        font-size: 23pt;
    }
    h2 {
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-radius: 5px;
        color: #ffffff !important;
        padding: 8px;
        font-size: 20pt;
    }
    h3 {
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-radius: 5px;
        color: #ffffff !important;
        padding: 8px;
        font-size: 17pt;
    }
    h4, h5, h6 {
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-radius: 5px;
        color: #ffffff !important;
        padding: 8px;
        font-size: 14pt;
    }

    p {
        font-size: 12pt;
        line-height: 1.6;
        color: #ffffff;
    }

    pre {
        background-color: #111111 !important;
    }

    li {
        font-size: 12pt;
        line-height: 1.4;
        color: #ffffff;
        margin-bottom: 4px;
    }
    code {
        background-color: #2a2a2a;
        color: #e6e6e6;
        padding: 0px 3px;
        border-radius: 3px;
        font-family: 'Fira Code', 'Consolas', monospace;
        font-size: 0.9em;
        font-weight: 500;
        letter-spacing: 0.5px;
        border: 1px solid #444;
        box-shadow: 0 1px 1px rgba(0,0,0,0.1);
        transition: all 0.2s ease;
    }

    code:hover {
        background-color: #333;
        border-color: #555;
        box-shadow: 0 2px 2px rgba(0,0,0,0.15);
    }


    </style>
    """,
    unsafe_allow_html=True,
)

# 왼쪽 열을 사용하여 사용자가 입력한 마크다운 텍스트 가져오기
with c1:
    st.header(":blue[Markdown Editor]", divider="gray")
    
    st.session_state.default_content = st_ace(
        value=st.session_state.default_content,
        placeholder="명세서 입력",
        language="markdown",
        theme=theme,
        keybinding=True,
        font_size=font_size,
        tab_size=tab_size,
        show_gutter=True,
        show_print_margin=False,
        wrap=wrap,
        auto_update=auto_update,
        min_lines=45,
        key="ace",
    )
    
    
with c2:
    st.empty()

# 오른쪽 열을 사용하여 마크다운 텍스트의 렌더링 효과 표시
with c3:
    st.header(":blue[Preview]", divider="gray")
    st.markdown(st.session_state.default_content, unsafe_allow_html=True)