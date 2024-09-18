import streamlit as st
from streamlit_ace import st_ace, KEYBINDINGS, LANGUAGES, THEMES
from specification_builder import SpecificationBuilder
import os
import time
import pyautogui
## https://discuss.streamlit.io/t/web-markdown-editor/64329

@st.dialog("Save Markdown file as")
def save_cur_state(markdown_code, file_name):
    st.write(f"Current File name: {file_name}")
    if st.button("Save"):
        with open(f"data/specs/{file_name}", "w", encoding='utf-8') as f:
            cleaned_content = '\n'.join(line.rstrip() for line in markdown_code.splitlines())
            file = f.write(cleaned_content)
            st.write("Saved")
def regen_all(sb, file_name):
    sb.collect_static_data()
    sb.generate_semantic_data("TABLE_NOTICE")
    sb.generate_semantic_data("HOW_TO_USE")
    sb.build_mdfile()
    return True

def regen_component(sb, file_name, component_name):
    sb.read_mdfile()
    sb.generate_semantic_data(component_name)
    sb.build_mdfile()
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

try:
    with open("template.md", "rb") as f:
        file = f.read().decode('utf-8')
        default_content = file
except:
    default_content = ""

with st.sidebar:
    
    filetree, setup_section, md_ref = st.tabs(["명세서 리스트", "편집기 세팅", "가이드"])
    with filetree:
        print(os.getcwd())
        files = [f for f in os.listdir("./data/specs") if f.endswith(".md")]
        
        st.subheader(":blue[확인 대기중인 명세서 리스트]", divider = 'grey')
        target_table = st.radio(
            label = '※ 파일 선택 후 raw code 확인',
            options = files
        )
        st.subheader(":blue[재생성 하기]", divider = 'grey')
        sb = SpecificationBuilder(target_table.split(".")[0])
        sb.read_mdfile()
        if st.button("🎲 전체 파일 재생성"):
             regen_all(sb, target_table)
        if st.button("🎲 TABLE NOTICE 재생성"):
             regen_component(sb, target_table, "TABLE_NOTICE")
        if st.button("🎲 HOW TO USE 재생성"):
             regen_component(sb, target_table, "HOW_TO_USE")
        with open(f"data/specs/{target_table}", "rb") as f:
            file = f.read().decode('utf-8')
        default_content = file
        
        on = st.toggle(label = 'See the raw code')
        if on:
            st.code(default_content, language="markdown")
        
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
    
    markdown_text = st_ace(
        value=default_content,
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
    
    if st.button("Save Current State"):
        save_cur_state(markdown_code=markdown_text, file_name = target_table)
    


with c2:
    st.empty()

# 오른쪽 열을 사용하여 마크다운 텍스트의 렌더링 효과 표시
with c3:
    st.header(":blue[Preview]", divider="gray")
    st.markdown(markdown_text, unsafe_allow_html=True)