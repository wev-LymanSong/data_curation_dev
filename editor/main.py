import streamlit as st
from streamlit_ace import st_ace, KEYBINDINGS, LANGUAGES, THEMES
import os
import time
import pyautogui
## https://discuss.streamlit.io/t/web-markdown-editor/64329

st.set_page_config(page_title="테이블 명세서 편집기", layout="wide", page_icon="💡")
markdown_garammar = """
| 요소 예시 | 문법                             |
|-----------------|----------------------------------|
|헤더 생성         | `# Heading 1` `## Headign 2`        |
|*italic*, **bold**, ~~취소선~~ | `*italic*` `**bold**` `~~취소선~~`    |
|리스트 생성| `- 항목1` <br> `    1. 첫 번째 항목`            |
|[링크 삽입](https://abc.com)         | `[제목](https://example.com)`   |
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
    with open("../template.md", "rb") as f:
        file = f.read().decode('utf-8')
        default_content = file
except:
    default_content = ""

with st.sidebar:
    filetree, setup_section, md_ref = st.tabs(["명세서 리스트", "편집기 세팅", "가이드"])
    with filetree:
        files = [f for f in os.listdir("../data/specs") if f.endswith(".md")]
        target_table = st.radio(
            label = "명세서 선택",
            options = files
        )

        with open(f"../data/specs/{target_table}", "rb") as f:
            file = f.read().decode('utf-8')
        default_content = file

    
    with setup_section:
        st.subheader(":blue[편집기 파라미터 세팅]", divider = 'grey')
        theme = st.selectbox("Theme", options=THEMES, index=35)
        font_size = st.slider("Font size", 5, 24, 14)
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
    }
    pre {
        background-color: #111111 !important;
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
        tab_size=4,
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
    st.markdown(markdown_text, unsafe_allow_html=True)
