import re

def extract_link(content: str):
    # 정규 표현식을 사용하여 링크와 URL을 추출
    match = re.match(r'\[(.*?)\]\((.*?)\)', content)
    if match:
        return match.group(1), match.group(2)
    else:
        return match.group(1), None

def is_content_bold(content:str) -> bool:
    return content.startswith("**") and content.endswith("**")

def is_inline_code(content:str) -> bool:
    return content.startswith("`") and content.endswith("`")

def is_code_block(content:str) -> bool:
    if content.startswith("```") and content.endswith("```"):
        lines = content.split('\n')
        if len(lines) > 1 and lines[0].startswith("```"):
            language = lines[0][3:].strip()
            return True, language
        return True, None
    return False, None

def parse_content(c:str):
    """
    Args:
        c (str): 파싱할 문자열
    Returns:
        tuple: 파싱된 문자열, 링크, 어노테이션
    Description:
        대상 컨텐츠를 파싱하여 여러 컨텐츠 내용(c), 링크(optional), 어노테이션 등 정보를 추출한 뒤 리치 텍스트 객체의 구성요소들로 반환
    """
    is_bold = False
    link = None
    is_code = False
    is_italic = False
    is_strikethrough = False
    is_underline = False
    is_color = "default"

    if c.startswith("**"):
        is_bold = is_content_bold(c)
        c = c.strip("**")
    if c.startswith("```"):
        is_code, language = is_code_block(c)
    elif c.endswith("`"):
        is_code = is_inline_code(c)
        c = c.strip("`")
    elif re.match(r'\[(.*?)\]\((.*?)\)', c):
        c, link = extract_link(c)
    return c, link, {
        "bold":is_bold,
        "italic":is_italic,
        "strikethrough":is_strikethrough,
        "underline":is_underline,
        "code":is_code,
        "color":is_color
    }

def slice_content(content: str):
    """
    Args:
        content (str): 분할할 문자열
    Returns:
        list: 분할된 문자열 리스트
    Description:
        대상 문자열을 패턴 기준으로 컨텐츠 단위로 분할
    """
    # 정규 표현식을 사용하여 패턴을 찾음
    pattern = re.compile(r'(\[.*?\]\(.*?\)|```.*?```|`[^`\n]+`|\*\*.*?\*\*)', re.DOTALL)
    matches = pattern.finditer(content)
    
    # 매칭된 패턴을 기준으로 문자열을 분할
    slices = []
    last_end = 0
    for match in matches:
        start, end = match.span()
        if start != last_end:
            slices.append(content[last_end:start])
        slices.append(match.group())
        last_end = end
    
    if last_end < len(content):
        slices.append(content[last_end:])
    
    return slices


def parse_text(content_string:str):
    """
    Args:
        content_string (str): 파싱할 문자열
    Returns:
        list: 파싱된 문자열 리스트
    Description:
        대상 문자열을 패턴 기준으로 컨텐츠 단위로 분할, 각 컨텐츠를 파싱하여 리치 텍스트 리스트를 반환
    """
    content_list = slice_content(content_string)
    rich_texts = []
    for content in content_list:
        content, link, annotations = parse_content(content)
        rich_texts.append(
            {
                "type": "text",
                "text": {
                    "content": content,
                    "link": {"url": link} if link else None
                },
                "annotations": annotations,
                "plain_text": content,
                "href": link
            }
        )
    return rich_texts

def get_heading(level:int, content_string:str, is_toggleable = False):
    """
    Args:
        level (int): 헤딩 레벨
        content_string (str): 헤딩 내용
    Returns:
        dict: 헤딩 블록 객체
    Description:
        헤딩 블록 객체를 반환
    """
    rich_texts = parse_text(content_string)
    return {
        "object": "block",
        "type": f"heading_{level}",  # heading_1, heading_2, heading_3 중 선택 가능
        f"heading_{level}": {  # 헤딩 유형에 맞게 key 변경 (heading_1, heading_2, heading_3)
            "rich_text": rich_texts,
            "is_toggleable": is_toggleable
        }
    }

def get_text_block(content_string:str, block_type:str = "paragraph", language:str = None):
    """
    Args:
        content_string (str): 텍스트 블록 내용
        block_type (str): 블록 유형
        language (str): 언어
    Returns:
        dict: 텍스트 블록 객체
    Description:
        텍스트 블록 객체를 반환
    """
    rich_texts = parse_text(content_string)
    to_return = {
        "object": "block",
    }
    to_return['type'] = block_type
    to_return[block_type] = {}
    if language is not None:
        to_return[block_type]['caption'] = []
    to_return[block_type]['rich_text'] = rich_texts
    if language is not None:
        to_return[block_type]["language"] = language
    
    return to_return


def get_divider_block():
    """
    Args:
        None
    Returns:
        dict: 디바이더 블록 객체
    Description:
        디바이더 블록 객체를 반환
    """
    return {
        "object": "block",
        "type": "divider",
        "divider": {}  # Divider는 속성 없이 빈 객체로 정의
    }