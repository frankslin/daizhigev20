import re
import os
import json
import argparse

# 加载朝代数据
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(SCRIPT_DIR, "dynasties.json"), "r", encoding="utf-8") as f:
    dynasties = set(json.load(f))

# 作者提取模式
AUTHOR_PATTERNS = [
    # 文言格式
    (r"([^《\n]{1,10}?)等集[\s\n]", "文言等集"),
    (r"([^《\n]{1,10}?)撰[\s\n]", "文言撰"),
    (r"([^《\n]{1,10}?)纂[\s\n]", "文言纂"),
    (r"([^《\n]{1,10}?)译[\s\n]", "文言译"),
    (r"([^《\n]{1,10}?)著[\s\n]", "文言著"),
    (r"([^《\n]{1,10}?)编[\s\n]", "文言编"),
    # 基础标识
    (r"等集[：:]\s*([^\n\r]{1,10}?)[\s\n\r]", "等集标识"),
    (r"撰[：:]\s*([^\n\r]{1,10}?)[\s\n\r]", "撰标识"),
    (r"纂[：:]\s*([^\n\r]{1,10}?)[\s\n\r]", "纂标识"),
    (r"作者[：:]\s*([^\n\r]{1,10}?)[\s\n\r]", "作者标识"),
    (r"著[：:]\s*([^\n\r]{1,10}?)[\s\n\r]", "著标识"),
    # 括号格式
    (r"（([^）]{1,10}?)）著", "括号著"),
    (r"\(([^\)]{1,10}?)\)撰", "括号撰"),
    # 书名关联
    (r"^([^《\n]{1,4}?)《[^》]{2,30}》", "书名前"),
    (r"《[^》]{2,30}》([^《\n]{1,10}?)", "书名后"),
]
AUTHOR_PATTERNS = [(re.compile(pattern), name) for pattern, name in AUTHOR_PATTERNS]

# 朝代前缀模式
dynasty_prefixes = [
    (
        r"^([\u4e00-\u9fa5]{1,4})[\s]*([\u4e00-\u9fa5]{1,6})$",
        "朝代+作者",
    ),  # 如"梁僧旻"
    (
        r"^([\u4e00-\u9fa5]{1,4})[代朝][\s]*([\u4e00-\u9fa5]{1,6}?)$",
        "代朝分隔",
    ),  # 如"唐代李白"
    (
        r"^([\u4e00-\u9fa5]{1,4})[人僧][\s]*([\u4e00-\u9fa5]{1,6}?)$",
        "人称分隔",
    ),  # 如"梁僧宝唱"
]
dynasty_prefixes = [(re.compile(pattern), desc) for pattern, desc in dynasty_prefixes]


def is_valid_author(author, splited):
    """验证是否为合理的作者名"""
    if not author or len(author) < 2 or len(author) > 5:
        return False

    # 排除明显不是人名的词
    invalid_keywords = [
        "不详",
        "未知",
        "佚名",
        "无名氏",
        "待考",
        "缺名",
        "本书",
        "本稿",
        "此文",
        "该文",
        "原文",
        "古籍",
    ]

    if author in invalid_keywords:
        return False

    # 检查是否包含明显非人名的字符
    if re.search(r"[0-9a-zA-Z]", author):
        return False
    if any(c in author for c in "。？！，"):
        return False
    return True


def extract_dynasty_and_author(author_str):
    """更智能的朝代和作者分离"""
    # 常见的朝代前缀模式

    for pattern, desc in dynasty_prefixes:
        match = re.match(pattern, author_str)
        if match:
            potential_dynasty = match.group(1)
            potential_author = match.group(2)

            # 验证朝代
            if potential_dynasty in dynasties and len(potential_author) >= 2:
                return potential_dynasty, potential_author

    return None, author_str


def try_extract_metadata_from_content(content):
    """
    从内容中提取作者和朝代信息
    
    Args:
        content (str): 要分析的内容文本
        
    Returns:
        dict: 包含author和dynasty字段的字典
    """
    out = {"author": "未知", "dynasty": "未知"}
    valid_matches = find_valid_author_matches(content)
    if valid_matches:
        earliest_match = valid_matches[0]
        out["author"] = earliest_match["author"]
        out["dynasty"] = earliest_match["dynasty"]
    return out


def find_valid_author_matches(content):
    """返回所有按出现顺序排列的有效作者匹配"""
    valid_matches = []

    for pattern, name in AUTHOR_PATTERNS:
        matches = pattern.finditer(content)  # 使用finditer获取所有匹配及其位置
        for match in matches:
            author_name = match.group(1).strip()
            start_pos = match.start()  # 匹配在内容中的起始位置

            # 尝试分离朝代和作者
            spilted = False
            dynasty, clean_author = extract_dynasty_and_author(author_name)
            if dynasty:
                out["dynasty"] = dynasty
                author_name = clean_author
                spilted = True

            # 验证作者有效性
            if is_valid_author(author_name, spilted):
                valid_matches.append(
                    {
                        "author": author_name,
                        "dynasty": dynasty if dynasty else "未知",
                        "position": start_pos,
                        "pattern_name": name,
                    }
                )

    valid_matches.sort(key=lambda x: x["position"])
    return valid_matches


def format_author_output(matches):
    """将匹配结果格式化为 [朝代]作者 字符串列表"""
    formatted = []
    seen = set()
    for match in matches:
        dynasty = match["dynasty"] or "未知"
        author = match["author"]
        key = (dynasty, author)
        if key in seen:
            continue
        seen.add(key)
        if dynasty == "未知":
            formatted.append(author)
        else:
            formatted.append(f"[{dynasty}]{author}")
    return formatted


# 使用示例
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="从 Markdown 内容中提取作者与朝代信息")
    parser.add_argument("markdown_file", help="待分析的 Markdown 文件路径")
    args = parser.parse_args()

    with open(args.markdown_file, "r", encoding="utf-8") as f:
        content = f.read()

    matches = find_valid_author_matches(content)
    formatted = format_author_output(matches)

    if formatted:
        print(",".join(formatted))
    else:
        print("未知")
