import re
import os
import json
import argparse
import sys

# 加载朝代数据
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(SCRIPT_DIR, "dynasties.json"), "r", encoding="utf-8") as f:
    dynasties = set(json.load(f))
DYNASTY_LIST = sorted(dynasties, key=len, reverse=True)
CONNECTOR_SYMBOLS = {"·", "•", "．", "・", "‧"}
AUTHOR_LINE_RE = re.compile(r"^\s*author\s*:\s*(.*)$")
AUTHOR_SPLIT_RE = re.compile(r"[，,、/；;]+")
POLITE_SUFFIXES = ("顿首再拜", "顿首", "再拜", "拜手")
PUNCT_AFTER = r"(?:[\s\n\r，,。．\.、…]|$)"


def strip_polite_suffix(name):
    """移除作者名末尾的礼貌性词语（如“顿首”）"""
    if not name:
        return name
    name = name.strip()
    changed = True
    while changed and name:
        changed = False
        for suffix in POLITE_SUFFIXES:
            if name.endswith(suffix):
                name = name[: -len(suffix)].rstrip()
                changed = True
    return name


def normalize_dynasty_name(name):
    """规范化朝代名称（如去掉“代”“朝”等冗余后缀）"""
    if not name:
        return name
    name = name.strip()
    for suffix in ("代", "朝"):
        if name.endswith(suffix):
            base = name[:-1]
            if base in dynasties:
                return base
    return name

# 作者提取模式
AUTHOR_PATTERNS = [
    # 文言格式
    (rf"([^《\n]{{1,10}}?)等集{PUNCT_AFTER}", "文言等集"),
    (rf"([^《\n]{{1,10}}?)撰{PUNCT_AFTER}", "文言撰"),
    (rf"([^《\n]{{1,10}}?)纂{PUNCT_AFTER}", "文言纂"),
    (rf"([^《\n]{{1,10}}?)译{PUNCT_AFTER}", "文言译"),
    (rf"([^《\n]{{1,10}}?)著{PUNCT_AFTER}", "文言著"),
    (rf"([^《\n]{{1,10}}?)编{PUNCT_AFTER}", "文言编"),
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
    candidate = author_str.strip()
    if not candidate:
        return None, author_str

    for dynasty in DYNASTY_LIST:
        if candidate.startswith(dynasty):
            remainder = candidate[len(dynasty):]
            remainder = remainder.lstrip()

            # 处理常见的分隔符，如顿号或圆点
            while remainder and remainder[0] in CONNECTOR_SYMBOLS:
                remainder = remainder[1:].lstrip()

            # 处理“代”“朝”“人”等前缀词
            while remainder and remainder[0] in {"代", "朝", "人"}:
                remainder = remainder[1:].lstrip()

            remainder = strip_polite_suffix(remainder)

            normalized_dynasty = normalize_dynasty_name(dynasty)

            if len(remainder) >= 2:
                return normalized_dynasty, remainder

    return None, strip_polite_suffix(author_str.strip())


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
                author_name = clean_author
                spilted = True

            author_name = strip_polite_suffix(author_name)

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


def derive_from_existing_author(raw_value):
    """尝试从现有 author 字段推断朝代信息"""
    if not raw_value:
        return []

    raw_value = raw_value.strip().strip("\"'").strip()
    if not raw_value:
        return []

    parts = [part.strip() for part in AUTHOR_SPLIT_RE.split(raw_value) if part.strip()]
    if not parts:
        parts = [raw_value]

    formatted = []
    for part in parts:
        if part.startswith("[") and "]" in part:
            formatted.append(part)
            continue

        dynasty, author_name = extract_dynasty_and_author(part)
        if dynasty:
            formatted.append(f"[{dynasty}]{author_name}")
        else:
            formatted.append(strip_polite_suffix(part))

    return formatted


# 使用示例
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="更新 Markdown frontmatter 中的作者信息")
    parser.add_argument("markdown_file", help="待分析的 Markdown 文件路径")
    parser.add_argument(
        "--preview", action="store_true", help="仅预览提取结果，不写回文件"
    )
    parser.add_argument(
        "--lines",
        type=int,
        default=10,
        help="frontmatter 后读取的行数（默认 10 行）",
    )
    args = parser.parse_args()

    md_path = args.markdown_file

    with open(md_path, "r", encoding="utf-8") as f:
        first_line = f.readline()
        if not first_line.startswith("---"):
            sys.exit(0)

        front_lines = [first_line]
        while True:
            line = f.readline()
            if not line:
                sys.exit(0)
            front_lines.append(line)
            if line.strip() == "---":
                break

        content_lines = []
        for _ in range(max(args.lines, 0)):
            line = f.readline()
            if not line:
                break
            content_lines.append(line)

        remaining_content = f.read()

    snippet = "".join(content_lines)
    matches = find_valid_author_matches(snippet)
    formatted = format_author_output(matches)

    if args.preview:
        if formatted:
            print(",".join(formatted))
        else:
            print("未提取到作者信息")
        sys.exit(0)

    interior = front_lines[1:-1]
    author_idx = None
    old_author_value = None
    author_line_present = False
    for idx, line in enumerate(interior):
        match = AUTHOR_LINE_RE.match(line)
        if match:
            author_line_present = True
            author_idx = idx
            captured = match.group(1).strip()
            if captured:
                old_author_value = captured.strip("\"'").strip()
            break

    if author_line_present and old_author_value and not args.preview:
        # 已有作者信息则不修改
        sys.exit(0)

    if not formatted and old_author_value:
        derived = derive_from_existing_author(old_author_value)
        if derived:
            formatted = derived

    if not formatted:
        sys.exit(0)

    author_value = ",".join(formatted)

    new_author_line = f"author: '{author_value}'\n"

    if author_idx is not None:
        existing_value = old_author_value or ""
        if existing_value == author_value:
            sys.exit(0)
        interior[author_idx] = new_author_line
    else:
        interior.insert(0, new_author_line)

    new_front = [front_lines[0]] + interior + [front_lines[-1]]

    new_content = "".join(new_front) + "".join(content_lines) + remaining_content

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(new_content)

    old_display = old_author_value if old_author_value else "(none)"
    print(f"{md_path}: {old_display} -> {author_value}")
