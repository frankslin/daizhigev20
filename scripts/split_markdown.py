#!/usr/bin/env python3
"""
Markdown 文件拆分工具
根据指定大小限制将大型 markdown 文件拆分为多个小文件
优先在章节标题处分拆，次选段落边界
"""

import os
import sys
import re
import yaml
import argparse
from pathlib import Path

# 默认大小限制 (500KB)
DEFAULT_SIZE_LIMIT = 500 * 1024

def parse_frontmatter(content):
    """解析 frontmatter"""
    if not content.startswith('---\n'):
        return None, content

    # 找到第二个 ---
    try:
        end_pos = content.index('\n---\n', 4) + 5
        frontmatter_text = content[4:end_pos-5]  # 去掉前后的 ---
        body = content[end_pos:]

        # 解析 YAML
        frontmatter = yaml.safe_load(frontmatter_text)
        return frontmatter, body
    except (ValueError, yaml.YAMLError):
        return None, content

def format_frontmatter(frontmatter):
    """格式化 frontmatter 为字符串"""
    if not frontmatter:
        return ""

    yaml_str = yaml.dump(frontmatter, default_flow_style=False, allow_unicode=True, sort_keys=False)
    return f"---\n{yaml_str}---\n\n"

def find_split_points(body):
    """找到合适的分拆点，优先章节标题，次选段落边界"""
    lines = body.split('\n')
    split_points = []

    # 优先级1: 章节标题 (## 级别及以上)
    for i, line in enumerate(lines):
        if re.match(r'^#{1,3}\s+', line.strip()):
            split_points.append(('chapter', i, line.strip()))

    # 优先级2: 段落边界 (连续空行)
    for i in range(len(lines) - 1):
        if lines[i].strip() == '' and i > 0 and lines[i-1].strip() != '':
            # 找到段落结束点
            split_points.append(('paragraph', i, ''))

    # 按行号排序
    split_points.sort(key=lambda x: x[1])
    return split_points

def calculate_size(text):
    """计算文本的字节大小"""
    return len(text.encode('utf-8'))

def split_content(frontmatter, body, size_limit):
    """根据大小限制拆分内容"""
    split_points = find_split_points(body)
    lines = body.split('\n')

    chunks = []
    current_start = 0

    for i, (point_type, line_num, line_content) in enumerate(split_points):
        # 计算当前块的大小
        current_chunk = '\n'.join(lines[current_start:line_num])
        chunk_size = calculate_size(format_frontmatter(frontmatter) + current_chunk)

        # 如果当前块超过大小限制，或者是最优分拆点
        if chunk_size > size_limit or (point_type == 'chapter' and chunk_size > size_limit * 0.1):
            if current_start < line_num:
                chunks.append('\n'.join(lines[current_start:line_num]))
                current_start = line_num

    # 添加最后一块
    if current_start < len(lines):
        chunks.append('\n'.join(lines[current_start:]))

    # 如果没有分拆点或者只有一块，直接返回整个内容
    if len(chunks) <= 1:
        return [body]

    return chunks

def generate_filename(total_count, index):
    """生成带前导零的文件名"""
    digits = len(str(total_count))
    return f"{index+1:0{digits}d}.md"

def split_markdown_file(file_path, size_limit=DEFAULT_SIZE_LIMIT):
    """拆分 markdown 文件"""
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")

    # 读取文件内容
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 解析 frontmatter
    frontmatter, body = parse_frontmatter(content)

    # 如果文件小于限制大小，不需要拆分
    if calculate_size(content) <= size_limit:
        print(f"文件 {file_path.name} 大小未超过限制，无需拆分")
        return

    # 拆分内容
    chunks = split_content(frontmatter, body, size_limit)

    if len(chunks) <= 1:
        print(f"文件 {file_path.name} 无法找到合适的拆分点")
        return

    # 创建输出目录
    output_dir = file_path.parent / file_path.stem
    output_dir.mkdir(exist_ok=True)

    # 生成拆分后的文件
    for i, chunk in enumerate(chunks):
        # 更新 frontmatter
        if frontmatter:
            chunk_frontmatter = frontmatter.copy()
            chunk_frontmatter['chapter_count'] = i + 1
            chunk_frontmatter['total_chapters'] = len(chunks)
        else:
            chunk_frontmatter = {
                'chapter_count': i + 1,
                'total_chapters': len(chunks)
            }

        # 生成文件内容
        file_content = format_frontmatter(chunk_frontmatter) + chunk.strip()

        # 写入文件
        output_file = output_dir / generate_filename(len(chunks), i)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(file_content)

        print(f"生成文件: {output_file} ({calculate_size(file_content)} bytes)")

    print(f"拆分完成: {file_path.name} -> {len(chunks)} 个文件")
    print(f"输出目录: {output_dir}")

def main():
    parser = argparse.ArgumentParser(
        description='拆分大型 markdown 文件',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  # 拆分单个文件 (使用默认大小限制 500KB)
  python3 scripts/split_markdown.py "子藏/兵家/八阵合变图说.md"

  # 指定自定义大小限制 (300KB)
  python3 scripts/split_markdown.py "子藏/兵家/八阵合变图说.md" --size 300
        '''
    )

    parser.add_argument('file_path', help='要拆分的 markdown 文件路径')
    parser.add_argument('--size', '-s', type=int, default=DEFAULT_SIZE_LIMIT//1024,
                       help=f'大小限制 (KB, 默认: {DEFAULT_SIZE_LIMIT//1024})')

    args = parser.parse_args()

    try:
        size_limit = args.size * 1024  # 转换为字节
        split_markdown_file(args.file_path, size_limit)
    except Exception as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()