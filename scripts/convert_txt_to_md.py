#!/usr/bin/env python3
"""
TXT to Markdown 1:1 保真转换脚本

目标：输入什么样的文本，输出的 Markdown 渲染后就是什么样
- 保持所有换行（包括单换行）
- 转义所有可能影响显示的 Markdown 特殊字符
- 确保渲染结果与原文本完全一致
- 添加 YAML frontmatter
"""

import re
import sys
import os
import subprocess
from urllib.parse import quote
from datetime import datetime

# GitHub 仓库基础 URL
GITHUB_REPO_BASE_URL = "https://github.com/frankslin/daizhigev20/blob/master/"

# 文件大小阈值（字节），超过此大小不转换
MAX_FILE_SIZE = 1024 * 1024  # 1MB


def get_file_last_commit_time(file_path):
    """
    获取文件的最后一次 git commit 的作者时间戳
    """
    try:
        # 获取 git 仓库根目录
        git_root_result = subprocess.run(
            ['git', 'rev-parse', '--show-toplevel'],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.abspath(file_path)) or '.'
        )

        if git_root_result.returncode != 0:
            print("警告：无法找到 git 仓库根目录，使用当前时间")
            return datetime.now().isoformat() + 'Z'

        git_root = git_root_result.stdout.strip()

        # 计算文件相对于 git 根目录的路径
        abs_file_path = os.path.abspath(file_path)
        try:
            rel_path = os.path.relpath(abs_file_path, git_root)
        except ValueError:
            # 文件不在 git 仓库中
            print(f"警告：文件 '{file_path}' 不在 git 仓库中，使用当前时间")
            return datetime.now().isoformat() + 'Z'

        print(f"调试：git 根目录 '{git_root}'，相对路径 '{rel_path}'")

        # 使用 git log 获取文件的最后提交时间（作者时间）
        result = subprocess.run(
            ['git', 'log', '-1', '--format=%ai', rel_path],
            capture_output=True,
            text=True,
            cwd=git_root
        )

        if result.returncode == 0 and result.stdout.strip():
            # 解析 git 输出的时间格式，转换为 ISO 格式
            git_time_str = result.stdout.strip()
            print(f"调试：获取到 git 时间 '{git_time_str}'")
            # git 输出格式类似: "2023-12-01 10:30:45 +0800"
            # 需要转换为 ISO 格式
            try:
                # 解析完整的时间字符串（包含时区）
                # 格式: "2018-11-26 18:18:40 -0500"
                from datetime import timezone, timedelta

                # 分离时间和时区部分
                parts = git_time_str.rsplit(' ', 1)
                time_part = parts[0]  # "2018-11-26 18:18:40"
                tz_part = parts[1]    # "-0500"

                # 解析基础时间
                dt = datetime.strptime(time_part, '%Y-%m-%d %H:%M:%S')

                # 解析时区偏移
                tz_sign = 1 if tz_part[0] == '+' else -1
                tz_hours = int(tz_part[1:3])
                tz_minutes = int(tz_part[3:5])
                tz_offset = timedelta(hours=tz_hours, minutes=tz_minutes)
                tz = timezone(tz_sign * tz_offset)

                # 添加时区信息
                dt_with_tz = dt.replace(tzinfo=tz)

                # 转换为 UTC 并输出 ISO 格式
                dt_utc = dt_with_tz.astimezone(timezone.utc)
                iso_time = dt_utc.isoformat().replace('+00:00', 'Z')
                print(f"调试：转换为 ISO 时间 '{iso_time}'")
                return iso_time
            except ValueError:
                print(f"警告：无法解析 git 时间格式 '{git_time_str}'，使用当前时间")
                return datetime.now().isoformat() + 'Z'
        else:
            print(f"警告：git 命令返回码 {result.returncode}，输出：'{result.stdout}'，错误：'{result.stderr}'")
            print(f"警告：无法获取文件 '{file_path}' 的 git 提交时间，使用当前时间")
            return datetime.now().isoformat() + 'Z'

    except FileNotFoundError:
        print("警告：git 命令未找到，使用当前时间")
        return datetime.now().isoformat() + 'Z'
    except Exception as e:
        print(f"警告：获取 git 提交时间时出错 - {e}，使用当前时间")
        return datetime.now().isoformat() + 'Z'


def escape_markdown_special_chars(text):
    """
    转义 Markdown 特殊字符，确保它们显示为普通文本
    """
    
    # 必须最先处理反斜杠，因为它是转义字符本身
    text = text.replace('\\', '\\\\')
    
    # 需要转义的字符列表
    escape_chars = [
        ('`', '\\`'),           # 代码标记
        ('*', '\\*'),           # 强调/列表
        ('_', '\\_'),           # 强调
        ('#', '\\#'),           # 标题
        ('+', '\\+'),           # 列表
        ('-', '\\-'),           # 列表/分隔线
        ('=', '\\='),           # setext 标题
        ('!', '\\!'),           # 图片
        ('[', '\\['),           # 链接开始
        (']', '\\]'),           # 链接结束
        ('(', '\\('),           # 链接URL开始
        (')', '\\)'),           # 链接URL结束
        ('{', '\\{'),           # 扩展语法
        ('}', '\\}'),           # 扩展语法
        ('|', '\\|'),           # 表格
        ('~', '\\~'),           # 删除线
        ('^', '\\^'),           # 上标
        ('<', '\\<'),           # HTML标签
        ('>', '\\>'),           # 引用/HTML标签
        ('$', '\\$'),           # 数学公式
        ('%', '\\%'),           # 某些扩展
        ('&', '\\&'),           # HTML实体
    ]
    
    # 逐个转义
    for char, escaped in escape_chars:
        text = text.replace(char, escaped)
    
    return text


def process_line_for_markdown(line):
    """
    处理单行文本，使其在 Markdown 中正确显示
    """
    
    # 移除行尾的换行符
    line = line.rstrip('\n\r')
    
    # 转义特殊字符
    line = escape_markdown_special_chars(line)
    
    # 处理行首的数字+点号组合（可能被误识别为有序列表）
    line = re.sub(r'^(\s*)(\d+)(\.)(\s)', r'\1\2\\\3\4', line)
    
    # 处理可能被误识别为分隔线的连续字符
    if re.match(r'^\s*([*\-_])\s*\1\s*\1', line.strip()):
        line = re.sub(r'^(\s*)([*\-_])', r'\1\\\2', line)
    
    return line


def generate_frontmatter(input_file):
    """
    生成 YAML frontmatter
    """
    # 获取文件名（不含扩展名）
    filename_without_ext = os.path.splitext(os.path.basename(input_file))[0]

    # 获取路径（相对路径，不含文件名）
    file_dir = os.path.dirname(input_file)
    # 标准化路径，移除 '.' 和处理相对路径
    if file_dir and file_dir != '.':
        # 移除开头的 './' 并统一使用 / 分隔符
        file_dir = file_dir.replace('\\', '/')
        if file_dir.startswith('./'):
            file_dir = file_dir[2:]
        category = '/' + file_dir if file_dir else '/'
    else:
        category = '/'  # 根目录

    # 生成 GitHub 仓库 URL（将 .txt 改为 .md）
    # 将路径中的中文进行 URL 编码
    if file_dir and file_dir != '.' and file_dir:
        # 处理相对路径
        clean_dir = file_dir
        if clean_dir.startswith('./'):
            clean_dir = clean_dir[2:]
        if clean_dir:
            encoded_path = '/'.join(quote(part, safe='') for part in clean_dir.split('/'))
            encoded_filename = quote(filename_without_ext + '.md', safe='')
            github_repo_url = GITHUB_REPO_BASE_URL + encoded_path + '/' + encoded_filename
        else:
            encoded_filename = quote(filename_without_ext + '.md', safe='')
            github_repo_url = GITHUB_REPO_BASE_URL + encoded_filename
    else:
        encoded_filename = quote(filename_without_ext + '.md', safe='')
        github_repo_url = GITHUB_REPO_BASE_URL + encoded_filename

    # 获取文件的最后 git commit 时间戳
    lastmod = get_file_last_commit_time(input_file)
    # 生成当前日期（精确到日）
    current_date = datetime.now().strftime('%Y-%m-%d')

    frontmatter = f"""---
title:
  zh-hans: {filename_without_ext}
category: {category}
lastmod: {lastmod}
github_repo_url: {github_repo_url}
additional_info:
  - {current_date} 转换自「殆知阁」GitHub 仓库中的 txt 版本
---

"""
    return frontmatter


def txt_to_markdown_1to1(input_file, output_file):
    """
    将文本文件转换为 Markdown，确保渲染结果与原文完全一致
    """
    
    # 尝试不同编码读取文件
    encodings = ['utf-8', 'gbk', 'gb2312', 'utf-16', 'latin1']
    content = None
    used_encoding = None
    
    for encoding in encodings:
        try:
            with open(input_file, 'r', encoding=encoding) as f:
                content = f.read()
            used_encoding = encoding
            break
        except UnicodeDecodeError:
            continue
    
    if content is None:
        raise ValueError(f"无法读取文件 {input_file}，尝试了编码：{encodings}")
    
    print(f"使用编码 {used_encoding} 读取文件")
    
    # 生成 frontmatter
    frontmatter = generate_frontmatter(input_file)
    
    # 按行处理，保持原有的段落结构
    lines = content.splitlines()
    processed_lines = []
    
    i = 0
    while i < len(lines):
        line = lines[i]
        processed_line = process_line_for_markdown(line)
        
        if processed_line.strip():  # 非空行
            # 收集连续的非空行作为一个段落
            paragraph_lines = [processed_line]
            i += 1
            
            # 继续收集后续的非空行
            while i < len(lines) and lines[i].strip():
                next_line = process_line_for_markdown(lines[i])
                paragraph_lines.append(next_line)
                i += 1
            
            # 将段落中的行用硬换行连接
            if len(paragraph_lines) == 1:
                processed_lines.append(paragraph_lines[0])
            else:
                paragraph_text = '  \n'.join(paragraph_lines)
                processed_lines.append(paragraph_text)
            
            # 添加段落后的空行
            processed_lines.append('')
        else:  # 空行
            if not processed_lines or processed_lines[-1] != '':
                processed_lines.append('')
            i += 1
    
    # 移除最后多余的空行
    while processed_lines and processed_lines[-1] == '':
        processed_lines.pop()
    
    # 组合 frontmatter 和内容
    markdown_content = '\n'.join(processed_lines)
    final_content = frontmatter + markdown_content
    
    # 写入输出文件
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(final_content)
    
    return len(lines), used_encoding


def main():
    """主函数"""
    
    if len(sys.argv) < 2:
        print("用法: python3 txt2md.py <输入文件.txt> [输出文件.md]")
        print()
        print("示例:")
        print("  python3 txt2md.py document.txt")
        print("  python3 txt2md.py document.txt output.md")
        print()
        print("功能:")
        print("  将文本文件转换为 Markdown，确保渲染结果与原文完全一致")
        print("  - 添加 YAML frontmatter（包含标题和分类信息）")
        print("  - 保持原有的段落结构")
        print("  - 转义所有 Markdown 特殊字符")
        print("  - 1:1 保真转换")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    # 如果没有指定输出文件，自动生成
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    else:
        base_name = os.path.splitext(input_file)[0]
        output_file = base_name + '.md'
    
    # 检查输入文件是否存在
    if not os.path.exists(input_file):
        print(f"错误：输入文件 '{input_file}' 不存在")
        sys.exit(1)

    # 检查输入文件大小
    file_size = os.path.getsize(input_file)
    if file_size > MAX_FILE_SIZE:
        print(f"错误：输入文件 '{input_file}' 大小为 {file_size:,} 字节，超过最大限制 {MAX_FILE_SIZE:,} 字节（100KB）")
        sys.exit(1)

    # 检查输出文件是否已存在
    if os.path.exists(output_file):
        # 特殊情况：如果输入和输出文件内容相同，允许覆盖
        try:
            # 读取两个文件内容进行比较
            with open(input_file, 'rb') as f1, open(output_file, 'rb') as f2:
                if f1.read() == f2.read():
                    print(f"注意：输入文件 '{input_file}' 和输出文件 '{output_file}' 内容相同，将直接覆盖")
                else:
                    print(f"错误：输出文件 '{output_file}' 已存在且内容不同，不会覆盖")
                    sys.exit(1)
        except Exception as e:
            print(f"错误：无法比较文件内容 - {e}")
            print(f"输出文件 '{output_file}' 已存在，不会覆盖")
            sys.exit(1)
    
    try:
        print(f"正在转换 '{input_file}' 到 '{output_file}'...")
        
        line_count, encoding = txt_to_markdown_1to1(input_file, output_file)
        
        print(f"✅ 转换完成！")
        print(f"   - 处理了 {line_count} 行")
        print(f"   - 使用编码：{encoding}")
        print(f"   - 输出文件：{output_file}")
        
    except Exception as e:
        print(f"❌ 转换失败：{e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
