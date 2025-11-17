#!/usr/bin/env python3
import os
import sys
import argparse
import glob

import import_classics

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='更新指定的文件到 Elasticsearch 索引',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  # 更新单个文件
  python3 scripts/update_specific_files.py "史藏/正史/南史.txt"

  # 更新多个文件
  python3 scripts/update_specific_files.py "史藏/正史/南史.txt" "集藏/演义/清朝秘史.txt"

  # 使用通配符更新整个目录
  python3 scripts/update_specific_files.py "子藏/类书/御定佩文韵府/*.md"
  python3 scripts/update_specific_files.py data/子藏/类书/御定佩文韵府/*.md

  # 从文件读取列表
  python3 scripts/update_specific_files.py --from-file files_to_update.txt

  # 添加新文件到列表
  python3 scripts/update_specific_files.py --from-file files.txt --add "新文件1.md" "新文件2.md"
        '''
    )

    parser.add_argument('files', nargs='*', help='要更新的文件路径列表（支持通配符，相对于数据目录）')
    parser.add_argument('--from-file', '-f', help='从文件中读取要更新的文件列表')
    parser.add_argument('--add', nargs='*', help='添加新文件到 --from-file 指定的列表中')

    parser.add_argument('--data-dir', default='./data',
                       help='数据目录路径 (默认: ./data)')
    parser.add_argument('--index', default='chinese-classics',
                       help='Elasticsearch 索引名称 (默认: chinese-classics)')

    return parser.parse_args()

def expand_globs(patterns, data_dir='./data'):
    """
    扩展通配符模式，返回匹配的文件列表
    支持相对路径和绝对路径
    """
    expanded_files = []

    for pattern in patterns:
        # 清理路径前缀 - 支持多种可能的前缀格式
        clean_pattern = pattern

        # 规范化 data_dir 以便比较
        normalized_data_dir = os.path.normpath(data_dir)

        # 尝试各种可能的前缀
        possible_prefixes = [
            normalized_data_dir + '/',
            normalized_data_dir,
            data_dir + '/',
            data_dir,
            'data/',
            './data/',
        ]

        for prefix in possible_prefixes:
            if clean_pattern.startswith(prefix):
                clean_pattern = clean_pattern[len(prefix):]
                # 移除开头的斜杠
                while clean_pattern.startswith('/'):
                    clean_pattern = clean_pattern[1:]
                break

        # 构建完整路径模式
        full_pattern = os.path.join(data_dir, clean_pattern)

        # 使用 glob 扩展
        matches = glob.glob(full_pattern)

        if matches:
            # 转换回相对路径
            for match in matches:
                rel_path = os.path.relpath(match, data_dir)
                expanded_files.append(rel_path)
        else:
            # 如果没有匹配，可能是具体文件名（不含通配符）
            # 直接添加清理后的路径
            expanded_files.append(clean_pattern)

    return expanded_files


def read_files_from_file(file_list_path):
    """从文件中读取要更新的文件列表"""
    files_to_update = []

    if not os.path.exists(file_list_path):
        print(f"❌ 文件列表文件不存在: {file_list_path}")
        return files_to_update

    try:
        with open(file_list_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                # 跳过空行和注释行（以#开头）
                if line and not line.startswith('#'):
                    files_to_update.append(line)

        print(f"📄 从 {file_list_path} 读取了 {len(files_to_update)} 个文件")

    except Exception as e:
        print(f"❌ 读取文件列表失败: {e}")
        return []

    return files_to_update


def add_files_to_list(file_list_path, new_files):
    """添加新文件到文件列表"""
    try:
        # 读取现有文件列表
        existing_files = set()
        if os.path.exists(file_list_path):
            with open(file_list_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        existing_files.add(line)

        # 添加新文件
        added_count = 0
        with open(file_list_path, 'a', encoding='utf-8') as f:
            for new_file in new_files:
                if new_file not in existing_files:
                    f.write(new_file + '\n')
                    existing_files.add(new_file)
                    added_count += 1

        print(f"✅ 添加了 {added_count} 个新文件到 {file_list_path}")
        print(f"📊 文件列表现在包含 {len(existing_files)} 个文件")

        return True
    except Exception as e:
        print(f"❌ 添加文件失败: {e}")
        return False

def main():
    args = parse_arguments()

    data_dir = args.data_dir

    # 处理 --add 参数（添加文件到列表）
    if args.add:
        if not args.from_file:
            print("❌ 使用 --add 时必须同时指定 --from-file")
            return

        # 扩展通配符
        expanded_adds = expand_globs(args.add, data_dir)
        print(f"📝 准备添加 {len(expanded_adds)} 个文件到列表")

        if add_files_to_list(args.from_file, expanded_adds):
            print("✅ 文件添加完成")

        # 如果只是添加文件，不继续更新
        if not args.files:
            return

    # 获取要更新的文件列表
    files_to_update = []

    if args.from_file:
        # 从文件读取
        files_to_update = read_files_from_file(args.from_file)

    if args.files:
        # 从命令行参数读取并扩展通配符
        expanded_files = expand_globs(args.files, data_dir)
        files_to_update.extend(expanded_files)
        print(f"📄 通过命令行参数指定了 {len(expanded_files)} 个文件")

    if not files_to_update:
        print("❌ 没有找到需要更新的文件")
        print("请使用以下方式之一:")
        print("  1. 直接指定文件: python3 scripts/update_specific_files.py \"文件1.txt\" \"文件2.txt\"")
        print("  2. 使用通配符: python3 scripts/update_specific_files.py \"子藏/类书/*/*.md\"")
        print("  3. 从文件读取: python3 scripts/update_specific_files.py --from-file files_list.txt")
        return

    es = import_classics.connect_to_elasticsearch()
    if not es:
        return

    index_name = args.index

    print(f"🚀 更新 {len(files_to_update)} 个指定文件...")

    successful = 0
    failed = 0

    for i, relative_path in enumerate(files_to_update, 1):
        # relative_path 已经是清理过的相对路径（由 expand_globs 处理）
        filepath = os.path.join(data_dir, relative_path)
        print(f"[{i}/{len(files_to_update)}] 处理: {relative_path}")
        
        if not os.path.exists(filepath):
            print(f"  ❌ 文件不存在")
            failed += 1
            continue
        
        # 根据文件类型处理文件
        if filepath.lower().endswith('.md'):
            doc = import_classics.process_markdown_file(filepath, data_dir)
        else:
            doc = import_classics.process_text_file(filepath, data_dir)
        if doc:
            try:
                es.index(index=index_name, id=doc['_id'], document=doc['_source'])
                successful += 1
                print(f"  ✅ 更新成功")
            except Exception as e:
                print(f"  ❌ 索引失败: {e}")
                failed += 1
        else:
            print(f"  ❌ 处理失败")
            failed += 1
    
    print(f"\n📊 完成: 成功 {successful}, 失败 {failed}")

if __name__ == "__main__":
    main()
