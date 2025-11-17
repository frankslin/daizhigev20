#!/usr/bin/env python3
import os
import sys
import argparse

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

  # 更新 .md 文件
  python3 scripts/update_specific_files.py "佛藏/续藏经/印度撰述/经部/法华部/观世音菩萨救苦经.md"

  # 从文件读取列表（向后兼容）
  python3 scripts/update_specific_files.py --from-file files_to_update.txt
        '''
    )

    parser.add_argument('files', nargs='*', help='要更新的文件路径列表（相对于数据目录）')
    parser.add_argument('--from-file', '-f', help='从文件中读取要更新的文件列表（向后兼容）')

    parser.add_argument('--data-dir', default='./data',
                       help='数据目录路径 (默认: ./data)')
    parser.add_argument('--index', default='chinese-classics',
                       help='Elasticsearch 索引名称 (默认: chinese-classics)')

    return parser.parse_args()

def read_files_from_file(file_list_path):
    """从文件中读取要更新的文件列表（向后兼容功能）"""
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

def main():
    args = parse_arguments()

    # 获取要更新的文件列表
    if args.from_file:
        # 从文件读取（向后兼容）
        files_to_update = read_files_from_file(args.from_file)
    else:
        # 从命令行参数读取
        files_to_update = args.files
        print(f"📄 通过命令行参数指定了 {len(files_to_update)} 个文件")

    if not files_to_update:
        print("❌ 没有找到需要更新的文件")
        print("请使用以下方式之一:")
        print("  1. 直接指定文件: python3 scripts/update_specific_files.py \"文件1.txt\" \"文件2.txt\"")
        print("  2. 从文件读取: python3 scripts/update_specific_files.py --from-file files_list.txt")
        return

    es = import_classics.connect_to_elasticsearch()
    if not es:
        return

    data_dir = args.data_dir
    index_name = args.index
    
    print(f"🚀 更新 {len(files_to_update)} 个指定文件...")
    
    successful = 0
    failed = 0
    
    for i, relative_path in enumerate(files_to_update, 1):
        # 自动去除路径开头的 data_dir 前缀（如果存在）
        # 这样支持两种用法：
        # 1. python script.py "子藏/类书/xxx.md"
        # 2. python script.py "data/子藏/类书/xxx.md"
        clean_path = relative_path
        for prefix in [data_dir + '/', data_dir]:
            if clean_path.startswith(prefix):
                clean_path = clean_path[len(prefix):]
                if clean_path.startswith('/'):
                    clean_path = clean_path[1:]
                break

        filepath = os.path.join(data_dir, clean_path)
        print(f"[{i}/{len(files_to_update)}] 处理: {clean_path}")
        
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
