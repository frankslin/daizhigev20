#!/usr/bin/env python3
"""
从 Elasticsearch 索引中移除指定文件
支持单个文件或批量删除
"""

import os
import sys
import hashlib
import argparse

import import_classics


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='从 Elasticsearch 索引中删除指定文件',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  # 删除单个文件
  python3 scripts/remove_from_es.py "史藏/正史/南史.txt"

  # 删除多个文件
  python3 scripts/remove_from_es.py "史藏/正史/南史.txt" "集藏/演义/清朝秘史.txt"

  # 删除 .md 文件（会自动匹配对应的 .txt ID）
  python3 scripts/remove_from_es.py "佛藏/续藏经/印度撰述/经部/法华部/观世音菩萨救苦经.md"

  # 从文件读取列表
  python3 scripts/remove_from_es.py --from-file files_to_remove.txt

  # 先预览不实际删除
  python3 scripts/remove_from_es.py "史藏/正史/南史.txt" --dry-run
        '''
    )

    parser.add_argument('files', nargs='*', help='要删除的文件路径列表（相对于数据目录）')
    parser.add_argument('--from-file', '-f', help='从文件中读取要删除的文件列表')

    parser.add_argument('--data-dir', default='./data',
                       help='数据目录路径 (默认: ./data)')
    parser.add_argument('--index', default='chinese-classics',
                       help='Elasticsearch 索引名称 (默认: chinese-classics)')
    parser.add_argument('--dry-run', action='store_true',
                       help='仅显示将要删除的文档，不实际执行删除')

    return parser.parse_args()


def read_files_from_file(file_list_path):
    """从文件中读取要删除的文件列表"""
    files_to_remove = []

    if not os.path.exists(file_list_path):
        print(f"❌ 文件列表文件不存在: {file_list_path}")
        return files_to_remove

    try:
        with open(file_list_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                # 跳过空行和注释行（以#开头）
                if line and not line.startswith('#'):
                    files_to_remove.append(line)

        print(f"📄 从 {file_list_path} 读取了 {len(files_to_remove)} 个文件")

    except Exception as e:
        print(f"❌ 读取文件列表失败: {e}")
        return []

    return files_to_remove


def generate_doc_id(relative_path):
    """
    生成文档 ID
    与 import_classics.py 中的逻辑保持一致
    .md 文件会转换为 .txt 后缀来生成 ID
    """
    id_path = relative_path
    if relative_path.lower().endswith('.md'):
        id_path = relative_path[:-3] + '.txt'
    doc_id = hashlib.md5(id_path.encode('utf-8')).hexdigest()
    return doc_id, id_path


def check_document_exists(es, index_name, doc_id):
    """检查文档是否存在于索引中"""
    try:
        return es.exists(index=index_name, id=doc_id)
    except Exception as e:
        print(f"  ⚠️  检查文档失败: {e}")
        return False


def get_document_info(es, index_name, doc_id):
    """获取文档信息"""
    try:
        doc = es.get(index=index_name, id=doc_id)
        source = doc.get('_source', {})
        return {
            'title': source.get('title', ''),
            'collection': source.get('collection', ''),
            'file_type': source.get('file_type', 'text'),
            'char_count': source.get('char_count', 0),
            'indexed_at': source.get('indexed_at', '')
        }
    except Exception as e:
        return None


def delete_document(es, index_name, doc_id):
    """删除文档"""
    try:
        result = es.delete(index=index_name, id=doc_id)
        return result.get('result') == 'deleted'
    except Exception as e:
        print(f"  ❌ 删除失败: {e}")
        return False


def main():
    args = parse_arguments()

    # 获取要删除的文件列表
    if args.from_file:
        # 从文件读取
        files_to_remove = read_files_from_file(args.from_file)
    else:
        # 从命令行参数读取
        files_to_remove = args.files
        print(f"📄 通过命令行参数指定了 {len(files_to_remove)} 个文件")

    if not files_to_remove:
        print("❌ 没有找到需要删除的文件")
        print("请使用以下方式之一:")
        print("  1. 直接指定文件: python3 scripts/remove_from_es.py \"文件1.txt\" \"文件2.txt\"")
        print("  2. 从文件读取: python3 scripts/remove_from_es.py --from-file files_list.txt")
        return

    # 连接 Elasticsearch
    es = import_classics.connect_to_elasticsearch()
    if not es:
        return

    index_name = args.index

    # 检查索引是否存在
    if not es.indices.exists(index=index_name):
        print(f"❌ 索引不存在: {index_name}")
        return

    print(f"🗑️  准备从索引 {index_name} 中删除 {len(files_to_remove)} 个文件...")

    if args.dry_run:
        print("⚠️  预览模式：不会实际删除文档\n")
    else:
        print()

    successful = 0
    not_found = 0
    failed = 0

    data_dir = args.data_dir

    for i, relative_path in enumerate(files_to_remove, 1):
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

        doc_id, id_path = generate_doc_id(clean_path)

        print(f"[{i}/{len(files_to_remove)}] {clean_path}")

        # 检查文档是否存在
        if not check_document_exists(es, index_name, doc_id):
            print(f"  ⚠️  文档不存在于索引中")
            not_found += 1
            continue

        # 获取文档信息
        doc_info = get_document_info(es, index_name, doc_id)
        if doc_info:
            print(f"  📄 标题: {doc_info['title']}")
            print(f"  📚 分类: {doc_info['collection']}")
            print(f"  📝 类型: {doc_info['file_type']}")
            print(f"  📊 字符数: {doc_info['char_count']:,}")
            print(f"  🔑 文档 ID: {doc_id}")
            if id_path != relative_path:
                print(f"  ℹ️  ID 路径: {id_path}")

        if args.dry_run:
            print(f"  🔍 [预览] 将会删除此文档")
            successful += 1
        else:
            # 实际删除
            if delete_document(es, index_name, doc_id):
                print(f"  ✅ 删除成功")
                successful += 1
            else:
                print(f"  ❌ 删除失败")
                failed += 1

        print()  # 空行分隔

    # 显示统计
    print("=" * 60)
    if args.dry_run:
        print(f"📊 预览完成:")
        print(f"  可删除: {successful}")
        print(f"  不存在: {not_found}")
    else:
        print(f"📊 删除完成:")
        print(f"  成功: {successful}")
        print(f"  不存在: {not_found}")
        print(f"  失败: {failed}")


if __name__ == "__main__":
    main()
