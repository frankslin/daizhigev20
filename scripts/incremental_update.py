#!/usr/bin/env python3
"""
根据文件列表批量增量更新 Elasticsearch 索引（bulk upsert / delete）。
配合 `git diff --name-status` 的输出使用：文件列表里存在于磁盘的路径按
upsert 处理，磁盘上已不存在的路径按 delete 处理。
"""

import argparse
import hashlib
import os

from elasticsearch.helpers import bulk

import import_classics


def compute_doc_id(relative_path):
    id_path = relative_path
    if relative_path.lower().endswith('.md'):
        id_path = relative_path[:-3] + '.txt'
    return hashlib.md5(id_path.encode('utf-8')).hexdigest()


def read_file_list(path):
    files = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                files.append(line)
    return files


def build_actions(files, data_dir, index_name, stats):
    for relative_path in files:
        filepath = os.path.join(data_dir, relative_path)

        if not os.path.exists(filepath):
            doc_id = compute_doc_id(relative_path)
            stats['delete'] += 1
            yield {'_op_type': 'delete', '_index': index_name, '_id': doc_id}
            continue

        if filepath.lower().endswith('.md'):
            doc = import_classics.process_markdown_file(filepath, data_dir)
        else:
            doc = import_classics.process_text_file(filepath, data_dir)

        if doc:
            stats['upsert'] += 1
            yield {
                '_op_type': 'index',
                '_index': index_name,
                '_id': doc['_id'],
                '_source': doc['_source'],
            }
        else:
            stats['skipped'] += 1


def main():
    parser = argparse.ArgumentParser(description='按文件列表批量增量更新 ES 索引')
    parser.add_argument('--from-file', '-f', required=True, help='文件列表（每行一个相对 data-dir 的路径）')
    parser.add_argument('--data-dir', default='./data')
    parser.add_argument('--index', default='chinese-classics')
    parser.add_argument('--batch-size', type=int, default=100)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    files = read_file_list(args.from_file)
    print(f"📄 文件列表: {len(files)} 条")

    if args.dry_run:
        exists = sum(1 for r in files if os.path.exists(os.path.join(args.data_dir, r)))
        print(f"  将 upsert(存在于磁盘): {exists}")
        print(f"  将 delete(磁盘已不存在): {len(files) - exists}")
        return

    es = import_classics.connect_to_elasticsearch()
    if not es:
        return

    stats = {'upsert': 0, 'delete': 0, 'skipped': 0}
    actions = build_actions(files, args.data_dir, args.index, stats)

    success, errors = bulk(
        es, actions,
        request_timeout=180,
        max_retries=3,
        chunk_size=args.batch_size,
        raise_on_error=False,
        stats_only=False,
    )

    print(f"\n✅ 成功写入: {success}")
    print(f"   其中 upsert 尝试: {stats['upsert']}, delete 尝试: {stats['delete']}, 处理失败跳过: {stats['skipped']}")
    if errors:
        print(f"⚠️  失败 {len(errors)} 条，前 5 条:")
        for e in errors[:5]:
            print(f"   {e}")


if __name__ == '__main__':
    main()
