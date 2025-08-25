#!/usr/bin/env python3
"""
中国古典文献数字化资料导入Elasticsearch
专门处理daizhigev20目录下的传统典籍数据
"""

import os
import json
import hashlib
import re
from pathlib import Path
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import argparse
from datetime import datetime

# 典籍分类映射
COLLECTION_MAPPING = {
    '佛藏': 'Buddhist Texts',
    '儒藏': 'Confucian Classics', 
    '医藏': 'Medical Texts',
    '史藏': 'Historical Records',
    '子藏': 'Masters Literature',
    '易藏': 'I Ching Studies',
    '艺藏': 'Arts & Crafts',
    '诗藏': 'Poetry Collection',
    '道藏': 'Taoist Texts',
    '集藏': 'Collected Works'
}

def connect_to_elasticsearch():
    """连接到Elasticsearch"""
    try:
        es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        if es.ping():
            print("✅ 成功连接到Elasticsearch")
            cluster_info = es.info()
            print(f"集群信息: {cluster_info['cluster_name']} - 版本: {cluster_info['version']['number']}")
            return es
        else:
            print("❌ 无法连接到Elasticsearch")
            return None
    except Exception as e:
        print(f"❌ 连接错误: {e}")
        return None

def extract_text_metadata(content, filepath):
    """从文本内容中提取元数据"""
    metadata = {}
    
    # 尝试提取书名（通常在开头几行）
    lines = content.split('\n')[:20]
    for line in lines:
        line = line.strip()
        if line and len(line) < 50:
            # 可能是书名或章节名
            if any(char in line for char in ['卷', '篇', '章', '第']):
                metadata['chapter'] = line
                break
            elif len(line) < 20 and not any(char in line for char in ['。', '，', '、']):
                metadata['title'] = line
                break
    
    # 从文件路径提取书籍信息
    path_parts = filepath.split(os.sep)
    if len(path_parts) >= 3:
        # 找到藏的名称
        collection = '未知'
        book_category = ''
        for i, part in enumerate(path_parts):
            if part in COLLECTION_MAPPING:
                collection = part
                if i + 1 < len(path_parts):
                    book_category = path_parts[i + 1]
                break
        
        filename = path_parts[-1]
        
        metadata['collection'] = collection
        metadata['collection_en'] = COLLECTION_MAPPING.get(collection, collection)
        metadata['book_category'] = book_category
        metadata['filename'] = filename
    
    # 估算文本特征
    metadata['char_count'] = len(content)
    metadata['line_count'] = len(content.split('\n'))
    
    # 检测是否包含古文特征
    classical_indicators = ['曰', '者', '也', '矣', '焉', '乎', '哉', '耶']
    classical_score = sum(content.count(char) for char in classical_indicators)
    metadata['classical_score'] = classical_score
    metadata['is_classical'] = classical_score > 10
    
    return metadata

def process_text_file(filepath, base_dir):
    """处理单个古典文献文件"""
    try:
        # 获取相对路径
        relative_path = os.path.relpath(filepath, base_dir)
        
        # 读取文件内容，处理各种编码
        content = None
        encodings = ['utf-8', 'gb2312', 'gbk', 'gb18030']
        
        for encoding in encodings:
            try:
                with open(filepath, 'r', encoding=encoding, errors='ignore') as f:
                    content = f.read()
                break
            except UnicodeDecodeError:
                continue
        
        if not content:
            print(f"❌ 无法读取文件: {filepath}")
            return None
        
        # 清理文本内容
        content = content.strip()
        if not content:
            return None
        
        # 提取元数据
        metadata = extract_text_metadata(content, relative_path)
        
        # 如果文件太大，进行智能截取
        if len(content) > 50000:
            # 保留前30000字符和后5000字符
            content = content[:30000] + "\n\n...[文档过长，已截取中间部分]...\n\n" + content[-5000:]
            metadata['truncated'] = True
        else:
            metadata['truncated'] = False
        
        # 生成文档ID
        doc_id = hashlib.md5(relative_path.encode('utf-8')).hexdigest()
        
        # 构建Elasticsearch文档
        doc = {
            '_id': doc_id,
            '_source': {
                'title': metadata.get('title', os.path.splitext(metadata['filename'])[0]),
                'chapter': metadata.get('chapter', ''),
                'collection': metadata['collection'],
                'collection_en': metadata['collection_en'],
                'book_category': metadata['book_category'],
                'content': content,
                'filepath': relative_path,
                'filename': metadata['filename'],
                'char_count': metadata['char_count'],
                'line_count': metadata['line_count'],
                'is_classical': metadata['is_classical'],
                'classical_score': metadata['classical_score'],
                'truncated': metadata['truncated'],
                'file_size': os.path.getsize(filepath),
                'indexed_at': datetime.now().isoformat()
            }
        }
        
        return doc
        
    except Exception as e:
        print(f"❌ 处理文件失败 {filepath}: {e}")
        return None

def create_chinese_classics_index(es, index_name):
    """创建专门用于中国古典文献的索引"""
    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "max_result_window": 50000,
            "analysis": {
                "analyzer": {
                    "chinese_text_analyzer": {
                        "type": "standard",
                        "stopwords": ["的", "了", "在", "是", "我", "有", "和", "就", "不", "人", "都", "一", "一个", "上", "也", "很", "到", "说", "要", "去", "你", "会", "着", "没有", "看", "好", "自己", "这"]
                    },
                    "classical_chinese_analyzer": {
                        "type": "keyword"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "title": {
                    "type": "text",
                    "analyzer": "chinese_text_analyzer",
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                "chapter": {
                    "type": "text", 
                    "analyzer": "chinese_text_analyzer"
                },
                "collection": {"type": "keyword"},
                "collection_en": {"type": "keyword"},
                "book_category": {"type": "keyword"},
                "content": {
                    "type": "text",
                    "analyzer": "chinese_text_analyzer"
                },
                "filepath": {"type": "keyword"},
                "filename": {"type": "keyword"},
                "char_count": {"type": "integer"},
                "line_count": {"type": "integer"},
                "is_classical": {"type": "boolean"},
                "classical_score": {"type": "integer"},
                "truncated": {"type": "boolean"},
                "file_size": {"type": "long"},
                "indexed_at": {"type": "date"}
            }
        }
    }
    
    try:
        if es.indices.exists(index=index_name):
            print(f"📝 索引 {index_name} 已存在")
            # 可选：删除并重建索引
            # es.indices.delete(index=index_name)
            # es.indices.create(index=index_name, body=mapping)
            # print(f"🔄 重建索引 {index_name}")
        else:
            es.indices.create(index=index_name, body=mapping)
            print(f"✅ 创建索引 {index_name}")
    except Exception as e:
        print(f"❌ 创建索引失败: {e}")

def bulk_index_documents(es, documents, index_name):
    """批量索引文档"""
    try:
        actions = []
        for doc in documents:
            if doc:
                action = {
                    '_index': index_name,
                    '_id': doc['_id'],
                    '_source': doc['_source']
                }
                actions.append(action)
        
        if actions:
            success, failed = bulk(es, actions, request_timeout=120, max_retries=3)
            if failed:
                print(f"❌ 失败 {len(failed)} 个文档")
            return success
        
        return 0
        
    except Exception as e:
        print(f"❌ 批量索引失败: {e}")
        return 0

def show_import_stats(es, index_name, data_dir):
    """显示导入统计信息"""
    try:
        # 基本统计
        stats = es.indices.stats(index=index_name)
        doc_count = stats['indices'][index_name]['total']['docs']['count']
        size_mb = stats['indices'][index_name]['total']['store']['size_in_bytes'] / 1024 / 1024
        
        print(f"\n📊 导入完成统计:")
        print(f"  总文档数: {doc_count}")
        print(f"  索引大小: {size_mb:.2f} MB")
        
        # 按藏统计
        search_result = es.search(
            index=index_name,
            body={
                "aggs": {
                    "by_collection": {
                        "terms": {
                            "field": "collection",
                            "size": 20
                        }
                    },
                    "by_classical": {
                        "terms": {
                            "field": "is_classical"
                        }
                    }
                },
                "size": 0
            }
        )
        
        print(f"\n📚 各藏分布:")
        for bucket in search_result['aggregations']['by_collection']['buckets']:
            collection = bucket['key']
            count = bucket['doc_count']
            en_name = COLLECTION_MAPPING.get(collection, collection)
            print(f"  {collection} ({en_name}): {count} 个文档")
        
        # 古文统计
        classical_stats = search_result['aggregations']['by_classical']['buckets']
        classical_true = next((b['doc_count'] for b in classical_stats if b['key']), 0)
        print(f"\n📜 古典文献特征:")
        print(f"  古文文档: {classical_true} 个")
        print(f"  现代文档: {doc_count - classical_true} 个")
        
        # 测试搜索
        print(f"\n🔍 搜索功能测试:")
        test_queries = ["史", "道", "医", "诗"]
        for query in test_queries:
            result = es.search(
                index=index_name,
                body={
                    "query": {"match": {"content": query}},
                    "size": 0
                }
            )
            count = result['hits']['total']['value']
            print(f"  '{query}': {count} 个相关文档")
        
    except Exception as e:
        print(f"❌ 统计信息获取失败: {e}")

def main():
    parser = argparse.ArgumentParser(description='导入中国古典文献到Elasticsearch')
    parser.add_argument('--dir', default='/home/ubuntu/daizhigev20', help='数据目录路径')
    parser.add_argument('--index', default='chinese-classics', help='Elasticsearch索引名')
    parser.add_argument('--batch-size', type=int, default=50, help='批量处理大小')
    parser.add_argument('--dry-run', action='store_true', help='仅扫描不导入')
    
    args = parser.parse_args()
    
    # 连接Elasticsearch
    es = connect_to_elasticsearch()
    if not es:
        return
    
    data_dir = args.dir
    print(f"🔍 扫描目录: {data_dir}")
    
    # 收集所有txt文件
    all_files = []
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.lower().endswith('.txt'):
                filepath = os.path.join(root, file)
                all_files.append(filepath)
    
    print(f"📊 找到 {len(all_files)} 个文本文件")
    
    # 按藏分组显示统计
    collection_stats = {}
    total_size = 0
    for filepath in all_files:
        parts = filepath.split(os.sep)
        collection_found = False
        for part in parts:
            if part in COLLECTION_MAPPING:
                collection = part
                collection_stats[collection] = collection_stats.get(collection, 0) + 1
                collection_found = True
                break
        if not collection_found:
            collection_stats['其他'] = collection_stats.get('其他', 0) + 1
        
        try:
            total_size += os.path.getsize(filepath)
        except:
            pass
    
    print(f"\n📚 各藏文件统计 (总大小: {total_size/1024/1024:.2f} MB):")
    for collection, count in sorted(collection_stats.items()):
        if collection in COLLECTION_MAPPING:
            en_name = COLLECTION_MAPPING[collection]
            print(f"  {collection} ({en_name}): {count} 个文件")
        else:
            print(f"  {collection}: {count} 个文件")
    
    if args.dry_run:
        print("\n🔍 干运行完成，未进行实际导入")
        return
    
    # 创建索引
    create_chinese_classics_index(es, args.index)
    
    # 批量处理文件
    total_indexed = 0
    batch_docs = []
    
    print(f"\n🚀 开始导入数据...")
    
    for i, filepath in enumerate(all_files, 1):
        filename = os.path.basename(filepath)
        
        # 找到所属的藏
        collection = '未知'
        for part in filepath.split(os.sep):
            if part in COLLECTION_MAPPING:
                collection = part
                break
        
        if i % 50 == 1 or i <= 10:  # 前10个和每50个文件显示一次进度
            print(f"📝 [{i:5d}/{len(all_files)}] 处理: {collection}/{filename}")
        
        doc = process_text_file(filepath, data_dir)
        if doc:
            batch_docs.append(doc)
        
        # 达到批量大小或是最后一批
        if len(batch_docs) >= args.batch_size or i == len(all_files):
            indexed = bulk_index_documents(es, batch_docs, args.index)
            total_indexed += indexed
            batch_docs = []
            
            if i % 200 == 0 or i == len(all_files):  # 每200个文件显示一次总进度
                print(f"📈 进度: {i}/{len(all_files)} ({i/len(all_files)*100:.1f}%) - 已索引: {total_indexed}")
    
    print(f"\n🎉 导入完成！总计索引: {total_indexed} 个文档")
    
    # 显示详细统计
    show_import_stats(es, args.index, data_dir)

if __name__ == "__main__":
    main()
