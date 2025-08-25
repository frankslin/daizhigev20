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

def safe_read_file(filepath):
    """安全读取文件，处理各种编码"""
    encodings = ['utf-8', 'gb2312', 'gbk', 'gb18030', 'big5', 'cp936']
    
    for encoding in encodings:
        try:
            with open(filepath, 'r', encoding=encoding, errors='ignore') as f:
                content = f.read()
            if content.strip():  # 确保不是空文件
                return content
        except (UnicodeDecodeError, IOError):
            continue
    
    return None

def extract_text_metadata(content, filepath):
    """从文本内容中提取元数据 - 修复版"""
    metadata = {}
    
    try:
        # 安全获取文件基本信息
        filename = os.path.basename(filepath)
        path_parts = filepath.split(os.sep)
        
        # 初始化默认值
        metadata['filename'] = filename
        metadata['collection'] = '未分类'
        metadata['collection_en'] = 'Uncategorized'
        metadata['book_category'] = '其他'
        
        # 从路径中提取藏和分类信息
        for i, part in enumerate(path_parts):
            if part in COLLECTION_MAPPING:
                metadata['collection'] = part
                metadata['collection_en'] = COLLECTION_MAPPING[part]
                
                # 尝试获取书籍分类（藏下面的子目录）
                if i + 1 < len(path_parts) - 1:  # 有子目录且不是最后的文件名
                    metadata['book_category'] = path_parts[i + 1]
                break
        
        # 尝试从文件内容提取标题
        if content:
            lines = content.split('\n')[:30]  # 增加检查行数
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                    
                # 过滤掉明显不是标题的行
                if len(line) > 100 or any(char in line for char in ['http', 'www', '@']):
                    continue
                    
                # 寻找可能的标题
                if len(line) <= 50:
                    # 检查是否是章节标题
                    if any(char in line for char in ['卷', '篇', '章', '第', '序', '跋', '前言']):
                        if 'chapter' not in metadata:
                            metadata['chapter'] = line
                    # 检查是否是书名
                    elif len(line) <= 30 and not any(char in line for char in ['。', '，', '？', '！']):
                        if 'title' not in metadata:
                            metadata['title'] = line
                        break
        
        # 如果没有找到标题，使用文件名（去掉.txt）
        if 'title' not in metadata:
            title = os.path.splitext(filename)[0]
            metadata['title'] = title
            
        # 如果没有找到章节，设为空
        if 'chapter' not in metadata:
            metadata['chapter'] = ''
        
        # 文本统计
        if content:
            metadata['char_count'] = len(content)
            metadata['line_count'] = len(content.split('\n'))
            
            # 古文特征检测
            classical_indicators = ['曰', '者', '也', '矣', '焉', '乎', '哉', '耶', '之', '其', '而']
            classical_score = sum(content.count(char) for char in classical_indicators)
            metadata['classical_score'] = classical_score
            metadata['is_classical'] = classical_score > 20
        else:
            metadata['char_count'] = 0
            metadata['line_count'] = 0
            metadata['classical_score'] = 0
            metadata['is_classical'] = False
        
        return metadata
        
    except Exception as e:
        print(f"⚠️  元数据提取警告 {filepath}: {e}")
        # 返回基本元数据
        return {
            'filename': os.path.basename(filepath),
            'collection': '未分类',
            'collection_en': 'Uncategorized',
            'book_category': '其他',
            'title': os.path.splitext(os.path.basename(filepath))[0],
            'chapter': '',
            'char_count': 0,
            'line_count': 0,
            'classical_score': 0,
            'is_classical': False
        }

def process_text_file(filepath, base_dir):
    """处理单个古典文献文件 - 增强容错版"""
    try:
        # 检查文件是否存在
        if not os.path.exists(filepath):
            print(f"❌ 文件不存在: {filepath}")
            return None
            
        # 检查文件大小
        file_size = os.path.getsize(filepath)
        if file_size == 0:
            print(f"⚠️  跳过空文件: {filepath}")
            return None
            
        # 获取相对路径
        try:
            relative_path = os.path.relpath(filepath, base_dir)
        except Exception as e:
            print(f"⚠️  路径处理错误 {filepath}: {e}")
            relative_path = filepath
        
        # 安全读取文件内容
        content = safe_read_file(filepath)
        if not content:
            print(f"❌ 无法读取文件内容: {filepath}")
            return None
        
        # 清理和验证内容
        content = content.strip()
        if len(content) < 10:  # 内容太少，可能不是有效文档
            print(f"⚠️  跳过内容过少的文件: {filepath}")
            return None
        
        # 提取元数据
        metadata = extract_text_metadata(content, relative_path)
        
        # 处理过长的内容
        original_length = len(content)
        if original_length > 100000:  # 100KB以上截取
            # 保留前60000字符和后20000字符
            content = content[:60000] + f"\n\n...[文档过长({original_length}字符)，已截取中间部分]...\n\n" + content[-20000:]
            metadata['truncated'] = True
            metadata['original_char_count'] = original_length
        else:
            metadata['truncated'] = False
            metadata['original_char_count'] = original_length
        
        # 生成文档ID（使用相对路径确保一致性）
        doc_id = hashlib.md5(relative_path.encode('utf-8')).hexdigest()
        
        # 构建Elasticsearch文档
        doc = {
            '_id': doc_id,
            '_source': {
                'title': metadata['title'],
                'chapter': metadata['chapter'],
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
                'original_char_count': metadata['original_char_count'],
                'file_size': file_size,
                'indexed_at': datetime.now().isoformat()
            }
        }
        
        return doc
        
    except Exception as e:
        print(f"❌ 处理文件失败 {filepath}: {e}")
        return None

def create_chinese_classics_index(es, index_name):
    """创建专门用于中国古典文献的索引（使用IK分词器）"""
    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "max_result_window": 50000,
            "analysis": {
                "analyzer": {
                    "ik_chinese_analyzer": {
                        "type": "ik_max_word"
                    },
                    "ik_chinese_search_analyzer": {
                        "type": "ik_smart"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "title": {
                    "type": "text",
                    "analyzer": "ik_chinese_analyzer",
                    "search_analyzer": "ik_chinese_search_analyzer",
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                "chapter": {
                    "type": "text", 
                    "analyzer": "ik_chinese_analyzer",
                    "search_analyzer": "ik_chinese_search_analyzer"
                },
                "collection": {"type": "keyword"},
                "collection_en": {"type": "keyword"},
                "book_category": {"type": "keyword"},
                "content": {
                    "type": "text",
                    "analyzer": "ik_chinese_analyzer",
                    "search_analyzer": "ik_chinese_search_analyzer"
                },
                "filepath": {"type": "keyword"},
                "filename": {"type": "keyword"},
                "char_count": {"type": "integer"},
                "line_count": {"type": "integer"},
                "is_classical": {"type": "boolean"},
                "classical_score": {"type": "integer"},
                "truncated": {"type": "boolean"},
                "original_char_count": {"type": "integer"},
                "file_size": {"type": "long"},
                "indexed_at": {"type": "date"}
            }
        }
    }
    
    try:
        if es.indices.exists(index=index_name):
            print(f"📝 索引 {index_name} 已存在")
        else:
            es.indices.create(index=index_name, body=mapping)
            print(f"✅ 创建索引 {index_name}")
    except Exception as e:
        print(f"❌ 创建索引失败: {e}")
        return False
    return True

def bulk_index_documents(es, documents, index_name):
    """批量索引文档"""
    if not documents:
        return 0
        
    try:
        actions = []
        for doc in documents:
            if doc and '_id' in doc and '_source' in doc:
                action = {
                    '_index': index_name,
                    '_id': doc['_id'],
                    '_source': doc['_source']
                }
                actions.append(action)
        
        if actions:
            success, failed = bulk(es, actions, request_timeout=180, max_retries=3, chunk_size=100)
            if failed:
                print(f"⚠️  批量索引部分失败: 成功{success}, 失败{len(failed)}")
                # 打印前几个失败的详情
                for i, fail in enumerate(failed[:3]):
                    print(f"    失败{i+1}: {fail}")
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
        print(f"  总文档数: {doc_count:,}")
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
            print(f"  {collection} ({en_name}): {count:,} 个文档")
        
        # 古文统计
        classical_stats = search_result['aggregations']['by_classical']['buckets']
        classical_true = next((b['doc_count'] for b in classical_stats if b['key']), 0)
        print(f"\n📜 古典文献特征:")
        print(f"  古文文档: {classical_true:,} 个")
        print(f"  现代文档: {doc_count - classical_true:,} 个")
        
        # 测试搜索
        print(f"\n🔍 搜索功能测试:")
        test_queries = ["史", "道", "医", "诗", "经"]
        for query in test_queries:
            result = es.search(
                index=index_name,
                body={
                    "query": {"match": {"content": query}},
                    "size": 0
                }
            )
            count = result['hits']['total']['value']
            print(f"  '{query}': {count:,} 个相关文档")
        
    except Exception as e:
        print(f"❌ 统计信息获取失败: {e}")

def main():
    parser = argparse.ArgumentParser(description='导入中国古典文献到Elasticsearch')
    parser.add_argument('--dir', default='/home/ubuntu/daizhigev20', help='数据目录路径')
    parser.add_argument('--index', default='chinese-classics', help='Elasticsearch索引名')
    parser.add_argument('--batch-size', type=int, default=30, help='批量处理大小')
    parser.add_argument('--dry-run', action='store_true', help='仅扫描不导入')
    parser.add_argument('--skip-existing', action='store_true', help='跳过已存在的文档')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🚀 中国古典文献导入工具 - 增强版")
    print("=" * 60)
    
    # 连接Elasticsearch
    es = connect_to_elasticsearch()
    if not es:
        return
    
    data_dir = args.dir
    print(f"🔍 扫描目录: {data_dir}")
    
    if not os.path.exists(data_dir):
        print(f"❌ 数据目录不存在: {data_dir}")
        return
    
    # 收集所有txt文件
    print("📁 收集文件列表...")
    all_files = []
    skipped_files = []
    
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.lower().endswith('.txt'):
                filepath = os.path.join(root, file)
                try:
                    if os.path.getsize(filepath) > 0:  # 跳过空文件
                        all_files.append(filepath)
                    else:
                        skipped_files.append(filepath)
                except OSError:
                    skipped_files.append(filepath)
    
    print(f"📊 找到 {len(all_files):,} 个有效文本文件")
    if skipped_files:
        print(f"⚠️  跳过 {len(skipped_files)} 个空文件或无法访问的文件")
    
    # 按藏分组显示统计
    collection_stats = {}
    total_size = 0
    for filepath in all_files:
        collection_found = False
        for part in filepath.split(os.sep):
            if part in COLLECTION_MAPPING:
                collection = part
                collection_stats[collection] = collection_stats.get(collection, 0) + 1
                collection_found = True
                break
        if not collection_found:
            collection_stats['未分类'] = collection_stats.get('未分类', 0) + 1
        
        try:
            total_size += os.path.getsize(filepath)
        except:
            pass
    
    print(f"\n📚 各藏文件统计 (总大小: {total_size/1024/1024:.2f} MB):")
    for collection, count in sorted(collection_stats.items()):
        if collection in COLLECTION_MAPPING:
            en_name = COLLECTION_MAPPING[collection]
            print(f"  {collection} ({en_name}): {count:,} 个文件")
        else:
            print(f"  {collection}: {count:,} 个文件")
    
    if args.dry_run:
        print("\n🔍 干运行完成，未进行实际导入")
        return
    
    # 创建索引
    print(f"\n🏗️  准备索引: {args.index}")
    if not create_chinese_classics_index(es, args.index):
        return
    
    # 检查已存在的文档（如果启用跳过选项）
    existing_ids = set()
    if args.skip_existing:
        print("🔍 检查已存在的文档...")
        try:
            result = es.search(
                index=args.index,
                body={"query": {"match_all": {}}, "_source": False},
                size=10000,
                scroll='5m'
            )
            
            while result['hits']['hits']:
                for hit in result['hits']['hits']:
                    existing_ids.add(hit['_id'])
                
                if result.get('_scroll_id'):
                    result = es.scroll(scroll_id=result['_scroll_id'], scroll='5m')
                else:
                    break
                    
            print(f"📝 找到 {len(existing_ids)} 个已存在的文档")
        except Exception as e:
            print(f"⚠️  检查已存在文档失败: {e}")
    
    # 批量处理文件
    total_indexed = 0
    total_skipped = 0
    total_failed = 0
    batch_docs = []
    
    print(f"\n🚀 开始导入数据...")
    print(f"批量大小: {args.batch_size}")
    
    for i, filepath in enumerate(all_files, 1):
        filename = os.path.basename(filepath)
        
        # 显示进度
        if i % 100 == 1 or i <= 10:
            print(f"📝 [{i:5d}/{len(all_files)}] 处理: {filename}")
        
        # 生成文档ID检查是否跳过
        relative_path = os.path.relpath(filepath, data_dir)
        doc_id = hashlib.md5(relative_path.encode('utf-8')).hexdigest()
        
        if args.skip_existing and doc_id in existing_ids:
            total_skipped += 1
            continue
        
        # 处理文件
        doc = process_text_file(filepath, data_dir)
        if doc:
            batch_docs.append(doc)
        else:
            total_failed += 1
        
        # 达到批量大小或是最后一批
        if len(batch_docs) >= args.batch_size or i == len(all_files):
            if batch_docs:
                indexed = bulk_index_documents(es, batch_docs, args.index)
                total_indexed += indexed
                batch_docs = []
            
            # 显示进度
            if i % 500 == 0 or i == len(all_files):
                print(f"📈 进度: {i:,}/{len(all_files):,} ({i/len(all_files)*100:.1f}%)")
                print(f"   已索引: {total_indexed:,}, 跳过: {total_skipped:,}, 失败: {total_failed:,}")
    
    print(f"\n🎉 导入完成!")
    print(f"📊 总计: 索引{total_indexed:,}, 跳过{total_skipped:,}, 失败{total_failed:,}")
    
    # 显示详细统计
    if total_indexed > 0:
        show_import_stats(es, args.index, data_dir)
    
    print("\n✨ 导入任务完成！")

if __name__ == "__main__":
    main()
