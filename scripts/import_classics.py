#!/usr/bin/env python3
"""
中国古典文献数字化资料导入Elasticsearch
专门处理daizhigev20目录下的传统典籍数据
"""

import os
import hashlib
import yaml
import re
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

def parse_markdown_file(content, filepath):
    """解析Markdown文件，提取YAML front matter和正文"""
    result = {
        'yaml_metadata': {},
        'content': content,
        'has_yaml': False
    }

    if not content.strip():
        return result

    # 检查是否有YAML front matter
    if content.startswith('---'):
        parts = content.split('---', 2)
        if len(parts) >= 3:
            try:
                # 解析YAML
                yaml_content = parts[1].strip()
                yaml_data = yaml.safe_load(yaml_content)

                result['yaml_metadata'] = yaml_data or {}
                result['content'] = parts[2].strip()  # 去掉YAML后的正文
                result['has_yaml'] = True

                print(f"  📄 解析YAML成功: {len(result['yaml_metadata'])} 个字段")

            except yaml.YAMLError as e:
                print(f"  ⚠️  YAML解析失败: {e}")
                # 保持原始内容

    return result

def extract_markdown_headings(content):
    """提取Markdown文档中的各级标题"""
    headings = []
    heading_pattern = r'^(#{1,6})\s+(.+)$'

    for line_num, line in enumerate(content.split('\n'), 1):
        match = re.match(heading_pattern, line.strip())
        if match:
            level = len(match.group(1))
            title = match.group(2).strip()
            headings.append({
                'level': level,
                'title': title,
                'line_number': line_num
            })

    return headings

def extract_markdown_metadata(yaml_data, content, filepath):
    """从Markdown文件的YAML元数据中提取信息"""
    metadata = {}

    try:
        # 安全获取文件基本信息
        filename = os.path.basename(filepath)
        path_parts = filepath.split(os.sep)

        # 提取Markdown标题结构
        headings = extract_markdown_headings(content)
        metadata['headings'] = headings

        # 从YAML中提取信息
        if yaml_data:
            # 标题处理
            title_data = yaml_data.get('title', {})
            if isinstance(title_data, dict):
                metadata['title'] = title_data.get('zh-hans') or title_data.get('zh-hant') or filename
            else:
                metadata['title'] = str(title_data) if title_data else filename

            # 作者信息处理
            author_data = yaml_data.get('author', {})
            if isinstance(author_data, dict):
                # 处理多语言作者名
                metadata['author'] = author_data.get('zh-hant') or author_data.get('zh-hans') or author_data.get('en', '')
                metadata['author_info'] = author_data
            elif isinstance(author_data, str):
                metadata['author'] = author_data
                metadata['author_info'] = {'name': author_data}
            elif isinstance(author_data, list):
                # 处理多个作者
                authors = []
                for auth in author_data:
                    if isinstance(auth, dict):
                        authors.append(auth.get('zh-hant') or auth.get('zh-hans') or auth.get('en', str(auth)))
                    else:
                        authors.append(str(auth))
                metadata['author'] = ', '.join(authors)
                metadata['author_info'] = author_data
            else:
                metadata['author'] = ''
                metadata['author_info'] = {}

            # 朝代信息
            dynasty = yaml_data.get('dynasty')
            if dynasty:
                metadata['dynasty'] = dynasty

            # 分类信息
            category = yaml_data.get('category', '')
            if category:
                metadata['book_category'] = category

            # 源语言
            metadata['source_language'] = yaml_data.get('source_language', 'zh-hant')

            # 最后修改时间
            lastmod = yaml_data.get('lastmod')
            if lastmod:
                metadata['lastmod'] = lastmod

            # 源URL
            source_urls = yaml_data.get('source_url', [])
            if source_urls:
                metadata['source_urls'] = source_urls if isinstance(source_urls, list) else [source_urls]

            # 规范ID
            canonical_id = yaml_data.get('canonical_id')
            if canonical_id:
                metadata['canonical_id'] = canonical_id

            # 版权和许可证信息
            metadata['copyright'] = yaml_data.get('copyright', '')
            metadata['license'] = yaml_data.get('license', '')

            # 附加信息
            additional_info = yaml_data.get('additional_info', [])
            if additional_info:
                metadata['additional_info'] = additional_info

        else:
            # 没有YAML，尝试从Markdown标题提取信息
            if headings:
                # 使用第一个一级标题作为标题
                h1_headings = [h for h in headings if h['level'] == 1]
                if h1_headings:
                    metadata['title'] = h1_headings[0]['title']
                else:
                    metadata['title'] = headings[0]['title']
            else:
                metadata['title'] = os.path.splitext(filename)[0]

            # 默认空作者信息
            metadata['author'] = ''
            metadata['author_info'] = {}

        # 从路径中提取藏和分类信息
        # 为了向后兼容，将 .md 文件名改为 .txt 记录
        if filename.lower().endswith('.md'):
            metadata['filename'] = filename[:-3] + '.txt'
        else:
            metadata['filename'] = filename
        metadata['collection'] = '未分类'
        metadata['collection_en'] = 'Uncategorized'

        for i, part in enumerate(path_parts):
            if part in COLLECTION_MAPPING:
                metadata['collection'] = part
                metadata['collection_en'] = COLLECTION_MAPPING[part]

                # 如果YAML中没有分类信息，从路径提取
                if 'book_category' not in metadata and i + 1 < len(path_parts) - 1:
                    metadata['book_category'] = path_parts[i + 1]
                break

        # 如果没有分类，设置默认值
        if 'book_category' not in metadata:
            metadata['book_category'] = '其他'

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
        print(f"⚠️  Markdown元数据提取警告 {filepath}: {e}")
        # 返回基本元数据
        filename = os.path.basename(filepath)
        return {
            'filename': filename[:-3] + '.txt' if filename.lower().endswith('.md') else filename,
            'title': os.path.splitext(os.path.basename(filepath))[0],
            'collection': '未分类',
            'collection_en': 'Uncategorized',
            'book_category': '其他',
            'char_count': len(content) if content else 0,
            'line_count': len(content.split('\n')) if content else 0,
            'classical_score': 0,
            'is_classical': False
        }

def extract_text_metadata(content, filepath):
    """从文本内容中提取元数据 - 修复版"""
    metadata = {}
    
    try:
        # 安全获取文件基本信息
        filename = os.path.basename(filepath)
        path_parts = filepath.split(os.sep)
        
        # 初始化默认值
        # 为了向后兼容，将 .md 文件名改为 .txt 记录（虽然这里主要是 .txt 文件）
        if filename.lower().endswith('.md'):
            metadata['filename'] = filename[:-3] + '.txt'
        else:
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
        
        # 提取Markdown标题结构
        headings = extract_markdown_headings(content)
        metadata['headings'] = headings

        # 如果没有找到标题，使用文件名（去掉.txt）
        if 'title' not in metadata:
            if headings:
                # 使用第一个一级标题作为标题
                h1_headings = [h for h in headings if h['level'] == 1]
                if h1_headings:
                    metadata['title'] = h1_headings[0]['title']
                else:
                    metadata['title'] = headings[0]['title']
            else:
                title = os.path.splitext(filename)[0]
                metadata['title'] = title

        # 如果没有找到章节，设为空
        if 'chapter' not in metadata:
            metadata['chapter'] = ''

        # 设置默认作者信息
        if 'author' not in metadata:
            metadata['author'] = ''
            metadata['author_info'] = {}
        
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
        filename = os.path.basename(filepath)
        return {
            'filename': filename[:-3] + '.txt' if filename.lower().endswith('.md') else filename,
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

def process_markdown_file(filepath, base_dir):
    """处理单个Markdown文件 - 包含YAML元数据解析"""
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
        raw_content = safe_read_file(filepath)
        if not raw_content:
            print(f"❌ 无法读取文件内容: {filepath}")
            return None

        # 解析Markdown和YAML
        parsed = parse_markdown_file(raw_content, filepath)
        content = parsed['content']
        yaml_metadata = parsed['yaml_metadata']

        # 清理和验证内容
        content = content.strip()
        if len(content) < 10:  # 内容太少，可能不是有效文档
            print(f"⚠️  跳过内容过少的文件: {filepath}")
            return None

        # 从YAML和内容提取元数据
        metadata = extract_markdown_metadata(yaml_metadata, content, relative_path)

        # 处理过长的内容
        original_length = len(content)
        if original_length > 1024000:  # 1024KB以上截取
            # 保留前400K字符和后200K字符
            content = content[:409600] + f"\n\n...[文档过长({original_length}字符)，已截取中间部分]...\n\n" + content[-204800:]
            metadata['truncated'] = True
            metadata['original_char_count'] = original_length
            print(f"⚠️  已截取内容过长的文件: {filepath}")
        else:
            metadata['truncated'] = False
            metadata['original_char_count'] = original_length

        # 生成文档ID（使用统一的文件名确保一致性）
        # 将.md文件的路径转换为.txt以保证同名文件有相同ID
        id_path = relative_path
        if relative_path.lower().endswith('.md'):
            id_path = relative_path[:-3] + '.txt'
        doc_id = hashlib.md5(id_path.encode('utf-8')).hexdigest()

        # 构建文档
        doc = {
            '_id': doc_id,
            '_source': {
                'content': content,
                'filepath': id_path,  # 使用转换后的路径，保持一致性
                'file_size': file_size,
                'file_type': 'markdown',
                'indexed_at': datetime.now().isoformat(),
                'has_yaml_metadata': parsed['has_yaml'],
                'yaml_metadata': yaml_metadata,
                **metadata
            }
        }

        return doc

    except Exception as e:
        print(f"❌ 处理Markdown文件失败 {filepath}: {e}")
        return None

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
        if original_length > 1024000:  # 1024KB以上截取
            # 保留前400K字符和后200K字符
            content = content[:409600] + f"\n\n...[文档过长({original_length}字符)，已截取中间部分]...\n\n" + content[-204800:]
            metadata['truncated'] = True
            metadata['original_char_count'] = original_length
            print(f"⚠️  已截取内容过长的文件: {filepath}")
        else:
            metadata['truncated'] = False
            metadata['original_char_count'] = original_length
        
        # 生成文档ID（使用相对路径确保一致性）
        # 确保 .md 文件和 .txt 文件使用相同的ID计算方式
        id_path = relative_path
        if relative_path.lower().endswith('.md'):
            id_path = relative_path[:-3] + '.txt'
        doc_id = hashlib.md5(id_path.encode('utf-8')).hexdigest()
        
        # 构建Elasticsearch文档
        doc = {
            '_id': doc_id,
            '_source': {
                'title': metadata['title'],
                'author': metadata.get('author', ''),
                'author_info': metadata.get('author_info', {}),
                'dynasty': metadata.get('dynasty', ''),
                'headings': metadata.get('headings', []),
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
                'file_type': 'text',
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
                "author": {
                    "type": "text",
                    "analyzer": "ik_chinese_analyzer",
                    "search_analyzer": "ik_chinese_search_analyzer",
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                "author_info": {
                    "type": "object",
                    "enabled": True
                },
                "dynasty": {
                    "type": "keyword"
                },
                "headings": {
                    "type": "nested",
                    "properties": {
                        "level": {"type": "integer"},
                        "title": {
                            "type": "text",
                            "analyzer": "ik_chinese_analyzer",
                            "search_analyzer": "ik_chinese_search_analyzer"
                        },
                        "line_number": {"type": "integer"}
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
                "indexed_at": {"type": "date"},
                "source_language": {"type": "keyword"},
                "lastmod": {"type": "date"},
                "source_urls": {"type": "keyword"},
                "canonical_id": {"type": "keyword"},
                "copyright": {"type": "text"},
                "license": {"type": "keyword"},
                "additional_info": {"type": "text"},
                "file_type": {"type": "keyword"},
                "has_yaml_metadata": {"type": "boolean"},
                "yaml_metadata": {
                    "type": "object",
                    "enabled": True
                }
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
            if file.lower().endswith(('.txt', '.md')):
                filepath = os.path.join(root, file)
                try:
                    if os.path.getsize(filepath) > 0:  # 跳过空文件
                        all_files.append(filepath)
                    else:
                        skipped_files.append(filepath)
                except OSError:
                    skipped_files.append(filepath)
    
    print(f"📊 找到 {len(all_files):,} 个有效文件（.txt 和 .md）")
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
        # 确保 .md 文件和 .txt 文件使用相同的ID计算方式
        id_path = relative_path
        if relative_path.lower().endswith('.md'):
            id_path = relative_path[:-3] + '.txt'
        doc_id = hashlib.md5(id_path.encode('utf-8')).hexdigest()
        
        if args.skip_existing and doc_id in existing_ids:
            total_skipped += 1
            continue
        
        # 根据文件类型处理文件
        if filepath.lower().endswith('.md'):
            doc = process_markdown_file(filepath, data_dir)
        else:
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
