#!/usr/bin/env python3
"""
修复版：只重新索引修改过的文件
解决超时问题，增加更好的错误处理
"""

import os
import sys
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionTimeout
import importlib.util

# 导入原来的处理函数
spec = importlib.util.spec_from_file_location("import_classics", "scripts/import_classics.py")
import_classics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(import_classics)

def get_indexed_files_info(es, index_name):
    """获取所有已索引文件的信息 - 修复版"""
    indexed_files = {}
    
    try:
        print("正在获取已索引文件信息...")
        
        # 使用scroll API处理大量数据，增加超时时间
        result = es.search(
            index=index_name,
            body={
                "query": {"match_all": {}},
                "_source": ["filepath", "indexed_at", "file_size"],
                "size": 1000  # 减小批次大小
            },
            scroll='10m',
            timeout='60s',  # 增加超时时间
            request_timeout=120  # 增加请求超时
        )
        
        batch_count = 0
        total_docs = 0
        
        while result['hits']['hits']:
            batch_count += 1
            current_batch = len(result['hits']['hits'])
            total_docs += current_batch
            
            print(f"  处理批次 {batch_count}, 当前批次 {current_batch} 文档, 累计 {total_docs} 文档")
            
            for hit in result['hits']['hits']:
                source = hit['_source']
                indexed_files[source['filepath']] = {
                    'indexed_at': source.get('indexed_at'),
                    'file_size': source.get('file_size', 0),
                    'doc_id': hit['_id']
                }
            
            # 继续滚动，增加超时时间
            try:
                if result.get('_scroll_id'):
                    result = es.scroll(
                        scroll_id=result['_scroll_id'], 
                        scroll='10m',
                        request_timeout=120
                    )
                else:
                    break
                    
                # 如果没有更多结果，退出
                if not result['hits']['hits']:
                    break
                    
            except ConnectionTimeout:
                print("  滚动查询超时，尝试继续...")
                break
            except Exception as e:
                print(f"  滚动查询出错: {e}")
                break
                
        print(f"✅ 成功获取 {len(indexed_files)} 个文档信息")
                
    except ConnectionTimeout:
        print("❌ 连接超时，可能是因为数据量太大")
        print("💡 建议：")
        print("   1. 直接运行完整重新索引")
        print("   2. 或者分批处理特定文件")
    except Exception as e:
        print(f"❌ 获取已索引文件信息失败: {e}")
    
    return indexed_files

def get_sample_indexed_files(es, index_name, sample_size=1000):
    """获取样本文件进行快速检查"""
    try:
        result = es.search(
            index=index_name,
            body={
                "query": {"match_all": {}},
                "_source": ["filepath", "indexed_at", "file_size"],
                "size": sample_size
            },
            request_timeout=30
        )
        
        indexed_files = {}
        for hit in result['hits']['hits']:
            source = hit['_source']
            indexed_files[source['filepath']] = {
                'indexed_at': source.get('indexed_at'),
                'file_size': source.get('file_size', 0),
                'doc_id': hit['_id']
            }
        
        total_docs = result['hits']['total']['value']
        print(f"📊 索引中总共有 {total_docs} 个文档")
        print(f"📋 获取了 {len(indexed_files)} 个样本用于比较")
        
        return indexed_files, total_docs
        
    except Exception as e:
        print(f"获取样本失败: {e}")
        return {}, 0

def find_modified_files_by_sample(data_dir, sample_indexed_files):
    """通过样本快速估算修改情况"""
    print("🔍 通过样本检查文件修改情况...")
    
    modified_count = 0
    checked_count = 0
    
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.lower().endswith('.txt'):
                filepath = os.path.join(root, file)
                relative_path = os.path.relpath(filepath, data_dir)
                
                if relative_path in sample_indexed_files:
                    checked_count += 1
                    try:
                        file_stat = os.stat(filepath)
                        file_mtime = datetime.fromtimestamp(file_stat.st_mtime)
                        file_size = file_stat.st_size
                        
                        indexed_info = sample_indexed_files[relative_path]
                        indexed_at_str = indexed_info['indexed_at']
                        
                        if indexed_at_str:
                            indexed_at = datetime.fromisoformat(indexed_at_str.replace('Z', '+00:00'))
                            indexed_at = indexed_at.replace(tzinfo=None)
                            
                            if file_mtime > indexed_at or file_size != indexed_info['file_size']:
                                modified_count += 1
                                
                    except OSError:
                        pass
    
    if checked_count > 0:
        estimated_modified = int((modified_count / checked_count) * 15694)
        print(f"📊 样本分析结果:")
        print(f"  检查样本: {checked_count} 个文件")
        print(f"  样本中修改: {modified_count} 个文件")
        print(f"  预估总修改: {estimated_modified} 个文件")
        return estimated_modified
    else:
        print("⚠️  样本中没有找到匹配的文件")
        return 15694  # 假设全部需要更新

def main():
    print("🔍 检查需要更新的文件（优化版）...")
    
    # 连接Elasticsearch
    es = import_classics.connect_to_elasticsearch()
    if not es:
        return
    
    data_dir = '/home/ubuntu/daizhigev20'
    index_name = 'chinese-classics'
    
    # 检查索引是否存在
    try:
        if not es.indices.exists(index=index_name):
            print(f"❌ 索引 {index_name} 不存在")
            return
    except Exception as e:
        print(f"❌ 检查索引失败: {e}")
        return
    
    print(f"\n选择检查模式:")
    print(f"1. 快速检查（推荐）- 使用样本估算")
    print(f"2. 完整检查 - 获取所有文档信息（可能很慢）")
    print(f"3. 跳过检查 - 直接重新索引所有文件")
    
    choice = input("请选择 (1/2/3): ").strip()
    
    if choice == "3":
        print("🚀 直接重新索引所有文件...")
        os.system("nohup python3 import_classics.py --dir /home/ubuntu/daizhigev20 --index chinese-classics --batch-size 30 > reindex_all.log 2>&1 &")
        print("📝 后台任务已启动，查看进度: tail -f reindex_all.log")
        return
    elif choice == "2":
        # 完整检查
        print("⚠️  这可能需要几分钟时间...")
        indexed_files = get_indexed_files_info(es, index_name)
        
        if len(indexed_files) == 0:
            print("❌ 无法获取索引信息，建议选择选项3直接重新索引")
            return
    else:
        # 快速检查（默认）
        sample_indexed_files, total_docs = get_sample_indexed_files(es, index_name, 2000)
        
        if len(sample_indexed_files) == 0:
            print("❌ 无法获取样本，建议选择选项3直接重新索引")
            return
        
        estimated_modified = find_modified_files_by_sample(data_dir, sample_indexed_files)
        
        print(f"\n📊 估算结果:")
        print(f"  预计需要更新约 {estimated_modified} 个文件")
        
        if estimated_modified > 5000:
            print("💡 修改文件较多，建议直接重新索引全部文件")
            response = input("是否直接重新索引全部？(y/N): ")
            if response.lower() == 'y':
                os.system("nohup python3 import_classics.py --dir /home/ubuntu/daizhigev20 --index chinese-classics --batch-size 30 > reindex_all.log 2>&1 &")
                print("📝 后台任务已启动，查看进度: tail -f reindex_all.log")
        else:
            print("💡 修改文件不多，可以考虑精确更新")
        
        return

if __name__ == "__main__":
    main()

