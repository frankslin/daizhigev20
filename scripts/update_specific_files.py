#!/usr/bin/env python3
import os
import sys
import importlib.util

# 导入处理函数
spec = importlib.util.spec_from_file_location("import_classics", "scripts/import_classics.py")
import_classics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(import_classics)

def read_files_list(file_list_path):
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

def main():
    # 文件列表的路径，可以通过命令行参数指定，默认为 files_to_update.txt
    file_list_path = sys.argv[1] if len(sys.argv) > 1 else "files_to_update.txt"
    
    # 从文件读取要更新的文件列表
    files_to_update = read_files_list(file_list_path)
    
    if not files_to_update:
        print("❌ 没有找到需要更新的文件")
        return
    
    es = import_classics.connect_to_elasticsearch()
    if not es:
        return
    
    data_dir = '/home/ubuntu/daizhigev20'
    index_name = 'chinese-classics'
    
    print(f"🚀 更新 {len(files_to_update)} 个指定文件...")
    
    successful = 0
    failed = 0
    
    for i, relative_path in enumerate(files_to_update, 1):
        filepath = os.path.join(data_dir, relative_path)
        print(f"[{i}/{len(files_to_update)}] 处理: {relative_path}")
        
        if not os.path.exists(filepath):
            print(f"  ❌ 文件不存在")
            failed += 1
            continue
        
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
