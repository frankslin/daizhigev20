#!/usr/bin/env python3
import os
import sys
import importlib.util

# 导入处理函数
spec = importlib.util.spec_from_file_location("import_classics", "scripts/import_classics.py")
import_classics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(import_classics)

# 指定要更新的文件列表（相对路径）
files_to_update = [
    "集藏/小说/白圭志.txt",
    # 在这里添加更多修改过的文件...
]

def main():
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
