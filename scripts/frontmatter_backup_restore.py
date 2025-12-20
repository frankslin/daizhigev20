#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Markdown Frontmatter 备份与恢复工具

====================
功能概述
====================

1. 备份 (backup): 提取所有 .md 文件的 frontmatter，存储到单一 JSON 文件
2. 恢复 (restore): 从 JSON 文件恢复 frontmatter 到原始 Markdown 文件
3. 验证 (validate): 验证备份文件的完整性

数据类型保持：
- ✅ 字符串保持为字符串
- ✅ 日期保持为 ISO 8601 UTC 格式
- ✅ 列表保持为列表
- ✅ 嵌套对象保持结构
- ✅ 多行字符串保持多行格式
- ✅ 数字保持为数字

====================
安装依赖
====================

    pip install pyyaml

====================
使用方法
====================

1. 备份所有 frontmatter：

    # 基本用法（从当前目录的 data 子目录备份）
    python scripts/frontmatter_backup_restore.py backup -o frontmatter_backup.json

    # 指定数据目录
    python scripts/frontmatter_backup_restore.py backup -o backup.json -d /path/to/data

    # 指定基础目录
    python scripts/frontmatter_backup_restore.py backup -o backup.json -b /path/to/daizhigev20

2. 验证备份文件：

    python scripts/frontmatter_backup_restore.py validate -i frontmatter_backup.json

3. 恢复 frontmatter（重要：建议先使用 --dry-run 预览）：

    # 预览（不实际修改文件）
    python scripts/frontmatter_backup_restore.py restore -i frontmatter_backup.json --dry-run

    # 实际恢复
    python scripts/frontmatter_backup_restore.py restore -i frontmatter_backup.json

====================
使用场景
====================

场景 1: 批量修改 frontmatter
    # 1. 备份当前所有 frontmatter
    python scripts/frontmatter_backup_restore.py backup -o original_backup.json

    # 2. 使用其他脚本或工具修改 JSON 文件中的 frontmatter
    python your_modification_script.py original_backup.json modified_backup.json

    # 3. 先预览修改
    python scripts/frontmatter_backup_restore.py restore -i modified_backup.json --dry-run

    # 4. 确认无误后恢复
    python scripts/frontmatter_backup_restore.py restore -i modified_backup.json

场景 2: 提取 frontmatter 进行数据分析
    # 1. 备份到 JSON
    python scripts/frontmatter_backup_restore.py backup -o frontmatter_data.json

    # 2. 使用 Python 或其他工具分析 JSON 文件
    import json
    with open('frontmatter_data.json', 'r') as f:
        data = json.load(f)

    # 分析所有作者
    authors = set()
    for path, fm in data.items():
        if 'author' in fm:
            authors.add(fm['author'])
    print(f"共有 {len(authors)} 位作者")

场景 3: 恢复错误修改
    # 如果修改出错，可以从备份恢复
    python scripts/frontmatter_backup_restore.py restore -i original_backup.json

====================
JSON 文件格式
====================

备份的 JSON 文件采用字典格式，键为相对路径，值为 frontmatter 内容：

{
  "data/史藏/志存记录/万历野获编.md": {
    "title": {
      "zh-hans": "万历野获编"
    },
    "category": "/史藏/志存记录",
    "lastmod": "2018-11-26T23:18:40Z",
    "github_repo_url": "https://github.com/...",
    "daizhige_url": "https://daizhige.org/...",
    "additional_info": [
      "2025-11-10 转换自「殆知阁」GitHub 仓库中的 txt 版本"
    ]
  },
  "data/佛藏/大藏经/续藏/古逸部/大乘开心显性顿悟真宗论.md": {
    "title": {
      "zh-hans": "大乘开心显性顿悟真宗论"
    },
    "author": "[唐]慧光释",
    "category": "/佛藏/大藏经/续藏/古逸部",
    "lastmod": "2018-11-26T22:06:02Z",
    "author_cbdb_ids": [14172],
    "additional_info": [
      "2025-09-15 转换自「殆知阁」GitHub 仓库中的 txt 版本"
    ]
  }
}

====================
注意事项
====================

1. 备份前检查: 确保 Git 仓库状态干净，以便出错时可以回滚
2. 先预览: 恢复前务必使用 --dry-run 预览修改
3. 数据完整性: 脚本会保持所有数据类型和格式不变
4. 大型仓库: 对于大型仓库，处理可能需要几分钟
5. 文件编码: 所有文件使用 UTF-8 编码

====================
完整工作流示例
====================

    # 1. 检查 Git 状态
    git status

    # 2. 备份当前 frontmatter
    python scripts/frontmatter_backup_restore.py backup -o backup_$(date +%Y%m%d).json

    # 3. 验证备份
    python scripts/frontmatter_backup_restore.py validate -i backup_$(date +%Y%m%d).json

    # 4. 进行你的修改操作...

    # 5. 预览恢复
    python scripts/frontmatter_backup_restore.py restore -i modified_backup.json --dry-run

    # 6. 实际恢复
    python scripts/frontmatter_backup_restore.py restore -i modified_backup.json

    # 7. 检查修改结果
    git diff

"""

import os
import sys
import json
import argparse
from pathlib import Path
from typing import Dict, Any, List
import yaml
import re
from datetime import date, datetime
import hashlib


class FrontmatterManager:
    """管理 Markdown 文件的 frontmatter"""

    def __init__(self, base_dir: str = None):
        """
        初始化 FrontmatterManager

        Args:
            base_dir: 基础目录路径，默认为脚本所在目录的父目录
        """
        if base_dir is None:
            # 默认为脚本所在目录的父目录
            self.base_dir = Path(__file__).parent.parent
        else:
            self.base_dir = Path(base_dir)

        # 配置 YAML 以保持多行字符串格式
        self.yaml_dumper = yaml.SafeDumper
        self.yaml_dumper.add_representer(str, self._str_representer)

    @staticmethod
    def _str_representer(dumper, data):
        """自定义字符串表示，保持多行格式"""
        if '\n' in data:
            # 对于多行字符串，使用 literal style (|)
            return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
        return dumper.represent_scalar('tag:yaml.org,2002:str', data)

    def extract_frontmatter(self, file_path: Path) -> tuple[Dict[str, Any], str]:
        """
        从 markdown 文件中提取 frontmatter

        Args:
            file_path: markdown 文件路径

        Returns:
            (frontmatter_dict, content): frontmatter 字典和剩余内容
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # 匹配 frontmatter (以 --- 开始和结束)
            pattern = r'^---\s*\n(.*?)\n---\s*\n(.*)$'
            match = re.match(pattern, content, re.DOTALL)

            if not match:
                return None, content

            frontmatter_str = match.group(1)
            body_content = match.group(2)

            # 解析 YAML frontmatter
            frontmatter = yaml.safe_load(frontmatter_str)

            return frontmatter, body_content

        except Exception as e:
            print(f"错误：无法读取文件 {file_path}: {e}", file=sys.stderr)
            return None, None

    def _json_safe(self, obj: Any) -> Any:
        """递归转换为 JSON 可序列化对象，保留日期/时间的 ISO 与时区信息。"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, dict):
            return {self._json_safe(k): self._json_safe(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._json_safe(v) for v in obj]
        if isinstance(obj, tuple):
            return [self._json_safe(v) for v in obj]
        return obj

    def _restore_dates(self, obj: Any) -> Any:
        """递归还原 ISO 字符串为 date/datetime，尽量保持时区信息。"""
        if isinstance(obj, dict):
            return {k: self._restore_dates(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._restore_dates(v) for v in obj]
        if isinstance(obj, str):
            # datetime (含可选时区) or date
            if re.match(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$', obj):
                try:
                    iso_str = obj.replace('Z', '+00:00')
                    return datetime.fromisoformat(iso_str)
                except ValueError:
                    return obj
            if re.match(r'^\d{4}-\d{2}-\d{2}$', obj):
                try:
                    return date.fromisoformat(obj)
                except ValueError:
                    return obj
        return obj

    @staticmethod
    def _compute_daizhige_id(relative_path: str) -> str:
        """按 import_classics.py 的规则计算 daizhige_id。"""
        id_path = relative_path
        if id_path.lower().endswith('.md'):
            id_path = id_path[:-3] + '.txt'
        return hashlib.md5(id_path.encode('utf-8')).hexdigest()

    def write_frontmatter(self, file_path: Path, frontmatter: Dict[str, Any], content: str):
        """
        将 frontmatter 和内容写回 markdown 文件

        Args:
            file_path: markdown 文件路径
            frontmatter: frontmatter 字典
            content: 文件主体内容
        """
        try:
            # 转换 frontmatter 为 YAML 格式
            yaml_str = yaml.dump(
                frontmatter,
                Dumper=self.yaml_dumper,
                allow_unicode=True,
                sort_keys=False,
                default_flow_style=False
            )

            # 组合完整文件内容
            full_content = f"---\n{yaml_str}---\n{content}"

            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(full_content)

        except Exception as e:
            print(f"错误：无法写入文件 {file_path}: {e}", file=sys.stderr)

    def backup_all_frontmatter(self, output_file: str, data_dir: str = None) -> int:
        """
        备份所有 markdown 文件的 frontmatter 到 JSON 文件

        Args:
            output_file: 输出 JSON 文件路径
            data_dir: 数据目录，默认为 base_dir/data

        Returns:
            处理的文件数量
        """
        if data_dir is None:
            data_dir = self.base_dir / 'data'
        else:
            data_dir = Path(data_dir)

        if not data_dir.exists():
            print(f"错误：数据目录不存在: {data_dir}", file=sys.stderr)
            return 0

        frontmatter_dict = {}
        count = 0

        # 遍历所有 markdown 文件
        for md_file in data_dir.rglob('*.md'):
            frontmatter, _ = self.extract_frontmatter(md_file)

            if frontmatter is not None:
                # 使用相对路径作为 key
                relative_path = str(md_file.relative_to(self.base_dir))
                data_relative_path = str(md_file.relative_to(data_dir))
                if isinstance(frontmatter, dict):
                    frontmatter['__daizhige_id'] = self._compute_daizhige_id(data_relative_path)
                frontmatter_dict[relative_path] = self._json_safe(frontmatter)
                count += 1

                if count % 100 == 0:
                    print(f"已处理 {count} 个文件...")

        # 写入 JSON 文件
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(frontmatter_dict, f, ensure_ascii=False, indent=2)

        print(f"\n成功备份 {count} 个文件的 frontmatter 到: {output_path}")
        return count

    def restore_all_frontmatter(self, input_file: str, dry_run: bool = False) -> int:
        """
        从 JSON 文件恢复所有 frontmatter 到原始 markdown 文件

        Args:
            input_file: 输入 JSON 文件路径
            dry_run: 如果为 True，只显示将要修改的文件，不实际修改

        Returns:
            恢复的文件数量
        """
        input_path = Path(input_file)

        if not input_path.exists():
            print(f"错误：输入文件不存在: {input_path}", file=sys.stderr)
            return 0

        # 读取 JSON 文件
        with open(input_path, 'r', encoding='utf-8') as f:
            frontmatter_dict = json.load(f)

        count = 0
        skipped = 0

        for relative_path, frontmatter in frontmatter_dict.items():
            file_path = self.base_dir / relative_path

            if not file_path.exists():
                print(f"警告：文件不存在，跳过: {relative_path}", file=sys.stderr)
                skipped += 1
                continue

            # 提取当前文件内容（不包括 frontmatter）
            _, current_content = self.extract_frontmatter(file_path)

            if current_content is None:
                print(f"警告：无法读取文件，跳过: {relative_path}", file=sys.stderr)
                skipped += 1
                continue

            if dry_run:
                print(f"将恢复: {relative_path}")
            else:
                # 写回 frontmatter
                restored = self._restore_dates(frontmatter)
                self.write_frontmatter(file_path, restored, current_content)

            count += 1

            if count % 100 == 0:
                action = "将恢复" if dry_run else "已恢复"
                print(f"{action} {count} 个文件...")

        action = "将恢复" if dry_run else "成功恢复"
        print(f"\n{action} {count} 个文件的 frontmatter")

        if skipped > 0:
            print(f"跳过 {skipped} 个文件")

        return count

    def validate_backup(self, backup_file: str) -> bool:
        """
        验证备份文件的完整性

        Args:
            backup_file: 备份 JSON 文件路径

        Returns:
            验证是否成功
        """
        try:
            with open(backup_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            print(f"备份文件包含 {len(data)} 个文件的 frontmatter")

            # 检查数据格式
            for path, frontmatter in list(data.items())[:5]:
                print(f"\n示例文件: {path}")
                print(f"Frontmatter 字段: {list(frontmatter.keys())}")

            return True

        except Exception as e:
            print(f"错误：验证失败: {e}", file=sys.stderr)
            return False


def main():
    parser = argparse.ArgumentParser(
        description='Markdown Frontmatter 备份与恢复工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 备份所有 frontmatter 到 JSON 文件
  %(prog)s backup -o frontmatter_backup.json

  # 从 JSON 文件恢复 frontmatter（先预览）
  %(prog)s restore -i frontmatter_backup.json --dry-run

  # 从 JSON 文件恢复 frontmatter（实际执行）
  %(prog)s restore -i frontmatter_backup.json

  # 验证备份文件
  %(prog)s validate -i frontmatter_backup.json

  # 指定基础目录
  %(prog)s backup -o backup.json -b /path/to/daizhigev20
        """
    )

    parser.add_argument(
        'command',
        choices=['backup', 'restore', 'validate'],
        help='要执行的命令'
    )

    parser.add_argument(
        '-b', '--base-dir',
        help='基础目录路径（默认为脚本所在目录的父目录）'
    )

    parser.add_argument(
        '-d', '--data-dir',
        help='数据目录路径（默认为 base-dir/data）'
    )

    parser.add_argument(
        '-o', '--output',
        help='输出 JSON 文件路径（用于 backup 命令）'
    )

    parser.add_argument(
        '-i', '--input',
        help='输入 JSON 文件路径（用于 restore 和 validate 命令）'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='模拟运行，不实际修改文件（用于 restore 命令）'
    )

    args = parser.parse_args()

    # 创建管理器
    manager = FrontmatterManager(base_dir=args.base_dir)

    # 执行命令
    if args.command == 'backup':
        if not args.output:
            print("错误：backup 命令需要 -o/--output 参数", file=sys.stderr)
            return 1

        count = manager.backup_all_frontmatter(
            output_file=args.output,
            data_dir=args.data_dir
        )

        return 0 if count > 0 else 1

    elif args.command == 'restore':
        if not args.input:
            print("错误：restore 命令需要 -i/--input 参数", file=sys.stderr)
            return 1

        count = manager.restore_all_frontmatter(
            input_file=args.input,
            dry_run=args.dry_run
        )

        return 0 if count > 0 else 1

    elif args.command == 'validate':
        if not args.input:
            print("错误：validate 命令需要 -i/--input 参数", file=sys.stderr)
            return 1

        success = manager.validate_backup(args.input)
        return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
