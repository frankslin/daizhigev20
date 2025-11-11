#!/bin/bash

# 批量转换 txt 到 markdown 并删除原文件
# 用法: 在项目根目录（data 和 scripts 的上一层）运行
#       ./scripts/batch_convert_txt_to_md.sh

# 不使用 set -e，因为我们要手动处理每个文件的错误

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 获取当前工作目录（应该是项目根目录）
PROJECT_ROOT="$(pwd)"
# data 目录（相对于当前工作目录）
DATA_DIR="$PROJECT_ROOT/data"

# 转换脚本路径（相对于当前工作目录）
CONVERT_SCRIPT="$PROJECT_ROOT/scripts/convert_txt_to_md.py"

# 检查转换脚本是否存在
if [ ! -f "$CONVERT_SCRIPT" ]; then
    echo -e "${RED}错误：找不到转换脚本 $CONVERT_SCRIPT${NC}"
    exit 1
fi

# 检查 data 目录是否存在
if [ ! -d "$DATA_DIR" ]; then
    echo -e "${RED}错误：找不到 data 目录 $DATA_DIR${NC}"
    exit 1
fi

echo "========================================"
echo "批量 TXT 转 Markdown 脚本"
echo "========================================"
echo "项目根目录: $PROJECT_ROOT"
echo "Data 目录: $DATA_DIR"
echo "转换脚本: $CONVERT_SCRIPT"
echo ""

# 统计变量
total_files=0
success_count=0
skip_count=0
fail_count=0

# 存储失败的文件列表
declare -a failed_files

# 查找所有 txt 文件
echo "正在扫描 txt 文件..."
while IFS= read -r -d '' txt_file; do
    ((total_files++))

    # 获取相对路径（相对于 data 目录，用于显示）
    rel_path="${txt_file#$DATA_DIR/}"

    # 计算对应的 md 文件路径
    md_file="${txt_file%.txt}.md"

    echo ""
    echo "----------------------------------------"
    echo "[$total_files] 处理: $rel_path"

    # 检查 md 文件是否已存在
    if [ -f "$md_file" ]; then
        echo -e "${YELLOW}⊙ 跳过（md 文件已存在）${NC}"
        ((skip_count++))
        continue
    fi

    # 执行转换
    if python3 "$CONVERT_SCRIPT" "$txt_file" "$md_file"; then
        # 转换成功，验证输出文件是否存在
        if [ -f "$md_file" ]; then
            # 删除原始 txt 文件
            if rm "$txt_file"; then
                echo -e "${GREEN}✓ 转换成功并删除原文件${NC}"
                ((success_count++))
            else
                echo -e "${RED}✗ 转换成功但无法删除原文件${NC}"
                failed_files+=("$rel_path (无法删除原文件)")
                ((fail_count++))
            fi
        else
            echo -e "${RED}✗ 转换失败（输出文件不存在）${NC}"
            failed_files+=("$rel_path (输出文件不存在)")
            ((fail_count++))
        fi
    else
        echo -e "${RED}✗ 转换失败（脚本返回错误）${NC}"
        failed_files+=("$rel_path (转换脚本错误)")
        ((fail_count++))
    fi

done < <(find "$DATA_DIR" -type f -name "*.txt" -print0)

# 打印统计信息
echo ""
echo "========================================"
echo "转换完成统计"
echo "========================================"
echo "总文件数: $total_files"
echo -e "${GREEN}成功: $success_count${NC}"
echo -e "${YELLOW}跳过: $skip_count${NC}"
echo -e "${RED}失败: $fail_count${NC}"

# 如果有失败的文件，列出来
if [ $fail_count -gt 0 ]; then
    echo ""
    echo "失败的文件列表："
    for file in "${failed_files[@]}"; do
        echo "  - $file"
    done
    exit 1
fi

echo ""
echo -e "${GREEN}所有文件处理完成！${NC}"
exit 0