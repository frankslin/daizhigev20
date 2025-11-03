#!/bin/bash

# 中国古典文献大文件分析脚本 (支持到100MB+)
DATA_DIR="./data"

echo "🔍 大文件分析 - 目录: $DATA_DIR"
echo "📁 分析所有嵌套的 *.txt 文件 (支持到100MB+)..."
echo ""

# 详细大小分布分析
echo "📈 详细文件大小分布:"
find "$DATA_DIR" -name "*.txt" -type f -printf "%s %p\n" | sort -n | awk '
BEGIN {
    # 扩展的区间定义
    ranges[1] = 1024; labels[1] = "<1KB"
    ranges[2] = 10240; labels[2] = "1-10KB"  
    ranges[3] = 51200; labels[3] = "10-50KB"
    ranges[4] = 102400; labels[4] = "50-100KB"
    ranges[5] = 512000; labels[5] = "100-500KB"
    ranges[6] = 1048576; labels[6] = "500KB-1MB"
    ranges[7] = 2097152; labels[7] = "1-2MB"
    ranges[8] = 5242880; labels[8] = "2-5MB"
    ranges[9] = 10485760; labels[9] = "5-10MB"
    ranges[10] = 20971520; labels[10] = "10-20MB"
    ranges[11] = 52428800; labels[11] = "20-50MB"
    ranges[12] = 104857600; labels[12] = "50-100MB"
    ranges[13] = 999999999999; labels[13] = ">100MB"
    
    total_size = 0
}
{
    size = $1
    path = $2
    total++
    total_size += size
    
    # 分配到对应区间
    for (i = 1; i <= 13; i++) {
        if (size <= ranges[i]) {
            buckets[i]++
            bucket_sizes[i] += size
            
            # 记录每个区间的示例文件
            if (examples[i] == "" && size > 0) {
                filename = path
                gsub(/.*\//, "", filename)  # 只保留文件名
                examples[i] = filename
            }
            break
        }
    }
    
    # 存储所有大小用于百分位数计算
    sizes[total] = size
}
END {
    # 打印详细分布
    printf "  %-12s %8s %8s %10s %8s %s\n", "区间", "数量", "百分比", "总大小", "平均", "示例文件"
    print "  " repeat("-", 80)
    
    for (i = 1; i <= 13; i++) {
        count = buckets[i] + 0
        size_mb = bucket_sizes[i] / 1024 / 1024
        percent = count * 100 / total
        avg_kb = count > 0 ? bucket_sizes[i] / count / 1024 : 0
        
        bar = ""
        for (j = 1; j <= int(percent/1.5); j++) bar = bar "▓"
        
        printf "  %-12s %8d %7.1f%% %8.2fMB %6.1fKB %-20s %s\n", 
            labels[i], count, percent, size_mb, avg_kb, 
            substr(examples[i], 1, 20), bar
    }
    
    print ""
    printf "📊 总体统计:\n"
    printf "  总文件数: %d\n", total
    printf "  总大小: %.2f MB\n", total_size/1024/1024
    printf "  平均大小: %.2f KB\n", total_size/total/1024
    
    # 计算百分位数
    p50 = sizes[int(total * 0.5)]
    p75 = sizes[int(total * 0.75)]
    p90 = sizes[int(total * 0.9)]
    p95 = sizes[int(total * 0.95)]
    p99 = sizes[int(total * 0.99)]
    
    print ""
    printf "📏 百分位数统计:\n"
    printf "  50%% (中位数): %.1f KB\n", p50/1024
    printf "  75%%: %.1f KB\n", p75/1024
    printf "  90%%: %.1f KB\n", p90/1024
    printf "  95%%: %.1f KB\n", p95/1024
    printf "  99%%: %.1f KB\n", p99/1024
}
function repeat(str, n) { 
    result = ""; 
    for (i = 1; i <= n; i++) result = result str; 
    return result 
}'

echo ""

# 专门分析大文件 (>1MB)
echo "🔍 大文件详细分析 (>1MB):"
find "$DATA_DIR" -name "*.txt" -size +1M -printf "%s %p\n" | sort -nr | awk '
BEGIN { 
    print "  文件大小分布:"
    mb1_5 = 0; mb5_10 = 0; mb10_50 = 0; mb50_100 = 0; mb100_plus = 0
}
{
    size_mb = $1/1024/1024
    filename = $2
    gsub(/.*\/[^\/]*\//, "", filename)  # 保留目录/文件名
    
    if (size_mb <= 5) mb1_5++
    else if (size_mb <= 10) mb5_10++
    else if (size_mb <= 50) mb10_50++
    else if (size_mb <= 100) mb50_100++
    else mb100_plus++
    
    total_large++
    total_large_size += $1
    
    # 显示最大的20个文件
    if (NR <= 20) {
        printf "    %2d. %8.2f MB - %s\n", NR, size_mb, filename
    }
}
END {
    if (total_large > 0) {
        printf "\n  📊 大文件分类统计:\n"
        printf "    1-5MB:     %d 个文件\n", mb1_5
        printf "    5-10MB:    %d 个文件\n", mb5_10
        printf "    10-50MB:   %d 个文件\n", mb10_50
        printf "    50-100MB:  %d 个文件\n", mb50_100
        printf "    >100MB:    %d 个文件\n", mb100_plus
        printf "    总计:      %d 个大文件, %.2f MB\n", total_large, total_large_size/1024/1024
    } else {
        printf "  📝 没有找到大于1MB的文件\n"
    }
}'

echo ""

# 分析会被截取的文件 (>100KB, 即当前脚本的截取阈值)
echo "✂️  会被截取的文件分析 (>1024KB):"
truncated_count=$(find "$DATA_DIR" -name "*.txt" -size +1024k | wc -l)
if [ $truncated_count -gt 0 ]; then
    find "$DATA_DIR" -name "*.txt" -size +1024k -printf "%s %p\n" | sort -nr | awk '
    BEGIN { printf "  总共 %d 个文件会被截取\n\n", '$truncated_count' }
    {
        size_kb = $1/1024
        size_mb = $1/1024/1024
        filename = $2
        gsub(/.*\//, "", filename)
        
        total_truncated_size += $1
        
        if (size_mb >= 1) {
            printf "    %8.2f MB - %s\n", size_mb, filename
        } else {
            printf "    %8.1f KB - %s\n", size_kb, filename
        }
    }
    END {
        printf "\n  📊 截取影响:\n"
        printf "    被截取文件总大小: %.2f MB\n", total_truncated_size/1024/1024
        printf "    平均每个被截取文件: %.1f KB\n", total_truncated_size/'$truncated_count'/1024
    }'
else
    echo "  📝 没有找到需要截取的文件"
fi

echo ""
echo "✅ 大文件分析完成!"
