#!/bin/bash

# 定义文件大小区间
SMALL=4096    # 小于4K
MEDIUM=8192   # 4K-8K

# 初始化计数器
total_files=0
total_size=0
small_files=0
small_size=0
medium_files=0
medium_size=0
large_files=0
large_size=0

# 遍历所有文件并统计
while IFS= read -r -d '' file; do
    # 获取文件大小（字节）
    size=$(stat -c "%s" "$file" 2>/dev/null)
    if [[ -z "$size" ]]; then
        continue  # 跳过错误文件
    fi
    
    # 更新计数器
    ((total_files++))
    ((total_size+=size))
    
    if ((size < SMALL)); then
        ((small_files++))
        ((small_size+=size))
    elif ((size <= MEDIUM)); then
        ((medium_files++))
        ((medium_size+=size))
    else
        ((large_files++))
        ((large_size+=size))
    fi
done < <(find . -type f -print0)

# 计算占比（保留两位小数）
calc_percentage() {
    local numerator=$1
    local denominator=$2
    if ((denominator == 0)); then
        echo "0.00%"
    else
        echo "scale=2; ($numerator/$denominator)*100" | bc -l | awk '{printf "%.2f%%\n", $1}'
    fi
}

# 转换字节为易读单位
format_size() {
    local size=$1
    if ((size < 1024)); then
        echo "${size}B"
    elif ((size < 1048576)); then
        echo "$(echo "scale=2; $size/1024" | bc -l)K"
    elif ((size < 1073741824)); then
        echo "$(echo "scale=2; $size/1048576" | bc -l)M"
    else
        echo "$(echo "scale=2; $size/1073741824" | bc -l)G"
    fi
}

# 输出结果
echo "=== Linux内核源码文件大小统计 ==="
echo "总文件数: $total_files"
echo "总大小: $(format_size $total_size)"
echo

echo "1. 小于4K的文件:"
echo "   - 文件数: $small_files ($(calc_percentage $small_files $total_files))"
echo "   - 总大小: $(format_size $small_size) ($(calc_percentage $small_size $total_size))"

echo "2. 4K-8K的文件:"
echo "   - 文件数: $medium_files ($(calc_percentage $medium_files $total_files))"
echo "   - 总大小: $(format_size $medium_size) ($(calc_percentage $medium_size $total_size))"

echo "3. 大于8K的文件:"
echo "   - 文件数: $large_files ($(calc_percentage $large_files $total_files))"
echo "   - 总大小: $(format_size $large_size) ($(calc_percentage $large_size $total_size))"
