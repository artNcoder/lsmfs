#!/bin/bash

# 定义目标目录
target_dir="/data/HDD_Sdd_data/lsmfs_test"

# 检查目标目录是否存在，不存在则创建
if [ ! -d "$target_dir" ]; then
    mkdir -p "$target_dir"
    if [ $? -ne 0 ]; then
        echo "无法创建目录 $target_dir"
        exit 1
    fi
fi

# 循环创建 44 个文件夹
for i in $(seq -w 001 044); do
    folder_name="$target_dir/A-$i"
    # 如果文件夹存在，先删除
    if [ -d "$folder_name" ]; then
        rm -rf "$folder_name"
    fi
    # 创建文件夹
    mkdir -p "$folder_name"
    if [ $? -eq 0 ]; then
        echo "成功创建文件夹 $folder_name"
    else
        echo "创建文件夹 $folder_name 失败"
    fi
done
