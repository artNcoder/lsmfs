#!/usr/bin/env bash
set -eo pipefail

# 配置参数
SOURCE_DIR="/home/zhoujun/filebench/files/linux/"
TEST_DIR="/data/HDD_Sdd_data/linux-headfiles"
FILE_EXT=".h"
NEW_EXT=".new"
TEST_ROUNDS=5

# 依赖检查
check_deps() {
    local deps=("find" "cp" "sync" "date" "bc" "mkdir")
    for cmd in "${deps[@]}"; do
        if ! command -v "$cmd" &>/dev/null; then
            echo "错误：缺失必要命令 - $cmd" >&2
            exit 1
        fi
    done
}

# 安全创建目录
create_test_dir() {
    if ! mkdir -p "$TEST_DIR"; then
        echo "无法创建测试目录：$TEST_DIR" >&2
        exit 1
    fi
    if ! touch "$TEST_DIR/.write_test"; then
        echo "目录不可写：$TEST_DIR" >&2
        exit 1
    fi
    rm -f "$TEST_DIR/.write_test"
}



# 安全拷贝文件
safe_copy() {
    local src_file="$1"
    local dest_file="$2"
    
    if ! cp -f -- "$src_file" "$dest_file"; then
        echo -e "\n拷贝失败: $src_file -> $dest_file" >&2
        echo "错误代码: $?" >&2
        exit 1
    fi
}

# 文件准备
prepare_files() {
    echo "准备测试文件..."
    rm -rf "${TEST_DIR:?}/"*
    
    # 生成安全文件列表
    local file_list=()
    while IFS= read -r -d '' file; do
        file_list+=("$file")
    done < <(find "$SOURCE_DIR" -type f -name "*${FILE_EXT}" -print0 2>/dev/null)
    
    if (( ${#file_list[@]} == 0 )); then
        echo "错误：未找到任何${FILE_EXT}文件" >&2
        exit 1
    fi
    
    # 显示拷贝进度
    local total=${#file_list[@]}
    echo "正在拷贝 $total 个文件..."
    for ((i=0; i<total; i++)); do
        src_file="${file_list[$i]}"
        base_name=$(basename -- "$src_file")
        dest_file="${TEST_DIR}/${base_name}"
        
        printf "\r进度: %d/%d (%.1f%%)" $((i+1)) $total $(echo "scale=1;100*($i+1)/$total" | bc)
        safe_copy "$src_file" "$dest_file"
    done
    echo -e "\n验证文件数量..."
    
    # 最终验证
    local actual_count=$(find "$TEST_DIR" -type f | wc -l)
    
    echo "成功准备 $actual_count 个测试文件"
}



main() {
    check_deps
    create_test_dir
    prepare_files
}

trap 'echo -e "\n操作中断，清理中..."; rm -rf "$TEST_DIR"; exit 1' INT TERM
main
