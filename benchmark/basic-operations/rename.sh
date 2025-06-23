#!/bin/bash
# 文件名：cache_benchmark.sh
# 描述：存储性能基准测试工具（多轮测试版）

# 严格模式设置
set -eo pipefail


ARCHIVE_PATH="/home/zhoujun/filebench/files/linux-headfiles.tar.gz"
TARGET_DIR="/data/HDD_Sdd_data/linux-headfiles"                 #HDD
#TARGET_DIR="/data/SSD_Sdb_data"                #SSD
#TARGET_DIR="/data/NVME0_SSD_data"              #NVME
#TARGET_DIR="/data/HDD_Sdd_data/lsmfs_test"     #LSMFS

TEST_ROUNDS=5
FILE_EXT=".h"
NEW_EXT=".new"



# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # 关闭颜色

# 权限检查
if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}错误：本脚本需要root权限执行${NC}"
    echo -e "请使用以下命令运行："
    echo -e "sudo $0"
    exit 1
fi

# 缓存清理函数
clean_system_cache() {
    echo -e "${YELLOW}[清理系统缓存]${NC}"
    sync
    echo 3 > /proc/sys/vm/drop_caches
    if swapoff -a &>/dev/null; then
        swapon -a
    fi
    echo 3 > /proc/sys/vm/drop_caches
}


# 执行解压操作
extract_archive() {
	
	rm -rf $TARGET_DIR/*
	
    
    clean_system_cache
    echo "解压测试文件中..."
    tar -xzf "$ARCHIVE_PATH" -C "$TARGET_DIR" --overwrite
    sync
}


# 批量重命名计时
batch_rename() {
    local start_time end_time time_diff
    start_time=$(date +%s.%N)
    

    find "$TARGET_DIR" -type f -name "*${FILE_EXT}" -exec bash -c '
        for file; do
            mv -f -- "$file" "${file%'"${FILE_EXT}"'}'"${NEW_EXT}"'"
        done
    ' _ {} +
    
    end_time=$(date +%s.%N)
    time_diff=$(echo "$end_time - $start_time" | bc -l)
    echo -e "${BLUE}▶ 本次耗时: ${time_diff} 秒${NC}" >&2
    echo "$time_diff" # 返回纯数字结果
}



# 统计结果计算
calculate_stats() {
    local times=("$@")
    local total=0
    local min=${times[0]}
    local max=${times[0]}
    
    for time in "${times[@]}"; do
        total=$(echo "$total + $time" | bc -l)
        if (( $(echo "$time < $min" | bc -l) )); then
            min=$time
        fi
        if (( $(echo "$time > $max" | bc -l) )); then
            max=$time
        fi
    done
    
    local avg=$(echo "scale=4; $total / ${#times[@]}" | bc -l)
    
    echo -e "\n${YELLOW}====== 性能统计报告 ======${NC}"
    echo -e "测试轮次:    ${TEST_ROUNDS} 次"
    echo -e "平均耗时:    ${GREEN}${avg} 秒${NC}"
    echo -e "最快耗时:    ${GREEN}${min} 秒${NC}"
    echo -e "最慢耗时:    ${RED}${max} 秒${NC}"
    echo -e "波动范围:    ${BLUE}$(echo "$max - $min" | bc -l) 秒${NC}"
}

# 主执行流程
main() {
    declare -a results
    
    echo -e "${YELLOW}===== 开始性能测试 ====${NC}"

    # 初始解压（确保首次测试文件存在）
    extract_archive

    for ((i=1; i<=TEST_ROUNDS; i++)); do
        echo -n "第 $i 轮测试..."

        results+=("$(batch_rename)")

         # 重新准备测试文件（最后一次不需要）
        if ((i < TEST_ROUNDS)); then
            extract_archive
        fi
    done
    
    calculate_stats "${results[@]}"
}



# 执行主函数
main

# 安全退出
exit 0
