#!/bin/bash

#LSMFS on HDD
DIRECTORY="/data/HDD_Sdd_data/lsmfs_test8"

#EXT4 on HDD
#DIRECTORY="/data/HDD_Sdd_data/microwrites_ext4"

#XFS on HDD
#DIRECTORY="/data/HDD_Sdc_XFS_data/microwrites_xfs"

#EXT4 on SSD
#DIRECTORY="/data/SSD_Sdb_data/microwrites_SSD"


#EXT4 on NVME
#DIRECTORY="/data/NVME0_SSD_data/microwrites_NVME"

SIZE="4K"
BS="1K"
NUMJOBS=1024
N=262144

CPU_CORES="0-3"



if [ -d "$DIRECTORY" ]; then
    echo "检测到已存在目录，正在清空内容..."
    if ! rm -rf "${DIRECTORY:?}"/*; then  # 安全删除防止误操作
        echo "错误：无法清空目录内容" >&2
        exit 1
    fi
else
    mkdir -p "$DIRECTORY" || exit 1
fi

# 新增：清空系统缓存（需要sudo权限）
sync  # 确保数据落盘
if ! sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'; then
    echo "警告: 清空缓存失败，可能需要sudo权限" >&2
fi

# 计算循环次数
LOOPS=$((N / NUMJOBS))
TMP_LOG_DIR="/tmp/fio_logs_$$"
mkdir -p "$TMP_LOG_DIR"

# 记录开始时间
START_TIME=$(date +%s)
START_TIME_HR=$(date "+%Y-%m-%d %H:%M:%S")
LAST_CHECK_TIME=$START_TIME
TOTAL_FILES_GENERATED=0
TEN_SECOND_FILES=0

echo "============================================="
echo "Start time: $START_TIME_HR"
echo "Numer of files: $N"
echo "Fio numjobs: $NUMJOBS"
echo "Loop times: $LOOPS"
echo "Storage directory: $DIRECTORY"
echo "Log directory: $TMP_LOG_DIR"
echo "============================================="

# 主循环
for ((i=1; i<=LOOPS; i++)); do
    # 创建子目录
    SUBDIR="${DIRECTORY}/dir_${i}"
    if ! mkdir -p "$SUBDIR"; then
        echo "错误：无法创建目录 $SUBDIR" >&2
        exit 1
    fi

    # 运行fio测试
     taskset -c $CPU_CORES fio --directory="$SUBDIR" \
        --size="$SIZE" \
        -direct=1 \
        -iodepth=32 \
        -thread \
        -rw=randwrite \
        -ioengine=libaio \
        -bs="$BS" \
        -numjobs="$NUMJOBS" \
        -runtime=30 \
        -group_reporting \
        -name=randw0 \
        > "${TMP_LOG_DIR}/loop_${i}.log" 2>&1

    # 进度显示
    if (( i % 100 == 0 )); then
        echo "progress:  $i/$LOOPS cycles ($((i*100/LOOPS))% have been completed.)"
    fi

    TOTAL_FILES_GENERATED=$((TOTAL_FILES_GENERATED + NUMJOBS))
    TEN_SECOND_FILES=$((TEN_SECOND_FILES + NUMJOBS))

    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - LAST_CHECK_TIME))
    if [ $ELAPSED_TIME -ge 10 ]; then
        SPEED=$((TEN_SECOND_FILES / ELAPSED_TIME))
        echo "Files generated in the last ${ELAPSED_TIME} seconds: $TEN_SECOND_FILES, Speed: $SPEED files/s"
        LAST_CHECK_TIME=$CURRENT_TIME
        TEN_SECOND_FILES=0
    fi
done

# 清理临时日志
rm -rf "$TMP_LOG_DIR"

# 统计计算
END_TIME=$(date +%s)
ELAPSED_SEC=$((END_TIME - START_TIME))
TOTAL_FILES=$((LOOPS * NUMJOBS))

# 使用awk进行单位转换计算
SIZE_BYTES=$(echo $SIZE | awk '{
    unit = substr($0, length($0), 1)
    val = substr($0, 1, length($0)-1)
    if (unit == "K") factor=1024
    if (unit == "M") factor=1024^2
    if (unit == "G") factor=1024^3
    print val * factor
}')

TOTAL_SIZE=$((TOTAL_FILES * SIZE_BYTES))

# 格式化输出函数
format_bytes() {
    echo $1 | awk '{
        if ($0 == 0) print "0 B"
        split("B KB MB GB TB PB", units)
        base = 1024
        size = $0
        for (i=1; size >= base && i < 6; i++) size /= base
        printf "%.2f %s", size, units[i]
    }'
}

# 计算文件平均生成速度
AVERAGE_FILE_SPEED=$((TOTAL_FILES / ELAPSED_SEC))

echo
echo "================ Final statistical report ================"
echo "Storage directory: $DIRECTORY"        
echo "Blocksize(bs): $BS"             
echo "The number of generated files: $(printf "%'d" $TOTAL_FILES)"
echo "The size of single file: $SIZE"
echo "Total elapsed time: $((ELAPSED_SEC/3600))hour $((ELAPSED_SEC%3600/60))minutes $((ELAPSED_SEC%60))seconds"
echo "Average speed: $(format_bytes $((TOTAL_SIZE/ELAPSED_SEC)) )/s"
echo "Average file generation speed: $AVERAGE_FILE_SPEED files/s"
echo "============================================="
