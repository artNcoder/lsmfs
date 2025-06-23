#!/bin/bash

#LSMFS on HDD
DIRECTORY="/data/HDD_Sdd_data/spaceoverheads_1K"

#EXT4 on HDD
#DIRECTORY="/data/HDD_Sdd_data/spaceoverheads_1K"

#XFS on HDD
#DIRECTORY="/data/HDD_Sdd_data/spaceoverheads_1K"

#total size =1GB =SIZE * N
SIZE="1K"
BS="1K"
NUMJOBS=512
N=1048576
# 假设使用的磁盘是 sdd，可根据实际情况修改
DISK="sdd"

# 新增：存储目录预处理
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

# 记录 fio 开始前磁盘写入数据量
START_WRITTEN=$(iostat -d -k $DISK | awk 'NR==4 {print $7}')

echo "============================================="
echo "作业开始时间: $START_TIME_HR"
echo "目标生成文件数: $N"
echo "每次生成文件数: $NUMJOBS"
echo "总循环次数: $LOOPS"
echo "存储目录: $DIRECTORY"
echo "临时日志目录: $TMP_LOG_DIR"
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
    fio --directory="$SUBDIR" \
        --size="$SIZE" \
        -direct=1 \
        -iodepth=2 \
        -thread \
        -rw=write \
        -ioengine=libaio \
        -bs="$BS" \
        -numjobs="$NUMJOBS" \
        -runtime=30 \
        -group_reporting \
        -name=randw0 \
        --randrepeat=0 \
        --refill_buffers \
        --buffer_compress_percentage=0 \
        > "${TMP_LOG_DIR}/loop_${i}.log" 2>&1

    # 进度显示
    if (( i % 100 == 0 )); then
        echo "进度: 已完成 $i/$LOOPS 个循环 ($((i*100/LOOPS))%)"
    fi
done

sleep 10

# 记录 fio 结束后磁盘写入数据量
END_WRITTEN=$(iostat -d -k $DISK | awk 'NR==4 {print $7}')

# 计算 fio 运行期间磁盘写入数据量
TOTAL_WRITTEN=$((END_WRITTEN - START_WRITTEN))

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
ACTUAL_SIZE=$(du -sh "$DIRECTORY" 2>/dev/null | awk '{print $1}')
RATIO=$(awk "BEGIN {printf \"%.2f\", $ACTUAL_SIZE/$TOTAL_SIZE}")

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

echo
echo "================ Final statistical report ================"
echo "Storage directory: $DIRECTORY"        
echo "Blocksize(bs): $BS"             
echo "实际生成文件数: $(printf "%'d" $TOTAL_FILES)"
echo "Size of single file: $SIZE"
echo "Theoretical size: $(format_bytes $TOTAL_SIZE)"
echo "The actual occupied space size: $ACTUAL_SIZE"
echo "Space magnification ratio: ${RATIO}x"
echo "Total elapsed time: $((ELAPSED_SEC/3600))小时$((ELAPSED_SEC%3600/60))分$((ELAPSED_SEC%60))秒"
echo "Average speed: $(format_bytes $((TOTAL_SIZE/ELAPSED_SEC)) )/s"
echo "The amount of data written to disk: $(format_bytes $((TOTAL_WRITTEN * 1024)) )"
echo "============================================="
