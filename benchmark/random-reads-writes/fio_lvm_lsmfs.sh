#!/bin/bash

# 配置参数（用户需直接修改此处）
NUM_JOBS=44                  # ★ 并发任务数（直接修改这个数字）
#ext4
#BASE_DIR="/data/HDD_Sdd_data/randomreadandwrites"        # ★ 测试目录路径（直接修改这个路径）
#lsmfs-high-level
#BASE_DIR="/data/HDD_Sdd_data/lsmfs_high_level"        # ★ 测试目录路径（直接修改这个路径）

#lsmfs-low-level-passthrough
#BASE_DIR="/data/HDD_Sdd_data/lsmfs_low_level"

#lsmfs-low-level
BASE_DIR="/home/dockdroid/dockdroid_file/filetest/lsmfs-lowlevel-nopassthrough"

#LVM
#BASE_DIR="/opt/android-data"

FILENAME="testfile"
SIZE="128M"
MIX_WRITE=80
CONFIG_DIR="/tmp/fio_config"

# 创建测试目录结构
echo "创建测试目录..."
rm -rf "$BASE_DIR" 2>/dev/null
rm -rf "$CONFIG_DIR" 2>/dev/null
mkdir -p "$BASE_DIR"
mkdir -p "$CONFIG_DIR"

for i in $(seq -f "%03g" 1 $NUM_JOBS); do
    mkdir -p "${BASE_DIR}/A-${i}"
done

# 生成fio配置文件
CONFIG_FILE="${CONFIG_DIR}/fio_config.ini"

cat > "$CONFIG_FILE" <<EOF
[global]
ioengine=libaio
direct=1
size=$SIZE
bs=4k
rw=randrw
rwmixwrite=$MIX_WRITE
runtime=30
time_based
group_reporting

EOF

# 添加job段
for i in $(seq -f "%03g" 1 $NUM_JOBS); do
    cat >> "$CONFIG_FILE" <<EOF
[A-$i]
directory=${BASE_DIR}/A-$i
filename=$FILENAME
numjobs=1
stonewall
EOF
done

# 运行测试
echo "开始性能测试..."
fio "$CONFIG_FILE" --output-format=json --output="${CONFIG_DIR}/results.json" | tee "${CONFIG_DIR}/output.log"



# 解析结果

# 计算平均值
read_bw_sum=$(jq '[.jobs[].read.bw // 0] | add' "${CONFIG_DIR}/results.json")
write_bw_sum=$(jq '[.jobs[].write.bw // 0] | add' "${CONFIG_DIR}/results.json")
read_iops_sum=$(jq '[.jobs[].read.iops // 0] | add' "${CONFIG_DIR}/results.json")
write_iops_sum=$(jq '[.jobs[].write.iops // 0] | add' "${CONFIG_DIR}/results.json")
read_lat_sum=$(jq '[.jobs[].read.lat_ns.mean // 0] | add / 1000' "${CONFIG_DIR}/results.json")
write_lat_sum=$(jq '[.jobs[].write.lat_ns.mean // 0] | add / 1000' "${CONFIG_DIR}/results.json")

avg_read_bw=$(echo "scale=2; $read_bw_sum / $NUM_JOBS" | bc)
avg_write_bw=$(echo "scale=2; $write_bw_sum / $NUM_JOBS" | bc)
avg_read_iops=$(echo "scale=2; $read_iops_sum / $NUM_JOBS" | bc)
avg_write_iops=$(echo "scale=2; $write_iops_sum / $NUM_JOBS" | bc)
avg_read_lat=$(echo "scale=2; $read_lat_sum / $NUM_JOBS" | bc)
avg_write_lat=$(echo "scale=2; $write_lat_sum / $NUM_JOBS" | bc)

echo -e "\n平均值统计："
echo "Average Read BW:    $avg_read_bw KiB/s"
echo "Average Write BW:   $avg_write_bw KiB/s"
echo "Average Read IOPS:  $avg_read_iops"
echo "Average Write IOPS: $avg_write_iops"
echo "Average Read Lat:   $avg_read_lat us"
echo "Average Write Lat:  $avg_write_lat us"

# 清理测试文件（取消注释以下行以启用）
#rm -rf "$CONFIG_DIR"