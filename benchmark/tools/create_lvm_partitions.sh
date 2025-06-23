#!/bin/bash
set -euo pipefail

# 配置参数
VG_NAME="android-data"
LV_PREFIX="A-"
LV_COUNT=44
LV_SIZE="32G"
MOUNT_BASE="/opt/android-data"
DEVICE="/dev/sdc"

# 设备卸载函数
unmount_device() {
    echo "▣ 检查设备挂载状态：$DEVICE"
    local mounted_points=$(lsblk -nro MOUNTPOINT $DEVICE | grep -v '^$')

    if [ -n "$mounted_points" ]; then
        echo "检测到以下挂载点需要处理："
        printf "• %s\n" $mounted_points

        # 安全卸载流程
        for point in $mounted_points; do
            echo "正在卸载 $point ..."
            if ! umount $point; then
                echo "尝试强制卸载..."
                umount -f $point || { echo "卸载失败，请手动处理"; exit 1; }
            fi
        done

        # 二次确认卸载结果
        if lsblk -nro MOUNTPOINT $DEVICE | grep -q .; then
            echo "错误：仍有残留挂载点"
            exit 1
        fi
        echo "✓ 设备卸载完成"
    else
        echo "设备当前未挂载"
    fi
}

# 清理已有卷组
cleanup_existing_vg() {
    if vgdisplay $VG_NAME &>/dev/null; then
        echo "▣ 清理现有LVM配置"

        # 卸载逻辑卷挂载点
        mount | awk -v vg="$VG_NAME" \
            '$1 ~ "^/dev/mapper/" vg "-" {print $3}' | xargs -r umount

        # 移除卷组及物理卷
        vgremove -y $VG_NAME >/dev/null
        pvremove -y $DEVICE 2>/dev/null || true

        echo "✓ 历史卷组已清除"
    fi
}

# 创建LVM存储
create_lvm_structure() {
    echo "▣ 初始化存储配置"

    pvcreate -y $DEVICE > /dev/null
    vgcreate $VG_NAME $DEVICE > /dev/null

    # 批量创建逻辑卷
    for i in $(seq -f "%03g" 1 $LV_COUNT); do
        lvcreate -n ${LV_PREFIX}$i -L $LV_SIZE $VG_NAME -y >/dev/null
    done

    echo "✓ 创建完成 ${LV_COUNT} 个逻辑卷"
}

# 格式化并挂载
setup_filesystems() {
    echo "▣ 文件系统初始化"

    # 并行格式化
    for lv in /dev/$VG_NAME/${LV_PREFIX}*; do
        mkfs.ext4 -q -F $lv &
    done
    wait

    # 创建挂载结构
    mkdir -p $MOUNT_BASE
    for i in $(seq -f "%03g" 1 $LV_COUNT); do
        mount_point="$MOUNT_BASE/${LV_PREFIX}$i"
        mkdir -p $mount_point
        mount /dev/$VG_NAME/${LV_PREFIX}$i $mount_point
    done

    echo "✓ 文件系统已挂载"
}

# 主执行流程
{
    unmount_device
    cleanup_existing_vg
    create_lvm_structure
    setup_filesystems
}

echo -e "\n◆ 操作结果验证："
lsblk -o NAME,SIZE,MOUNTPOINT $DEVICE
echo -e "\n所有操作已完成，设备已就绪！"                                           
