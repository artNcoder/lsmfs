#### LSMFS

##### Commands

```
sync

echo 3 > /proc/sys/vm/drop_caches

rsync --stats -r -t -W  --inplace /home/zhoujun/filebench/files/linux /data/HDD_Sdd_data/lsmfs_low_level
```

##### Results

Number of files: 93,793 (reg: 87,869, dir: 5,861, link: 63)
Number of created files: 93,730 (reg: 87,869, dir: 5,861)
Number of deleted files: 0
Number of regular files transferred: 87,869
Total file size: 4,132,378,976 bytes
Total transferred file size: 4,132,376,864 bytes
Literal data: 4,132,376,864 bytes
Matched data: 0 bytes
File list size: 1,834,855
File list generation time: 0.001 seconds
File list transfer time: 0.000 seconds
Total bytes sent: 4,138,485,976
Total bytes received: 1,717,830

sent 4,138,485,976 bytes  received 1,717,830 bytes  75,967,042.31 bytes/sec=9.06
total size is 4,132,378,976  speedup is 1.00



#### PASSTHROUGH_HP_EXT4

##### Commands

```
rm -rf /data/HDD_Sdd_data/passthrough_hp_ext4/*

rm -rf /data/HDD_Sdd_data/mountponit/*

sync

echo 3 > /proc/sys/vm/drop_caches

./passthrough_hp /data/HDD_Sdd_data/passthrough_hp_ext4 /data/HDD_Sdd_data/mountponit

rsync --stats -r -t -W  --inplace /home/zhoujun/filebench/files/linux /data/HDD_Sdd_data/mountponit
```

##### Result

Number of files: 93,793 (reg: 87,869, dir: 5,861, link: 63)
Number of created files: 93,730 (reg: 87,869, dir: 5,861)
Number of deleted files: 0
Number of regular files transferred: 87,869
Total file size: 4,132,378,976 bytes
Total transferred file size: 4,132,376,864 bytes
Literal data: 4,132,376,864 bytes
Matched data: 0 bytes
File list size: 1,834,855
File list generation time: 0.001 seconds
File list transfer time: 0.000 seconds
Total bytes sent: 4,138,485,976
Total bytes received: 1,717,690

sent 4,138,485,976 bytes  received 1,717,690 bytes  66,243,258.66 bytes/sec=7.89MB/s
total size is 4,132,378,976  speedup is 1.00



#### PASSTHROUGH_HP_XFS

##### Commands

```
rm -rf /data/HDD_Sdc_XFS_data/passthrough_hp_xfs/*

rm -rf /data/HDD_Sdc_XFS_data/mountpoint/*

sync

echo 3 > /proc/sys/vm/drop_caches

./passthrough_hp /data/HDD_Sdc_XFS_data/passthrough_hp_xfs /data/HDD_Sdc_XFS_data/mountpoint

rsync --stats -r -t -W  --inplace /home/zhoujun/filebench/files/linux /data/HDD_Sdc_XFS_data/mountpoint
```

##### Result

Number of files: 93,793 (reg: 87,869, dir: 5,861, link: 63)
Number of created files: 93,730 (reg: 87,869, dir: 5,861)
Number of deleted files: 0
Number of regular files transferred: 87,869
Total file size: 4,132,378,976 bytes
Total transferred file size: 4,132,376,864 bytes
Literal data: 4,132,376,864 bytes
Matched data: 0 bytes
File list size: 1,834,855
File list generation time: 0.001 seconds
File list transfer time: 0.000 seconds
Total bytes sent: 4,138,485,976
Total bytes received: 1,717,810

sent 4,138,485,976 bytes  received 1,717,810 bytes  31,484,439.44 bytes/sec=3.75MB/s
total size is 4,132,378,976  speedup is 1.00













