#### LSMFS

##### git

```shell
sync

echo 3 > /proc/sys/vm/drop_caches

/usr/bin/time -p git clone /home/zhoujun/filebench/files/linux
```

Cloning into 'linux'...
done.
Updating files: 100% (87907/87907), done.
real 48.60
user 11.89
sys 8.14

##### git diff

```shell
cd linux

sync

echo 3 > /proc/sys/vm/drop_caches

/usr/bin/time -p git diff --patch v4.7 v4.14 > patch
```

warning: inexact rename detection was skipped due to too many files.
warning: you may want to set your diff.renameLimit variable to at least 9460 and retry the command.
real 155.32
user 20.69
sys 1.41

#### PASSTHROUGH_HP_EXT4

##### Commands

```shell
rm -rf /data/HDD_Sdd_data/passthrough_hp_ext4/*

rm -rf /data/HDD_Sdd_data/mountponit/*

sync

echo 3 > /proc/sys/vm/drop_caches

./passthrough_hp /data/HDD_Sdd_data/passthrough_hp_ext4 /data/HDD_Sdd_data/mountponit
```

##### git

```shell
cd /data/HDD_Sdd_data/mountponit

/usr/bin/time -p git clone /home/zhoujun/filebench/files/linux
```

Cloning into 'linux'...
done.
Updating files: 100% (87907/87907), done.
real 48.47
user 12.87
sys 11.19

##### git diff

```shell
cd linux

sync

echo 3 > /proc/sys/vm/drop_caches

/usr/bin/time -p git diff --patch v4.7 v4.14 > patch
```

warning: inexact rename detection was skipped due to too many files.
warning: you may want to set your diff.renameLimit variable to at least 9460 and retry the command.
real 41.50
user 16.16
sys 1.49



#### PASSTHROUGH_HP_XFS

##### Commands

```shell
rm -rf /data/HDD_Sdc_XFS_data/passthrough_hp_xfs/*

rm -rf /data/HDD_Sdc_XFS_data/mountpoint/*

sync

echo 3 > /proc/sys/vm/drop_caches

./passthrough_hp /data/HDD_Sdc_XFS_data/passthrough_hp_xfs /data/HDD_Sdc_XFS_data/mountpoint
```

##### git

```shell
cd /data/HDD_Sdc_XFS_data/mountpoint

/usr/bin/time -p git clone /home/zhoujun/filebench/files/linux
```

Cloning into 'linux'...
done.
Updating files: 100% (87907/87907), done.
real 45.75
user 12.54
sys 10.98

##### git diff

```shell
cd linux

sync

echo 3 > /proc/sys/vm/drop_caches

/usr/bin/time -p git diff --patch v4.7 v4.14 > patch
```

warning: inexact rename detection was skipped due to too many files.
warning: you may want to set your diff.renameLimit variable to at least 9460 and retry the command.
real 42.92
user 16.24
sys 1.44











