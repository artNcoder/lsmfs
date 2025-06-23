#### LSMFS

##### untar

```
sync

echo 3 > /proc/sys/vm/drop_caches

cd /data/HDD_Sdd_data/lsmfs_low_level

/usr/bin/time -p  tar -xf ~/filebench/files/linux.tar.gz
```


real 58.20
user 30.45
sys 12.45

##### tar

```
sync

echo 3 > /proc/sys/vm/drop_caches

/usr/bin/time  -p tar -zcvf linux-new.tar.gz linux >/dev/null
```

real 129.18
user 114.24
sys 10.32



#### PASSTHROUGH_HP_EXT4

```
rm -rf /data/HDD_Sdd_data/passthrough_hp_ext4/*

rm -rf /data/HDD_Sdd_data/mountponit/*

sync

echo 3 > /proc/sys/vm/drop_caches

./passthrough_hp /data/HDD_Sdd_data/passthrough_hp_ext4 /data/HDD_Sdd_data/mountponit
```



##### Untar

```
sync

echo 3 > /proc/sys/vm/drop_caches

cd /data/HDD_Sdd_data/mountponit

 /usr/bin/time -p  tar -xf /home/zhoujun/filebench/files/linux.tar.gz 
```


real 64.93
user 30.02
sys 19.90

##### tar

```
sync

echo 3 > /proc/sys/vm/drop_caches

 /usr/bin/time  -p tar -zcvf linux-new.tar.gz linux >/dev/null
```


real 157.58
user 120.92
sys 11.53



#### PASSTHROUGH_HP_XFS

```
rm -rf /data/HDD_Sdc_XFS_data/passthrough_hp_xfs/*

rm -rf /data/HDD_Sdc_XFS_data/mountpoint/*

sync

echo 3 > /proc/sys/vm/drop_caches

./passthrough_hp /data/HDD_Sdc_XFS_data/passthrough_hp_xfs /data/HDD_Sdc_XFS_data/mountpoint
```

##### untar

```
sync

echo 3 > /proc/sys/vm/drop_caches

cd /data/HDD_Sdc_XFS_data/mountpoint

/usr/bin/time -p  tar -xf /home/zhoujun/filebench/files/linux.tar.gz
```


real 60.93
user 31.37
sys 19.90

##### tar

```
sync

echo 3 > /proc/sys/vm/drop_caches

/usr/bin/time  -p tar -zcvf linux-new.tar.gz linux >/dev/null
```


real 143.47
user 116.29
sys 11.64



