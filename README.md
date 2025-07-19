# LSMFS

- en [English](README.md)

- zh_CN [简体中文](README.zh_CN.md)

A file system based on the LSM-tree and FUSE architectures.

# Required Dependencies

**FUSE Library**

- Install the FUSE development library.

```
sudo apt-get install libfuse-dev
```

**Rocksdb Library**

- Install Rocksdb and related dependencies.

```
sudo apt-get install build-essential
sudo apt-get install libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev
```

- Download the RocksDB source code.

```
git clone https://github.com/facebook/rocksdb.git
```

# Usage

- Ensure that the RocksDB source code and the lsm_fuse source code are in the same directory with the following structure:

- - Current Directory

  -- lsm_fuse

  -- rocksdb

- Compile the code.

```
cd lsm_fuse
./compile.sh
```

- Mount and run the file system.

```
mkdir /tmp/mnt
./lsmfs /tmp/mnt mydb
```

- Test the file system.

```
# Use common commands to test the file system, such as touch, cat, and vi.
cd /tmp/mnt
```

- Unmount the file system.

```
fusermount -u /tmp/mnt
```

# Compatibility

- Passed 8832 basic file operation-related test cases in the file system test suite pjdfstests.
