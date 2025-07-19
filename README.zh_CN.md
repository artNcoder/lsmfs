# LSMFS
- en [English](README.md)
- zh_CN [简体中文](README.zh_CN.md)

基于LSM-tree及FUSE架构的通用文件系统

# 所需依赖
**FUSE库**
- 安装FUSE开发库

    ```bash
    sudo apt-get install libfuse-dev
    ```
**Rocksdb库**
- 安装Rocksdb和相关依赖
    ```bash
    sudo apt-get install build-essential
    sudo apt-get install libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev
    ```

-  下载 RocksDB 源码

    ```bash
    git clone https://github.com/facebook/rocksdb.git
    ```

# 使用方法
- 确保rocksdb源码与lsm_fuse源码在同一目录下，结构如下   
\- 当前目录   
&ensp;&ensp;\-- lsm_fuse   
&ensp;&ensp;\-- rocksdb   
- 编译

    ```bash
    cd lsm_fuse
    ./compile.sh
    ```

- 挂载运行文件系统

    ```bash
    mkdir /tmp/mnt
    ./lsmfs /tmp/mnt mydb
    ```
- 测试文件系统

    ```bash
    # 使用常用的命令测试文件系统，如touch，cat，vi
    cd /tmp/mnt
    ```

- 卸载文件系统

    ```bash
    fusermount -u /tmp/mnt
    ```


# 兼容性
- 通过了文件系统测试套件pjdfstests中的8832个基本文件操作相关的测试用例。





