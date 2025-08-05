# LSMFS

一个基于 **LSM-tree** 和 **FUSE** 架构构建的高性能文件系统。

[🇺🇸 English](README.md) | [🇨🇳 简体中文](README.zh_CN.md)

---

## 📋 目录

- [项目简介](#项目简介)
- [系统架构](#系统架构)
- [依赖环境](#依赖环境)
- [安装步骤](#安装步骤)
- [使用方法](#使用方法)
- [兼容性](#兼容性)

---

## 📖 项目简介

**LSMFS** 是一个用户态文件系统，结合了 [RocksDB](https://github.com/facebook/rocksdb) 的高性能键值存储引擎和 [FUSE](https://github.com/libfuse/libfuse) 框架。它适用于高吞吐量文件操作的实验性或教学环境。

---

## 🏗️ 系统架构

下图展示了 LSMFS 的整体架构：

<img src="pics/LSMFS_structure.png" alt="LSMFS 架构图" width="50%" />

---

## 📦 依赖环境

### FUSE

安装 FUSE 开发库：

```bash
sudo apt-get install libfuse-dev
```

### RocksDB

安装 RocksDB 所需的依赖项：

```bash
sudo apt-get install build-essential
sudo apt-get install libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev
```

克隆 RocksDB 源代码：

```bash
git clone https://github.com/facebook/rocksdb.git
```

------

## ⚙️ 安装步骤

请确保目录结构如下：

```
当前目录
├── lsm_fuse     # LSMFS 源码目录
└── rocksdb      # 克隆的 RocksDB 目录
```

编译项目：

```bash
cd lsm_fuse
./compile.sh
```

------

## 🚀 使用方法

### 挂载文件系统

```bash
mkdir /tmp/mnt
./lsmfs /tmp/mnt mydb
```

### 测试文件系统

现在可以使用常用的文件操作命令，如 `touch`、`cat`、`vi` 等：

```bash
cd /tmp/mnt
touch testfile
cat testfile
```

### 卸载文件系统

```bash
fusermount -u /tmp/mnt
```

------

## ✅ 兼容性

- 已通过 `pjdfstests` 测试套件中的 **8,832** 个文件操作相关测试用例，表现稳定。

------

## 📄 许可证

本项目基于 [Apache License 2.0](LICENSE) 协议发布。

------

## 🤝 参与贡献

欢迎提交 Issues 和 PR，参与开发与维护！

---

## 🙏 致谢

特别感谢 [ViveNAS 项目](https://github.com/cocalele/ViveNAS) 为本项目提供的灵感与参考。

------

## 📫 联系我们

如有任何疑问或建议，请提交 issue 或联系项目维护者。
