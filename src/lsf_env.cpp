#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/convenience.h>
#include <rocksdb/table.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/filter_policy.h>
#include <cstring>
#include <string>
#include <iostream>
#include "lsf.h"
#include "lsf_i.h"
#include "lsf_utils.h"
#include <assert.h>

using namespace ROCKSDB_NAMESPACE;

using ROCKSDB_NAMESPACE::Options;

int lsf_mkfs(const char *db_path);

class BLKPE : public rocksdb::SliceTransform
{
public:
    BLKPE() {}
    BLKPE(BLKPE *) {}
    virtual const char *Name() const override
    {
        return "BLKTransform";
    }
    virtual Slice Transform(const Slice &key) const
    {
        block_key *m_block_key = (block_key *)key.data();
        return Slice((char *)&m_block_key->inode_no, sizeof(m_block_key->inode_no));
    }
    virtual bool InDomain(const Slice &key) const
    {
        return true;
    }
};

void setup_db_options(Options &options)
{
    // 设置后台线程数量
    options.env->SetBackgroundThreads(8);

    // 设置压缩风格为级别式压缩
    options.compaction_style = kCompactionStyleLevel;

    // 设置写缓冲区大小为64MB
    options.write_buffer_size = 256 << 20; // 单个 memtable 的大小

    // 设置最大写缓冲区数量
    options.max_write_buffer_number = 5;
    options.min_write_buffer_number_to_merge = 2;

    // 设置目标文件大小基准为64MB
    options.target_file_size_base = 128 << 20;

    // 设置最大后台压缩线程数
    // options.max_background_compactions = 8; // 这里跟IncreaseParallelism冲突
    // options.max_subcompactions = 4;         // 对性能影响不大

    // 设置触发 Level 0 文件压缩的文件数阈值
    options.level0_file_num_compaction_trigger = 4;

    // 设置 Level 0 文件数量导致写入减速的阈值
    options.level0_slowdown_writes_trigger = 12;

    // 设置 Level 0 文件数量导致写入停止的阈值
    options.level0_stop_writes_trigger = 16;

    // 从配置文件读取 LSM 树的层数，默认为 4
    //options.num_levels = 4;
    // S5LOG_DEBUG("Use rocksdb num_levels:%d", options.num_levels);

    // 启用动态层级调整
    options.level_compaction_dynamic_level_bytes = true;
    // 设置基础层（通常是 Level 1）的最大字节数为 512MB
    options.max_bytes_for_level_base = 1024 << 20;

    

    // 增加并行度，通过调用预定义的优化函数
    options.IncreaseParallelism();

    // 优化级别式压缩
    options.OptimizeLevelStyleCompaction();

    options.skip_stats_update_on_db_open = true;

    // 如果数据库不存在，则创建
    options.create_if_missing = true;

    //	options.info_log_level = rocksdb::InfoLogLevel::DEBUG_LEVEL;
    options.info_log_level = rocksdb::InfoLogLevel::INFO_LEVEL;
}

void setup_data_cf_options(rocksdb::ColumnFamilyOptions &options)
{
    LOG_DEBUG("setup data cf options start");
    // 优化基于层级的压缩策略

    options.OptimizeLevelStyleCompaction();
    //options.compression = rocksdb::kNoCompression;
    //options.compression_per_level.clear();
    // 在 setup_data_cf_options 和 setup_meta_cf_options 中：
    
    options.compression = rocksdb::kLZ4Compression;  // 使用 LZ4 压缩（轻量且高效）
    options.compression_per_level.resize(7);         // 假设 7 层 LSM 树
    for (int i = 0; i < 7; i++) {
        if (i < 2) options.compression_per_level[i] = rocksdb::kNoCompression;  // L0-L1 不压缩
        else options.compression_per_level[i] = rocksdb::kLZ4Compression;        // L2+ 使用 LZ4
    }
    

    // 配置写入缓冲区大小和数量
    options.write_buffer_size = 256 << 20;        // 设置写入缓冲区大小为512MB
    options.max_write_buffer_number = 5;          // 最多5个immutable memtable
    options.min_write_buffer_number_to_merge = 2; // 至少1个缓冲区就开始合并

    // 设置目标文件大小和从配置文件中读取层级数
    options.target_file_size_base = 128 << 20; // 建议设置为max_bytes_for_level_base/10
    // options.num_levels = 6;
    // LOG_DEBUGF("Use rocksdb num_levels(data_cf):%d", options.num_levels);

    // 配置基于块的表选项
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_size = 64 << 10; // 可以减少索引块
    // table_options.block_align = true;                                        // 启用块对齐
    table_options.block_cache = NewLRUCache(1LL * 1024 * 1024 * 1024, 8);    // 创建1GB的LRU缓存
    table_options.index_type = rocksdb::BlockBasedTableOptions::kHashSearch; // 使用哈希搜索作为索引类型
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));  // 10 bits/key

    // 设置表工厂和自定义的前缀提取器
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.prefix_extractor = std::make_shared<BLKPE>(new BLKPE);

    // 设置级别乘数，控制不同级别之间的大小关系
    // 启用动态层级调整
    options.level_compaction_dynamic_level_bytes = true;
    options.max_bytes_for_level_base = 1024 << 20; // 降低基础层大小
   

    LOG_DEBUG("setup data cf options end");
}

void setup_meta_cf_options(rocksdb::ColumnFamilyOptions &options)
{
    LOG_DEBUG("setup meta cf options start");
    // 优化基于层级的压缩策略
    options.OptimizeLevelStyleCompaction();
    //options.compression = rocksdb::kNoCompression;
    //options.compression_per_level.clear();
    
    options.compression = rocksdb::kLZ4Compression;  // 使用 LZ4 压缩（轻量且高效）
    options.compression_per_level.resize(7);         // 假设 7 层 LSM 树
    for (int i = 0; i < 7; i++) {
        if (i < 2) options.compression_per_level[i] = rocksdb::kNoCompression;  // L0-L1 不压缩
        else options.compression_per_level[i] = rocksdb::kLZ4Compression;        // L2+ 使用 LZ4
    }
    

    // 配置写入缓冲区数量和合并阈值
    options.max_write_buffer_number = 5;          // 最多允许5个写入缓冲区
    options.min_write_buffer_number_to_merge = 2; // 至少1个缓冲区就开始合并

    // 设置目标文件大小
    options.target_file_size_base = 128 << 20; // 上一级SST文件到了多大时触发

    // 配置基于块的表选项
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_size = 64 << 10; // 设置块大小为4MB
    // table_options.block_align = true;   // 启用块对齐  不太清楚作用
    table_options.block_cache = NewLRUCache(512LL * 1024 * 1024);

    options.level_compaction_dynamic_level_bytes = true;
    options.max_bytes_for_level_base = 1024 << 20; // 降低基础层大小

    table_options.cache_index_and_filter_blocks=true;
    table_options.pin_l0_filter_and_index_blocks_in_cache=true;
    // 添加布隆过滤器：
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));  // 10 bits/key

    // 使用配置好的表选项创建并设置表工厂
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    LOG_DEBUG("setup meta cf options end");
}

