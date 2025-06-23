#ifndef LSM_FUSE_IN_H
#define LSM_FUSE_IN_H
#include <mutex>
#include "lsf_dentry.h"
#include "lsf.h"
#include "lsf_common.h"
#include "lsf_utils.h"
#include "lsf_data_merge.h"
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/convenience.h>
#include <rocksdb/statistics.h>
#include <rocksdb/utilities/object_registry.h>
#include <rocksdb/utilities/transaction_db.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pthread.h>
#include <uv.h>
/*
#include <unordered_map>
#include <list>
#include <string>
#include <cstddef>
*/
//struct LsfInode;

//using namespace ROCKSDB_NAMESPACE;
using namespace LSF;

struct LsfSuperBlock
{
	std::string magic_str;
	int32_t version;
};

struct lsf_inode_num_t
{
	int64_t i_num;
	explicit lsf_inode_num_t(int64_t i) : i_num(i) {}
	__always_inline int64_t to_int()
	{
		return i_num;
	}
	struct lsf_inode_num_t from_int(int64_t i_num)
	{
		return lsf_inode_num_t{i_num};
	}
};

struct LsfFileHandle
{
	// LsfDentry::Ptr dentry;
	// LsfInode *inode;  // 关联的 inode
    // // 可以添加其他需要的信息，如文件偏移量等
    // off_t offset;
	int dirty;
	std::atomic<int> error;   // 0 表示目前没错，<0 表示后台写失败时的 -errno
	LsfFileHandle();
	~LsfFileHandle();
};



struct LsfFile
{
	LsfInode *inode;
	std::string file_name;
	uint64_t inode_num;
	uint64_t inode_parent_dir_num;
	int noatime;
	int nomtime;
	int dirty;
	LsfFile();
	~LsfFile();
};
/*
template<typename K, typename V>
class LRUCache {
public:
    LRUCache(size_t capacity) : capacity_(capacity) {}

    bool get(const K &key, V &value) {
        auto it = cache_map_.find(key);
        if (it == cache_map_.end())
            return false;
        // 将该节点移到 list 头部，因为它是最新使用的
        cache_list_.splice(cache_list_.begin(), cache_list_, it->second);
        value = it->second->second;
        return true;
    }

    void put(const K &key, const V &value) {
        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            // 更新已有的值，并将节点移到头部
            it->second->second = value;
            cache_list_.splice(cache_list_.begin(), cache_list_, it->second);
            return;
        }
        // 插入新的项到 list 头部
        cache_list_.emplace_front(key, value);
        cache_map_[key] = cache_list_.begin();
        if (cache_map_.size() > capacity_) {
            // 删除末尾节点（最久未使用的）
            auto last = cache_list_.end();
            last--;
            cache_map_.erase(last->first);
            cache_list_.pop_back();
        }
    }

private:
    size_t capacity_;
    // list 的每个节点存储一对 key-value，头部为最新使用的
    std::list<std::pair<K, V>> cache_list_;
    // map 用于快速定位 list 中的节点
    std::unordered_map<K, typename std::list<std::pair<K, V>>::iterator> cache_map_;
};
*/
class LsfContext
{
private:
	LsfContext(const LsfContext&) = delete;
	LsfContext& operator=(const LsfContext&) = delete;
	static LsfContext* instance;
	bool is_stack_object_;


public:
	static std::mutex mutex;
	std::string db_path;
	std::string mount_point;
	rocksdb::TransactionDB *db;
	time_t mount_time;
	rocksdb::ColumnFamilyHandle *default_cf;
	rocksdb::ColumnFamilyHandle *meta_cf;
	rocksdb::ColumnFamilyHandle *data_cf;

//        LRUCache<std::string,uint64_t> path_cache {5000};

	rocksdb::WriteOptions meta_opt;
	rocksdb::WriteOptions data_opt;
	rocksdb::ReadOptions read_opt;

	pthread_t libuv_thread;
	// struct fuse_session *se;
	struct LsfInode root_inode;
	std::unique_ptr<LsfFile> root_file;
	// 添加 statistics 成员
    std::shared_ptr<rocksdb::Statistics> statistics;

	int64_t inode_seed;
	//	int lazy_time; //1: update time lazy, i.e. option 'lazytime' is specified on mount
	// 1 by default
	//	int relatime;  //1 if 'relatime' is specified on mount
	int64_t generate_inode_num();
        
        std::atomic<int> getattr_count{0};
        std::atomic<int> mkdir_count{0};
        std::atomic<int> create_count{0};
        std::atomic<int> write_count{0};
        std::atomic<int> release_count{0};


	// 声明构造函数
    LsfContext();

	static LsfContext* getInstance();
	// 初始化方法
    bool initialize(/* 需要的参数 */) {
        // 原有的初始化逻辑
        return true;
    }

    // 清理方法
    void cleanup() {
		LOG_INFO("~LsfContext start");
    	if (db != nullptr) {
	        // 先刷新 WAL
	        LOG_DEBUG("FlushWal start");
	        db->FlushWAL(true);
	
	
	        LOG_DEBUG("DestroyColumnFamilyHandle(meta_cf) start");
	        // 按顺序销毁 column family handles
	        if (meta_cf != nullptr) {
	            db->DestroyColumnFamilyHandle(meta_cf);
	            meta_cf = nullptr;
	        }
	
	        LOG_DEBUG("DestroyColumnFamilyHandle(data_cf) start");
	        if (data_cf != nullptr) {
	            db->DestroyColumnFamilyHandle(data_cf);
	            data_cf = nullptr;
	        }
	
	        LOG_DEBUG("DestroyColumnFamilyHandle(default_cf) start");
	        if (default_cf != nullptr) {
	            db->DestroyColumnFamilyHandle(default_cf);
	            default_cf = nullptr;
	        }
	
	        // 关闭数据库
	        LOG_DEBUG("close db start");
	        db->Close();

    	    // 删除数据库对象
	        delete db;
    	    db = nullptr;
	    }
    	LOG_INFO("~LsfContext end");

    }
	~LsfContext() {
		cleanup();
		if (is_stack_object_ && instance == this) {
            instance = nullptr;
        }
    }
};

std::vector<std::string> split_path(const std::string &path);
// 主函数：获取父目录的 inode number
uint64_t get_parent_inode_num(LsfContext *ctx, const std::string &path);
// 初始化上下文
void init_context(const char *db_path, LsfContext *ctx);
// 释放资源
void free_context(LsfContext* ctx);

//LsfContext *get_fs_context();

struct block_key
{
	union
	{
		struct
		{
			__le64 block_index;
			__le64 inode_no;
		};
		char keybuf[16];
	};
	const char *to_string() const;
};

#define LSF_FULL_BLOCK_BMP (uint16_t)0xffff

struct block_head
{
	int8_t flags;
	int8_t pad0;
	union
	{
		uint16_t data_bmp;
		uint16_t merge_off;
	};
	char pad1[12];
};
static_assert(sizeof(struct block_head) == 16, "sizeof struct block_head not expected 16");
#define LSF_BLOCK_HEAD_SIZE sizeof(struct block_head)

static __always_inline int64_t deserialize_int64(const char *s)
{
	return *(int64_t *)s;
}

#define DO_FUNC(exp)                                                          \
	do                                                                        \
	{                                                                         \
		LOG_INFOF("do function %s. ", #exp);                                  \
		Status s = exp;                                                       \
		if (!s.ok())                                                          \
		{                                                                     \
			LOG_FATALF("Failed: `%s` status:%s", #exp, s.ToString().c_str()); \
		}                                                                     \
		LOG_INFOF("%s function end", #exp);                                   \
	} while (0)

void deserialize_superblock(const char *buf, LsfSuperBlock &sb);
std::string serialize_superblock(const LsfSuperBlock &sb);
void setup_db_options(rocksdb::Options &options);
void setup_data_cf_options(rocksdb::ColumnFamilyOptions &options);
void setup_meta_cf_options(rocksdb::ColumnFamilyOptions &options);

#endif
