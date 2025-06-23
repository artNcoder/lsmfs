//#pragma once
#ifndef LSF_DENTRY_H
#define LSF_DENTRY_H

#include "lsf_common.h"
//#include "lsf_i.h"
#include <list>
#include <mutex>
#include <sstream>
#include <functional>
#include <unordered_map>
#include <atomic>
#include <string>
#include <sys/stat.h>
#include <time.h>
#include <memory>
#include "lsf_list.h"

struct LsfInode;
class LsfDentry : public std::enable_shared_from_this<LsfDentry>{

    public:
        using Ptr = std::shared_ptr<LsfDentry>;
        friend class DentryCache;

        // 构造函数
        LsfDentry(const std::string& name, uint64_t ino, uint64_t parent, const struct stat& st);
        ~LsfDentry();
        // 设置inode
        void set_inode(LsfInode* inode);

        // 获取inode
        LsfInode* inode() const { return inode_; }
        bool is_root() const { return ino_ == LSF_ROOT_INUM; } // 根目录标识
        // 禁止拷贝（确保缓存条目唯一性）
        LsfDentry(const LsfDentry&) = delete;
        LsfDentry& operator=(const LsfDentry&) = delete;
    
        // 成员访问接口
        const std::string& name() const { return name_; }
        uint64_t ino() const { return ino_; }
        uint64_t parent_ino() const { return parent_; }
        const struct stat& stats() const { return st_; }
        struct stat& stats() {return st_; }
    
        LsfDentry::Ptr parent_dentry() const {
            return d_parent_.lock(); // 返回父目录的智能指针（需在 LsfDentry 中添加 d_parent_ 成员）
        }

        void remove_child(const Ptr& child) {
            std::lock_guard<std::mutex> lock(mutex_);
            d_sib_.remove(child);
        }
        // ----------- 目录树链表管理 -----------
        // 子目录管理
        void add_child(const Ptr& child);
        std::list<Ptr> children() const;
    
        // ----------- 别名链表管理 -----------
        // 添加别名（同一 inode 的不同 dentry 对象）
        void add_alias(const Ptr& alias);
        std::list<Ptr> aliases() const;

        // ----------- 引用计数管理 -----------
        // 引用计数
        void ref_inc() { ref_count_++; }
        void ref_dec() { if (ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) reclaim_callback_(this); }
    
        // ----------- LRU 链表管理 -----------
        // 触发 LRU 更新（需要从缓存管理器中更新位置）
        void touch();

        // ====================================================
        // 为支持 4 个链表，在每个 dentry 中保存其在各外部链表中的位置：
        // 1. lru_pos_：在 LRU 链表中的迭代器位置
        // 2. hash_pos_：在哈希链表中的迭代器位置
        // 3. alias_pos_：在别名链表中的迭代器位置
        // ----------------------------------------------------
        std::list<Ptr>::iterator lru_pos_;    // LRU 链表中的位置
        std::list<Ptr>::iterator hash_pos_;   // 哈希链表中的位置
        std::list<Ptr>::iterator alias_pos_;  // 别名链表中的位置
        
    private:
        std::string name_;
        uint64_t ino_;
        uint64_t parent_;
        struct stat st_;
        
        //std::shared_ptr<LsfInode> inode_;
        LsfInode *inode_ = nullptr;
        std::weak_ptr<LsfDentry> d_parent_;           // 父节点弱引用
        std::list<Ptr> d_sib_;                    // 子目录链表：构建目录树关系
        std::list<Ptr> alias_list_;              // 别名链表：维护指向同一 inode 的多个 dentry

        std::function<void(LsfDentry*)> reclaim_callback_;  
    
        std::atomic<int> ref_count_{0};             // 原子引用计数
        mutable std::mutex mutex_;                  // 节点级锁
};

// ------------------ 缓存管理器（单例模式） -------------------
class DentryCache {
    public:
        static DentryCache& instance() {
            static DentryCache instance;
            return instance;
        }

        std::vector<std::string> split_path(const std::string& path) {
            std::vector<std::string> components;
            if (path.empty()) return components;
        
            // 标准化路径：去除末尾的/
            std::string normalized = path;
            if (normalized != "/" && normalized.back() == '/') {
                normalized.pop_back();
            }
        
            // 特殊处理根目录
            if (normalized == "/") {
                components.push_back(""); // 使用空字符串标记根目录
                return components;
            }
        
            // 使用流式分割
            std::stringstream ss(normalized);
            std::string item;
            
            // 跳过第一个空组件（如果以/开头）
            if (normalized[0] == '/') {
                components.push_back("");
                ss.ignore(1); // 跳过开头的/
            }
        
            while (std::getline(ss, item, '/')) {
                if (!item.empty()) { // 跳过空组件
                    components.push_back(item);
                }
            }
        
            return components;
        }

            // 插入 root_dentry（不参与 LRU 淘汰）
        void insert_root(const LsfDentry::Ptr& root) {
            std::lock_guard<std::mutex> lock(mutex_);
            root_dentry_ = root;
            cache_map_[HashKey{LSF_ROOT_INUM, "/"}] = root; // 父 ino=2, name="/" 作为 key

            ino_map_[LSF_ROOT_INUM] = root;
        }

        // 获取 root_dentry
        LsfDentry::Ptr get_root() const {
            std::lock_guard<std::mutex> lock(mutex_);
            return root_dentry_;
        }

        std::string normalize_path(const std::string& path) {
            if (path.empty() || path == "/") {
                return "/"; // 根目录统一为 "/"
            }
            std::string normalized = path;
            if (!normalized.empty() && normalized.back() == '/') {
                normalized.pop_back();
            }
            return normalized;
        }

        std::string get_parent_path(const std::string& raw_path) {
            // 标准化路径
            std::string norm_path = normalize_path(raw_path);
    
            // 分割路径为组件列表
            std::vector<std::string> components = split_path(norm_path);
    
            // 移除最后一个组件
            if (!components.empty() && !components.back().empty()) {
                components.pop_back();
            }
    
            // 重新构造父路径
            std::string parent_path;
            for (const auto& comp : components) {
                if (comp.empty()) continue; // 跳过根目录的空字符串
                parent_path += "/" + comp;
            }
    
            // 处理根目录和空路径情况
            return parent_path.empty() ? "/" : parent_path;
        }

        LsfDentry::Ptr lookup(const std::string& path); // 根据路径查找 dentry
        // 查找或创建 dentry
        LsfDentry::Ptr lookupHash(uint64_t parent_ino, const std::string& name);
        LsfDentry::Ptr lookupInoHash(uint64_t ino); 
        // 查找 pathDentry  

        LsfDentry::Ptr insertDentry(uint64_t parent_ino, const std::string& name, const struct stat& st);
        void insertInoDentry(uint64_t ino, const LsfDentry::Ptr& entry);
         // 更新接口：更新 dentry 在各链表中的位置（用于内部管理）
        void updateEntryLRU(const LsfDentry::Ptr& entry);
        void updateEntryHash(const LsfDentry::Ptr& entry);   // 对应哈希链表更新
        void updateEntryAlias(const LsfDentry::Ptr& entry);  // 对应别名链表更新
        
        // 触发 LRU 淘汰
        void reclaim();
        void eraseDentry(const LsfDentry::Ptr& entry, bool if_lceq0);
        void eraseInoDentry(uint64_t ino);
    private:
        DentryCache();  // 私有构造函数
        ~DentryCache() = default;
        
        struct HashKey {
            uint64_t parent_ino;
            std::string name;
    
            bool operator==(const HashKey& other) const {
                return parent_ino == other.parent_ino && name == other.name;
            }
        };
    
        struct HashKeyHasher {
            size_t operator()(const HashKey& key) const {
                const size_t prime = 0x9e3779b9;  // 黄金比例素数
                size_t seed = std::hash<uint64_t>()(key.parent_ino);
                seed ^= std::hash<std::string>()(key.name) + prime + (seed << 6) + (seed >> 2);
                return seed;
                /*
                return std::hash<uint64_t>()(key.parent_ino) ^ 
                       std::hash<std::string>()(key.name);
                */
            }
        };

        LsfDentry::Ptr root_dentry_; // 独立于 LRU 的特殊成员

        // 快速查找哈希表（采用 unordered_map 以便高效定位）
        std::unordered_map<HashKey, LsfDentry::Ptr, HashKeyHasher> cache_map_;  
        std::unordered_map<uint64_t, LsfDentry::Ptr> ino_map_;

        // 另外维护一个哈希链表（便于链表操作、遍历、移除）
        std::list<LsfDentry::Ptr> hash_list_;

        // LRU 链表：管理所有 dentry 的使用情况
        std::list<LsfDentry::Ptr> lru_list_;    

        // 别名链表管理：以 inode 号为 key，维护同一 inode 的 dentry 列表
        std::unordered_map<uint64_t, std::list<LsfDentry::Ptr>> alias_map_;

        // 全局锁
        mutable std::mutex mutex_;                                              
        static constexpr size_t MAX_CACHE_ENTRIES = 10000000;
};

/*
// 树状缓存节点结构
typedef struct lsf_dentry {
    char *name;             // 节点名称（相对于父目录，如 "file" 或 "dir"）
    uint64_t ino;         // 当前节点的 inode 号
    uint64_t parent;      // 父目录的 inode 号
    struct stat st;         // 文件属性

    // 树状结构指针
    struct lsf_dentry *d_parent; // 指向父节点
    struct lsf_list_head subdirs;       // 子目录链表头（等同于 Linux 的 d_subdirs）
    struct lsf_list_head sibling_node;     // 兄弟节点链表（等同于 Linux 的 d_child）

    // LRU 链表指针
    struct lsf_dentry *lru_prev;
    struct lsf_dentry *lru_next;
} lsf_dentry_t;

// 全局缓存管理
extern pthread_mutex_t cache_lock;                // 缓存全局锁
extern struct lsf_list_head dentry_hash[];        // 哈希表
extern size_t cache_dentry_count;                 // 当前缓存条目数
extern lsf_dentry_t *lru_head;                    // LRU 链表头
extern lsf_dentry_t *lru_tail;                    // LRU 链表尾

// ------------------ 核心 API -------------------
lsf_dentry_t* dentry_create(const char *name, uint64_t ino, uint64_t parent, const struct stat *st);
void dentry_release(lsf_dentry_t *dentry);
lsf_dentry_t* dentry_lookup(uint64_t parent_ino, const char *name);
void dentry_update_lru(lsf_dentry_t *dentry);
void dentry_reclaim(void);
*/

#endif