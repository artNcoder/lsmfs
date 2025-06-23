#include "lsf_dentry.h"
#include "lsf.h"

// ------------------ LsfDentry 实现 -------------------
LsfDentry::LsfDentry(const std::string& name, uint64_t ino, uint64_t parent, const struct stat& st)
    : name_(name), ino_(ino), parent_(parent), st_(st) {}

void LsfDentry::add_child(const LsfDentry::Ptr& child) {
    std::lock_guard<std::mutex> lock(mutex_);
    child->d_parent_ = shared_from_this();
    d_sib_.push_back(child);
}

std::list<LsfDentry::Ptr> LsfDentry::children() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return d_sib_;  // 返回拷贝以保证线程安全
}

void LsfDentry::add_alias(const Ptr& alias) {
    std::lock_guard<std::mutex> lock(mutex_);
    alias_list_.push_back(alias);
}

std::list<LsfDentry::Ptr> LsfDentry::aliases() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return alias_list_;  // 返回别名链表（副本）
}

void LsfDentry::set_inode(LsfInode* inode) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (inode_)  {
        lsf_dec_inode_ref(inode_);
    }    // 先把旧的释放掉

    inode_ = inode;
    
}

void LsfDentry::touch() {
    // 更新自己的 LRU 状态，将自己移至链表头部
    if (is_root()) {
        LOG_DEBUG("Root dentry touched, but LRU not updated");
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    LOG_DEBUG("touch start");
    DentryCache::instance().updateEntryLRU(shared_from_this());
    LOG_DEBUG("touch end");
   // DentryCache::instance().reclaim();  // 访问时触发 LRU 更新
}

LsfDentry::~LsfDentry() {
    std::lock_guard<std::mutex> lock(mutex_);
    // 先清理所有子项引用
    if(inode_) {
        lsf_dec_inode_ref(inode_); 		
        inode_ = nullptr;
    }
    LOG_INFOF("Dentry destroyed: %s\n", name_.c_str());
}
        
// ------------------ DentryCache 实现 -------------------
DentryCache::DentryCache() = default;

LsfDentry::Ptr DentryCache::lookup(const std::string& path)
{
    LOG_INFO("lookup start");
    std::vector<std::string> components = split_path(path); // 自定义路径分割函数
    // 直接返回根目录（路径为 "/" 或空）
    if (components.empty() || (components.size() == 1 && components[0].empty())) {
        return get_root();
    }
    LsfDentry::Ptr current = get_root();             // 获取根目录 dentry
    //LOG_DEBUGF("Root dentry - ino: %d,  name: %s", current->ino(),current->name().c_str());
    for (const auto& component : components) {
        if (component.empty() || component == ".") continue; // 跳过空或当前目录
        if (component == "..") {
            current = current->parent_dentry();                   // 获取父目录（需在 LsfDentry 中实现 parent()）
            continue;
        }

        bool found = false;
        auto children = current->children();
        for (const auto& child : children) {
            if (child->name() == component) {
                current = child;
                found = true;
                break;
            }
        }

        if (!found) {
            return nullptr; // 路径不存在
        }
    }

    return current;
}

LsfDentry::Ptr DentryCache::lookupHash(uint64_t parent_ino, const std::string& name) {   
    LOG_INFO("lookupHash start");
    //std::lock_guard<std::mutex> lock(mutex_);
    // 组合键：父目录 inode + 文件名
    HashKey key{parent_ino, name};
    // 查找缓存
    auto it = cache_map_.find(key);
    if (it == cache_map_.end()) 
        return nullptr;
        // 命中缓存，更新 LRU
    // updateEntryLRU(it->second);
    return it->second;
    
}

LsfDentry::Ptr DentryCache::lookupInoHash(uint64_t ino)
{
    LOG_INFO("lookupInoHash start");
    //std::lock_guard<std::mutex> lock(mutex_);
    // 查找缓存
    auto it = ino_map_.find(ino);
    if (it == ino_map_.end()) 
        return nullptr;
        // 命中缓存，更新 LRU
     //   updateEntryLRU(it->second);
    return it->second;
}

LsfDentry::Ptr DentryCache::insertDentry(uint64_t parent_ino, const std::string& name, const struct stat& st) {
    LOG_INFO("insertDentry start");
    std::lock_guard<std::mutex> lock(mutex_);
    // 组合键：父目录 inode + 文件名
    HashKey key{parent_ino, name};
    // 创建新条目
    LOG_DEBUGF("cache insert parameters: name = %s, ino = %d, parent_ino = %d",name.c_str(), st.st_ino, parent_ino);
    auto new_dentry = std::make_shared<LsfDentry>(name, st.st_ino, parent_ino, st);
    new_dentry->reclaim_callback_ = [this](LsfDentry* raw) {
        std::lock_guard<std::mutex> lock(mutex_);
        cache_map_.erase({raw->parent_ino(), raw->name()});
        ino_map_.erase(raw->ino());
    };

    // 插入哈希表和 LRU
    cache_map_[key] = new_dentry;
    ino_map_[st.st_ino] = new_dentry;

    // 同时在 hash_list_ 中记录（便于遍历和批量管理）
    // hash_list_.push_back(new_dentry);
    // new_dentry->hash_pos_ = std::prev(hash_list_.end());

    // 插入到 LRU 链表，最新的放在链表头部
    // 根目录不加入 LRU 链表
    if (new_dentry->ino() != LSF_ROOT_INUM) {
        lru_list_.push_front(new_dentry);
        new_dentry->lru_pos_ = lru_list_.begin();
    }

    // 根据 inode 号，在别名映射中记录该 dentry
    // alias_map_[new_dentry->ino()].push_back(new_dentry);
    // new_dentry->alias_pos_ = std::prev(alias_map_[new_dentry->ino()].end());

    // 触发淘汰
    if (cache_map_.size() > MAX_CACHE_ENTRIES) {
        reclaim();
    }

    return new_dentry;
}

void DentryCache::insertInoDentry(uint64_t ino, const LsfDentry::Ptr& entry) {
    LOG_INFO("insertInoDentry start");
    std::lock_guard<std::mutex> lock(mutex_);
    ino_map_[ino] = entry;
}

void DentryCache::updateEntryLRU(const LsfDentry::Ptr& entry) {
    LOG_DEBUG("updateEntryLRU start");
    std::lock_guard<std::mutex> lock(mutex_);
    // 将 entry 移出其在 LRU 链表中的原有位置
    LOG_DEBUG("updateEntryLRU start1");
    lru_list_.splice(lru_list_.begin(), lru_list_, entry->lru_pos_);
    LOG_DEBUG("updateEntryLRU start2");
    entry->lru_pos_ = lru_list_.begin(); // 更新迭代器位置
    LOG_DEBUG("updateEntryLRU end");
   
}

void DentryCache::updateEntryHash(const LsfDentry::Ptr& entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    // 若 dentry 的关键属性发生变化（例如名称或父 inode 改变），需要更新其在 hash_list_ 中的位置
    hash_list_.erase(entry->hash_pos_);
    hash_list_.push_back(entry);
    entry->hash_pos_ = std::prev(hash_list_.end());
}

void DentryCache::updateEntryAlias(const LsfDentry::Ptr& entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    // 若 dentry 的相关属性更新，可能需要重新调整其在同一 inode 别名链表中的位置
    uint64_t ino = entry->ino();
    auto& alias_list = alias_map_[ino];
    alias_list.erase(entry->alias_pos_);
    alias_list.push_back(entry);
    entry->alias_pos_ = std::prev(alias_list.end());
}

void DentryCache::reclaim() {
    LOG_INFO("reclaim start");
    std::lock_guard<std::mutex> lock(mutex_);
    // 按 LRU 策略，淘汰最少使用的 dentry
    while (!lru_list_.empty() && cache_map_.size() > MAX_CACHE_ENTRIES) {
        // 选择 LRU 链表队尾的 dentry（最旧的）
        LsfDentry::Ptr to_reclaim = lru_list_.back();

        // 跳过根目录
        if (to_reclaim->ino() == LSF_ROOT_INUM) {
            lru_list_.pop_back();
            continue;
        }

        lru_list_.pop_back();
        uint64_t ino = to_reclaim->ino();
        // 从 hash_list_ 中移除该 dentry
        // hash_list_.erase(to_reclaim->hash_pos_);
        
        // 从别名链表中移除
        // 
        // auto& alias_list = alias_map_[ino];
        // alias_list.erase(to_reclaim->alias_pos_);
        // if (alias_list.empty()) {
        //     alias_map_.erase(ino);
        // }
        
        // 根据 key 从 cache_map_ 中移除
        HashKey key{ to_reclaim->parent_ino(), to_reclaim->name() };
        cache_map_.erase(key);
        ino_map_.erase(ino);
        LOG_DEBUGF("Reclaimed dentry: %s\n", to_reclaim->name());
        //std::cout << "Reclaimed dentry: " << to_reclaim->name() << std::endl;
    }
}

void DentryCache::eraseDentry(const LsfDentry::Ptr& entry, bool if_lceq0)
{
    LOG_INFO("eraseDentry start");
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!entry) return;

    // 1. 从哈希表 cache_map_ 删除
    HashKey key{entry->parent_ino(), entry->name()};
    cache_map_.erase(key);
    uint64_t ino = entry->ino();
    if (if_lceq0) {
        ino_map_.erase(ino);
    }
    

    // 2. 从哈希链表 hash_list_ 删除
    // hash_list_.erase(entry->hash_pos_);

    // 3. 从 LRU 链表删除
    if (!entry->is_root()) {
        lru_list_.erase(entry->lru_pos_);
    }

    // 4. 处理别名链表
    // auto& alias_list = alias_map_[ino];
    // alias_list.erase(entry->alias_pos_);
    // if (alias_list.empty()) {
    //     alias_map_.erase(ino);
    // }
    LOG_INFOF("Dentry erased: %s (inode: %lu)", entry->name().c_str(), entry->ino());
}


void DentryCache::eraseInoDentry(uint64_t ino)
{
    std::lock_guard<std::mutex> lock(mutex_);
    ino_map_.erase(ino);
}