#define FUSE_USE_VERSION 31

#include <fuse3/fuse_lowlevel.h>
#include <rocksdb/db.h>
#include <cstring>
#include <string>
#include <iostream>
#include <experimental/filesystem>
#include <thread>
#include <chrono>
// #include "lsf.h"
// #include "lsf_i.h"
// #include "lsf_utils.h"
#include <assert.h>
#include <limits.h>
#include "lsf_mkfs.h"
#include <experimental/filesystem>
#include <pthread.h>
#include <uv.h>
#include <atomic>
#include <unordered_map>
#include "lsf_dentry.h"
#include <atomic>
#include "lsf_tp.h"
#include "lsf_profiler.h"
#include <cstdio>


// 全局哈希表
// std::unordered_map<std::string, LsfInode*> inode_map;
namespace fs = std::experimental::filesystem;
using namespace ROCKSDB_NAMESPACE;
using namespace LSF;
static __always_inline Status _lsf_persist_inode(LsfContext *ctx, Transaction *tx, LsfInode *inode);
struct LsfContext *lsf_ctx = nullptr;
static ThreadPool *g_pool = nullptr;

std::string get_filename_from_path(const std::string& path)
{
    size_t last_slash = path.find_last_of("/\\");
    if (last_slash == std::string::npos)
    {
        return path;
    }
    return path.substr(last_slash + 1);
}

std::string format_string(const char *format, ...)
{
    va_list args;
    va_start(args, format);

    // 首先，计算所需的字符串长度
    va_list args_copy;
    va_copy(args_copy, args);
    int length = vsnprintf(nullptr, 0, format, args_copy);
    va_end(args_copy);

    if (length < 0)
    {
        va_end(args);
        return "";
    }

    // 分配足够的空间
    std::vector<char> buf(length + 1);

    // 实际执行格式化
    vsnprintf(buf.data(), buf.size(), format, args);

    va_end(args);

    return std::string(buf.data(), length);
}

static struct LsfInode *deserialize_inode(const char *buf)
{
    struct LsfInode *n = (struct LsfInode *)malloc(sizeof(struct LsfInode));
    if (n == NULL)
    {
        LOG_ERROR("Failed to alloc inode memory");
        return NULL;
    }
    memcpy(n, buf, sizeof(*n));
    n->ref_count = 1;
    return n;
}

static const off_t MAX_FILE_SIZE = (off_t)1 << 40; // 1TiB 上限

static int do_truncate(LsfInode *inode, off_t length, fuse_ino_t ino)
{ 
    PROFILE_FUNC();
    LOG_INFO("do_truncate start");
    LOG_INFOF("do_truncate called: size=%ld", length);

    if (length < 0)
        return -EINVAL;

     // 如果长度超出上限，立刻拒绝
    if ((uint64_t)length > (uint64_t)MAX_FILE_SIZE) {
        LOG_ERRORF("truncate size %ld > fs max %ld, reject EFBIG",
                  length, MAX_FILE_SIZE);
        return -EFBIG;
    }
    // 如果长度没变，直接返回
    if (length == inode->file_size) {
        getDentryByIno(ino)->stats().st_size = inode->file_size;
        LOG_INFO("do_truncate no-op (same size)");
        return 0;
    }

    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->meta_opt);

    //   std::unique_ptr<Transaction> tx_guard(tx);
    ScopedExecutor _1([tx]()
                      { delete tx; });
    ResourceManager _r;
    _r.push_back([tx]()
                 { tx->Rollback(); });

    // PinnableSlice inode_buf;
    // Status s = tx->GetForUpdate(lsf_ctx->read_opt, lsf_ctx->meta_cf, Slice((char *)&inode->inode_num, sizeof(inode->inode_num)), &inode_buf);
    // if (!s.ok())
    // {
    //     LOG_ERRORF("Failed to open file:%ld, for:%s", inode->inode_num, s.ToString().c_str());
    //     return -EIO;
    // }
    const int64_t BS          = inode->block_size;
    const int64_t old_sz      = inode->file_size;
    const int64_t old_blk_cnt = (old_sz + BS - 1) / BS;
    const int64_t new_blk_cnt = (length + BS - 1) / BS;

    if (length < old_sz) {
        // 缩小：删除 [new_blk_cnt, old_blk_cnt) 里的块
        for (int64_t blk = new_blk_cnt; blk < old_blk_cnt; ++blk) {
            block_key key = { {{
                .block_index = (uint64_t)blk,
                .inode_no    = (uint64_t)inode->inode_num
            }} };
            tx->Delete(lsf_ctx->data_cf,
                       Slice((char*)&key, sizeof(key)));
        }
    }

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    inode->mtime = now.tv_sec;
    inode->ctime = now.tv_sec;

    inode->file_size = length;
    Status s = _lsf_persist_inode(lsf_ctx, tx, inode);
    if (!s.ok())
    {
        LOG_ERRORF("Failed to persist inode:%d, for:%s", inode->inode_num, s.ToString().c_str());
        return -EIO;
    }

    s = tx->Commit();
    if (!s.ok())
    {
        LOG_ERRORF("Failed to commit deleteing op, for:%s", s.ToString().c_str());
        return -EIO;
    }
    _r.releaseall();
    getDentryByIno(ino)->stats().st_size = inode->file_size;
    getDentryByIno(ino)->stats().st_mtime = inode->mtime;
    getDentryByIno(ino)->stats().st_ctime = inode->ctime;
    LOG_INFO("do_truncate end");
    return 0;
}

struct LsfInode *getInodeFromParentIno(const char *file_name, uint64_t get_parent_inode_num, struct LsfContext *ctx)
{
    PROFILE_FUNC();
    PinnableSlice inode_num_buf;
    PinnableSlice inode_buf;
    int64_t inode_num;
    Status s;
    LsfInode *inode;

    LOG_DEBUGF("getInodeFromParentIno file_name = %s\n", file_name);
    // uint64_t parent_dir_inode_num = get_parent_inode_num(ctx, path);
    // char *file_name = get_filename_from_path(path);

    LOG_DEBUGF("getIndoe parent_dir_inode_num = %d\n", get_parent_inode_num);
    std::string name_str(file_name); // 转换为 string
    // LOG_DEBUGF("getIndoe filename = %s\n", file_name);
    std::string s1 = format_string("%ld_%s", get_parent_inode_num, file_name);
    // LOG_DEBUGF("getInode s1 = %s\n", s1.c_str());
    Slice file_key = s1;
    // free(file_name);

    s = ctx->db->Get(ctx->read_opt, ctx->meta_cf, file_key, &inode_num_buf);
    if (s.IsNotFound())
    {
        LOG_ERROR("inode not found");
        return NULL;
    }
    else
    {
        memcpy(&inode_num, inode_num_buf.data(), sizeof(inode_num));
    }

    s = ctx->db->Get(ctx->read_opt, ctx->meta_cf, Slice((char *)&inode_num, sizeof(inode_num)), &inode_buf);
    if (s.IsNotFound())
    {
        LOG_ERRORF("Internal error, open file inum:%d failed, inode not exists", inode_num);
        return NULL;
    }
    inode = deserialize_inode(inode_buf.data());
    //       inode->parent_dir_inode_num = parent_dir_inode_num;
    if (inode == NULL)
    {
        LOG_ERROR("inode == NULL");
        lsf_dec_inode_ref(inode);
        return NULL;
    }
    LOG_INFO("getInode success");
    return inode;
}

std::shared_ptr<LsfDentry> getDentry(fuse_ino_t parent, const char *name)
{
    PROFILE_FUNC();
    DentryCache &cache = DentryCache::instance();        
    if (LsfDentry::Ptr res = cache.lookupHash(parent, name))
    {
        LOG_DEBUGF("cache_map_缓存命中: parent= %d, name = %s\n", parent, name);
        return res;
    }

    LOG_DEBUGF("cache_map_缓存未命中: parent= %d, name = %s\n", parent, name);
    // 缓存未命中：查询 RocksDB
    struct stat st = {};
    uint64_t child_ino = 0;
    struct LsfInode *current_inode=getInodeFromParentIno(name, parent, lsf_ctx);
    // 检查查询结果
    if (!current_inode) return nullptr; // 数据库无记录
    LsfDentry::Ptr current = cacheAdd(current_inode, parent, name);    
    return current;
}

std::shared_ptr<LsfDentry> getDentryByIno(fuse_ino_t ino)
{
    PROFILE_FUNC();
    DentryCache &cache = DentryCache::instance();        
    if (LsfDentry::Ptr res = cache.lookupInoHash(ino))
    {
        LOG_DEBUGF("ino_map_缓存命中: ino= %lu\n", ino);
        return res;
    } 
    else
    {
        LOG_DEBUGF("ino_map_缓存未命中: ino= %lu\n", ino);
        return nullptr;
    }
}

void eraseOldDentry(const LsfDentry::Ptr& dentry, bool if_lceq0)
{
    PROFILE_FUNC();
    DentryCache& cache = DentryCache::instance();   
    if (dentry) {
        // 从父目录子节点链表中移除
        // if (auto parent = dentry->parent_dentry()) {
        //     parent->remove_child(dentry); // 需在 LsfDentry 实现链表操作
        // }
        // 执行缓存删除
        //cache.eraseInoDentry(dentry->ino());    
        LOG_INFOF("dentry->inode()->ref_count = %d.", dentry->inode()->ref_count);
        cache.eraseDentry(dentry, if_lceq0);
    }
}

std::shared_ptr<LsfDentry> cacheAdd(struct LsfInode *new_inode, uint64_t parent_ino, const std::string& name)
{
    PROFILE_FUNC();
    LOG_DEBUGF("cacheAdd start: parent_ino=%lu, name=%s, mode=%o, nlink=%hu", parent_ino, name.c_str(),new_inode->mode,new_inode->links_count);
    DentryCache& cache = DentryCache::instance();
    struct stat st = {};
    st.st_ino = new_inode->inode_num;
    st.st_mode = new_inode->mode;
    st.st_nlink = new_inode->links_count;
    st.st_uid = new_inode->uid;
    st.st_gid = new_inode->gid;
    st.st_size = new_inode->file_size;
    st.st_atime = new_inode->atime;
    st.st_mtime = new_inode->mtime;
    st.st_ctime = new_inode->ctime;
    st.st_rdev   = new_inode->rdev;
    lsf_add_inode_ref(new_inode);
    LOG_DEBUGF("cache insertDentry start, ino = %lu", st.st_ino);


    LsfDentry::Ptr new_dentry = cache.insertDentry(parent_ino, name, st);

    new_dentry->set_inode(new_inode);

    //cache.insertInoDentry(new_dentry->ino(), new_dentry);


    // LsfDentry::Ptr parent_dentry = cache.lookupHash(parent_ino, name);
    // parent_dentry->add_child(new_dentry);
    LOG_DEBUG("cacheAdd end");
    return new_dentry;
}

static void lsmfs_init(void *userdata, struct fuse_conn_info *conn)
{
    PROFILE_FUNC();
    profiler::enabled = false;   // 打开统计
    if (profiler::enabled)
    {
        std::string out = std::getenv("PWD") ? std::getenv("PWD") : ".";
        out += "/logs";
        profiler::init(out, 10);       // 10s 刷新一次
    }


    LOG_INFO("lsmfs_init start");

    // 允许更大的写入块(128KB)
    // conn->max_write = 65536;
    conn->max_write = 131072;
    conn->max_readahead = 131072;
    // conn->max_background = 128;
    // conn->congestion_threshold = 64;
    // 启用大块写入
    // // 支持原子 truncate-on-open
    // conn->want |= FUSE_CAP_ATOMIC_O_TRUNC;

    // // 支持 POSIX 锁
    // conn->want |= FUSE_CAP_POSIX_LOCKS;
    conn->want |= FUSE_CAP_SPLICE_WRITE;
    // 检查是否支持大块写入
    if (conn->capable & FUSE_CAP_SPLICE_WRITE)
    {
        LOG_INFOF("Big writes is supported and enabled, max_write = %d", conn->max_write);
    }
    else
    {
        LOG_ERROR("Big writes is not supported by kernel");
    }

    lsf_ctx = LsfContext::getInstance();
    LOG_DEBUGF("ctx db_path = %s ", lsf_ctx->db_path.c_str());
    bool db_exists = fs::exists(lsf_ctx->db_path);

    if (!db_exists)
    {
        lsf_mkfs(lsf_ctx->db_path.c_str());
        init_context(lsf_ctx->db_path.c_str(), lsf_ctx);
    }
    //lsmfs_getDBStats(lsf_ctx->db, lsf_ctx);


    /*
        // 程序启动时，初始化日志文件（清空之前可能存在的内容）
        FILE *log_fp = fopen(log_file_path, "w");
        if (log_fp) {
            fprintf(log_fp, "FUSE write 调用次数日志:\n");
            fclose(log_fp);
        } else {
            fprintf(stderr, "初始化日志文件失败: %s\n", log_file_path);
        }
    */
    struct stat root_st = {};
    root_st.st_ino = LSF_ROOT_INUM;
    root_st.st_mode = S_IFDIR | 00777;
    root_st.st_nlink = 2;      // 硬链接数（. 和 ..）
    root_st.st_uid = getuid(); // 用户 ID
    root_st.st_gid = getgid(); // 组 ID
    root_st.st_size = 4096;    // 典型目录大小
    root_st.st_atime = root_st.st_mtime = root_st.st_ctime = time(NULL);
    // 创建 root_dentry
    auto root = std::make_shared<LsfDentry>("/", LSF_ROOT_INUM, LSF_ROOT_INUM, root_st);
    DentryCache::instance().insert_root(root);
    root->set_inode(&lsf_ctx->root_inode);

    const fuse_ino_t ROOT_INO = LSF_ROOT_INUM;
    if (!g_pool)    {
      size_t n = std::max<size_t>(2, std::thread::hardware_concurrency());
      LOG_INFOF("threadpool n = %d\n", n);
      g_pool = new ThreadPool(n);
    }
    LOG_INFO("lsmfs_init end");
}

static void lsmfs_lookup (fuse_req_t req, fuse_ino_t parent, const char *name)
{
    PROFILE_FUNC();
    fuse_entry_param e = {};
    LOG_INFO("lookup start");
    if (strlen(name) > NAME_MAX)
    {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    LsfDentry::Ptr dentry = getDentry(parent, name);
    if(!dentry)
    {
        LOG_INFO("lookup找不到dentry");
        e.attr_timeout = 60;
        e.entry_timeout = 60;
        e.ino = e.attr.st_ino = 0;
        fuse_reply_entry(req, &e);
        return;
    }
    if (dentry->inode()->links_count == 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    
    e.ino = dentry->ino(); // 使用自定义 ino
    e.attr = dentry->stats();
    e.attr_timeout = 60;
    e.entry_timeout = 60;
    
    fuse_reply_entry(req, &e);
}

static void lsmfs_getattr(fuse_req_t req, fuse_ino_t ino,struct fuse_file_info *fi)
{
    PROFILE_FUNC();
    LOG_INFOF("getattr start, ino = %lu", ino);
    //LOG_INFOF("[STATS] getattr called %d times", lsf_ctx->getattr_count.load());
    LsfDentry::Ptr dentry = getDentryByIno(ino);
    if (!dentry)
    {
        fuse_reply_err(req, ENOENT);
        return;
    }
  
    dentry->stats().st_blocks = (dentry->stats().st_size + 511)/512;
    dentry->stats().st_blksize = 4096;
    dentry->stats().st_nlink = dentry->inode()->links_count;
    dentry->stats().st_ino = dentry->inode()->inode_num;
    dentry->stats().st_mode = dentry->inode()->mode;
    dentry->stats().st_nlink = dentry->inode()->links_count;
    dentry->stats().st_uid = dentry->inode()->uid;
    dentry->stats().st_gid = dentry->inode()->gid;
    dentry->stats().st_size = dentry->inode()->file_size;
    dentry->stats().st_atime = dentry->inode()->atime;
    dentry->stats().st_mtime = dentry->inode()->mtime;
    dentry->stats().st_ctime = dentry->inode()->ctime;
    dentry->stats().st_rdev = dentry->inode()->rdev;
    dentry->stats().st_atim.tv_nsec = dentry->inode()->atime_ns;
    dentry->stats().st_mtim.tv_nsec = dentry->inode()->mtime_ns;

    struct stat st = dentry->stats();
    

    LOG_INFOF("getattr end, st_ino: %d, size = %d, mode: %o, st_nlink: %hu\n", dentry->stats().st_ino, dentry->stats().st_size, dentry->stats().st_mode, dentry->stats().st_nlink);
    fuse_reply_attr(req, &st, 60.0);  // 属性缓存时间  
}

static void lsmfs_forget (fuse_req_t req, fuse_ino_t ino, uint64_t nlookup)
{
    PROFILE_FUNC();
    LOG_INFOF("forget start, ino = %d, nlookup = %d\n", ino, nlookup);
    // DentryCache &cache = DentryCache::instance();        
    // if(LsfDentry::Ptr dentry = getDentryByIno(ino))
    //     eraseOldDentry(dentry, false);
    fuse_reply_none(req);
}

// 自定义目录上下文结构体
struct DirContext
{
    Iterator *it;         // 数据库迭代器
    off_t current_offset; // 当前遍历的偏移量
    std::string current_key; // 保存当前迭代器的键
};

static void lsmfs_opendir(fuse_req_t req, fuse_ino_t ino,struct fuse_file_info *fi)
{
    PROFILE_FUNC();
    LOG_INFOF("opendir start , ino = %lu\n", ino);
    //LsfInode *inode = getInode(path, lsf_ctx);
    LsfDentry::Ptr dentry = getDentryByIno(ino);
    if (!dentry) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    
    //if (!S_ISDIR(inode->mode))
    if (!S_ISDIR(dentry->stats().st_mode))
    {
        LOG_ERRORF("Path is not a directory ino %d, mode: %o", ino, dentry->stats().st_mode);
        fuse_reply_err(req, ENOTDIR);
        return;
    }
    DirContext *dir_ctx = new DirContext;
    dir_ctx->it = lsf_ctx->db->NewIterator(lsf_ctx->read_opt, lsf_ctx->meta_cf);
    dir_ctx->current_key = "";
    dir_ctx->current_offset = 0;
    fi->fh = reinterpret_cast<uint64_t>(dir_ctx); // 保存到文件句柄
    LOG_INFO("opendir end");
    fuse_reply_open(req, fi);
}

static void lsmfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t offset,struct fuse_file_info *fi)
{
    PROFILE_FUNC();
    LOG_INFOF("readdir start for ino %lu, size %zu,offset %ld", ino, size, offset);

    LsfDentry::Ptr dentry = getDentryByIno(ino);
    if (!dentry) {
        LOG_ERROR("readdir dentry enoent");
        fuse_reply_err(req, ENOENT);
        return;
    }
    LOG_DEBUGF("Directory dir_inode mode: %o", dentry->stats().st_mode);

    DirContext *dir_ctx = nullptr;
    if (fi->fh) {
        dir_ctx = reinterpret_cast<DirContext*>(fi->fh);
    } else {
        dir_ctx = new DirContext;
        dir_ctx->it = lsf_ctx->db->NewIterator(lsf_ctx->read_opt, lsf_ctx->meta_cf);
        dir_ctx->current_key = "";
        dir_ctx->current_offset = 0;
        fi->fh = reinterpret_cast<uint64_t>(dir_ctx); // 保存到文件句柄
    }
    

    //DirContext *dir_ctx = reinterpret_cast<DirContext *>(fi->fh);
    if (!dir_ctx) {
        LOG_ERROR("Invalid directory context");
        fuse_reply_err(req, EBADF);
        return;
    }

    std::unique_ptr<char[]> buffer(new char[size]);
    size_t buffer_used = 0;

    if (offset == 0) {
        struct stat st;
        memset(&st, 0, sizeof(st));
        // 添加当前目录
        st.st_ino = ino;
        st.st_mode =  dentry->stats().st_mode;
        st.st_mtime = dentry->stats().st_mtime;

        offset = 1;
        buffer_used += fuse_add_direntry(req, buffer.get(), size, ".", &st, 1);

        st.st_ino = dentry->parent_ino(); 
        LOG_INFOF("dentry ino %d ,parent ino %d", ino, st.st_ino);
        offset = 2;
        buffer_used += fuse_add_direntry(req, buffer.get() + buffer_used, size - buffer_used, "..", &st, 2);
        // 设置起始位置
        std::string dir_prefix = format_string("%lu_", ino);
        dir_ctx->it->Seek(dir_prefix);
        dir_ctx->current_offset = 2;
    } else if (dir_ctx->current_offset < offset) {
        // 使用保存的key直接定位到上次位置
        if (!dir_ctx->current_key.empty()) {
            dir_ctx->it->Seek(dir_ctx->current_key);
            dir_ctx->it->Next(); // 跳过上次读取的最后一项
        }     
    }

    std::string dir_prefix = format_string("%ld_", ino);

    
    // 遍历条目并填充缓冲区 
    while (dir_ctx->it->Valid() && buffer_used < size) {
       
        std::string key = dir_ctx->it->key().ToString();
        //LOG_DEBUGF("entry_inode key = %s, buffer_used = %zu\n", key.c_str(), buffer_used);
        if (key.find(dir_prefix) != 0) break;
        // 从 key 中提取文件名（去掉前缀部分）
        std::string filename = key.substr(dir_prefix.length());
        //if (filename.empty() || key == dir_prefix || filename.c_str()[0] == '\0')
        if (filename.empty() || key == dir_prefix || filename.c_str()[0] == '\0' || static_cast<unsigned char>(filename.c_str()[0]) == 0x01 ||
            ((filename.c_str()[0] >= 0x00 && filename.c_str()[0] <= 0x08) || // 0x00-0x08
             (filename.c_str()[0] >= 0x0B && filename.c_str()[0] <= 0x1F) || // 0x0B-0x1F
             filename.c_str()[0] == 0x7F                                     // DEL 字符
             ))
        {
            dir_ctx->it->Next();
            continue; // 跳过空文件名
        }
        LOG_DEBUGF("filename = %s\n", filename.c_str());
        struct stat dummy_stat = {0};
        size_t minimum_required_space = fuse_add_direntry(NULL, NULL, 0, filename.c_str(), &dummy_stat, 0);
        //LOG_DEBUGF("minimum_required_space = %zu\n", minimum_required_space);
        if (size - buffer_used < minimum_required_space) {
        // 剩余空间不足以容纳下一个目录项，停止写入
            break;
        }
        PinnableSlice inode_num_buf;
        PinnableSlice inode_buf;
        int64_t inode_num;

        LsfDentry::Ptr tmp_dentry = getDentry(ino, filename.c_str());
        if(!tmp_dentry)
        {
            //LOG_ERROR("readdir inode not found");
            fuse_reply_err(req, ENOENT);
            return;
        }

       
        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_mode = tmp_dentry->stats().st_mode;
        st.st_size = tmp_dentry->stats().st_size;
        st.st_mtime = tmp_dentry->stats().st_mtime;
        // 提取文件名（去掉路径前缀）
        st.st_nlink = S_ISDIR(tmp_dentry->stats().st_mode) ? 2 : 1; // 添加链接数
        st.st_uid = tmp_dentry->stats().st_uid;                             // 添加用户ID
        st.st_gid = tmp_dentry->stats().st_gid;                             // 添加组ID
        st.st_ino = tmp_dentry->ino();                            // 添加inode号
                                                          //   filler(buf, filename.c_str(), &st, 0);
                                                          // 返回该目录项，同时向 filler 函数传入下一个偏移量（index+1）

        // 填充目录项
        size_t entry_len = fuse_add_direntry(
            req,
            buffer.get() + buffer_used,
            size - buffer_used,
            filename.c_str(),
            &st,
            dir_ctx->current_offset + 1 // 实际偏移量从 3 开始（1 和 2 已被 . 和 .. 占用）
        ); 
        size_t tmp_res = size - buffer_used;
        //LOG_INFOF("size = %zu, buffer_used = %zu, size - buffer_used = %zu, entry_len = %zu\n", size,buffer_used, tmp_res, entry_len);
        
        if (entry_len == 0) break;
        buffer_used += entry_len;
        //if (buffer_used > size) break;
        dir_ctx->current_key = key; // 保存当前键
        dir_ctx->current_offset++;
        dir_ctx->it->Next();                                                 
    }
    size_t tmp_res = size - buffer_used;
    //LOG_INFOF("size = %zu, buffer_used = %zu, size - buffer_used = %zu\n", size,buffer_used, tmp_res);
    // 返回结果
    LOG_INFO("readdir end");
    if (buffer_used > 0) {
        fuse_reply_buf(req, buffer.get(), buffer_used);
    } else {
        fuse_reply_buf(req, nullptr, 0);  // 目录读取完成
    }
    
}

static void lsmfs_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    PROFILE_FUNC();
    DirContext *dir_ctx = reinterpret_cast<DirContext *>(fi->fh);
    if (dir_ctx) {
        delete dir_ctx->it;  // 释放迭代器
        delete dir_ctx;      // 释放上下文结构体
    }


    LOG_INFO("lsmfs_releasedir end");
    fuse_reply_err(req, 0);
}

static void lsmfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,mode_t mode)
{
    PROFILE_FUNC();

    LOG_INFOF("mkdir start, parent: %d, name %s, mode: %o\n", parent, name, mode);
    if (strlen(name) > NAME_MAX)
    {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    lsf_ctx->mkdir_count++;
    const struct fuse_ctx *ctx = fuse_req_ctx(req);
    PinnableSlice inode_num_buf;
    PinnableSlice inode_buf;
    int64_t inode_num;
    Status s;

    std::string s1 = format_string("%ld_%s", parent, name);
    LOG_DEBUGF("mkdir file_key = %s\n", s1.c_str());
    Slice file_key = s1;
    
    // 开始事务
    lsf_ctx->meta_opt.disableWAL = true;
    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->meta_opt);
    //	std::unique_ptr<Transaction> tx_guard(tx);
    ScopedExecutor _1([tx]()
                      { delete tx; });
    ResourceManager _r;
    _r.push_back([tx]()
                 { tx->Rollback(); });

    
    // 检查目录是否已存在
    s = tx->GetForUpdate(lsf_ctx->read_opt, lsf_ctx->meta_cf, file_key, &inode_num_buf);
    if (s.ok())
    {
        LOG_INFO("getforupdate ok");
        fuse_reply_err(req, EEXIST);
        return;
    }
    
    //  创建新的 inode
    LsfInode *new_inode = lsf_calloc_inode();
    // InodeGuard inode_guard(new_inode);
    LsfInode *parent_inode = getDentryByIno(parent)->inode();
    bool  parent_sgid = (parent_inode->mode & S_ISGID) != 0;
    mode_t perms      = (mode & 0777);
    if (parent_sgid) perms |= S_ISGID;
    new_inode->mode = S_IFDIR | perms;

    new_inode->gid    = parent_sgid
                       ? parent_inode->gid
                       : ctx->gid;


    new_inode->links_count = 2;
   // new_inode->mode = S_IFDIR | (mode & 0755);
    new_inode->inode_num = lsf_ctx->generate_inode_num();
    new_inode->parent_dir_inode_num = parent;
    new_inode->block_size = BLOCK_SIZE;
    new_inode->atime = new_inode->ctime = new_inode->mtime = time(NULL);
    new_inode->uid = ctx->uid;
    //new_inode->gid = getgid();

    //	LOG_DEBUGF("Directory new_inode mode: %o", new_inode->mode);
    // 更新存储
    int64_t i_num = new_inode->inode_num;
    LOG_INFOF("Generated inode number: %lu", i_num);
    Slice inode_num_slice((const char *)&i_num, sizeof(i_num));
    LOG_INFO("mkdir meta_cf put start");
    s = tx->Put(lsf_ctx->meta_cf, file_key, inode_num_slice);
    if (!s.ok())
    {
        LOG_ERRORF("Failed put file inode:%s, for:%s", file_key.data(), s.ToString().c_str());
        lsf_dec_inode_ref(new_inode);
        fuse_reply_err(req, EIO);
        return;
    }
    s = _lsf_persist_inode(lsf_ctx, tx, new_inode);
    if (!s.ok())
    {
        LOG_ERRORF("Failed put inode:%s, for:%s", file_key.data(), s.ToString().c_str());
        lsf_dec_inode_ref(new_inode);
        fuse_reply_err(req, EIO);
        return;
    }
    s = tx->Put(lsf_ctx->meta_cf, INODE_SEED_KEY, Slice((char *)&lsf_ctx->inode_seed, sizeof(lsf_ctx->inode_seed)));
    if (!s.ok())
    {
        LOG_ERRORF("Failed put seed:%s, for:%s", file_key.data(), s.ToString().c_str());
        lsf_dec_inode_ref(new_inode);
        fuse_reply_err(req, EIO);
        return;
    }
    parent_inode->links_count++;
    parent_inode->ctime = time(NULL);
    parent_inode->mtime = time(NULL);
    s = _lsf_persist_inode(lsf_ctx, tx, parent_inode);
    if (!s.ok())
    {
        //free(file_name);
        LOG_ERRORF("Failed to persist inode: %s", s.ToString().c_str());
        fuse_reply_err(req, EIO); // 目标不是目录
        return;
    }

    s = tx->Commit();
    if (!s.ok())
    {
        LOG_ERRORF("Failed create file:%s on committing, for:%s", file_key.data(), s.ToString().c_str());
        lsf_dec_inode_ref(new_inode);
        fuse_reply_err(req, EIO);
        return;
    }
    _r.releaseall();
    
    cacheAdd(new_inode, parent, name);

    getDentryByIno(parent)->stats().st_nlink = parent_inode->links_count;
    getDentryByIno(parent)->stats().st_ctime = parent_inode->ctime;
    getDentryByIno(parent)->stats().st_mtime = parent_inode->mtime;
    //free(file_name);
    //	resource_mgr.releaseall();
    LOG_INFOF("mkdir end");
    struct fuse_entry_param e;
    e.ino = i_num;
    struct stat attr = {}; // 初始化所有成员为 0
    attr.st_ino = new_inode->inode_num;
    attr.st_mode = new_inode->mode;
    attr.st_nlink = new_inode->links_count;
    attr.st_uid = new_inode->uid;
    attr.st_gid = new_inode->gid;
    attr.st_size = 4096;
    attr.st_atime = new_inode->atime;
    attr.st_mtime = new_inode->mtime;
    attr.st_ctime = new_inode->ctime;
    e.attr = attr;  
    
    e.attr_timeout = 60.0;
    e.entry_timeout = 60.0;
    fuse_reply_entry(req, &e);
}

std::mutex inode_seed_mutex;

static void lsmfs_create(fuse_req_t req, fuse_ino_t parent, const char *name,mode_t mode, struct fuse_file_info *fi)
{
    PROFILE_FUNC();
    LOG_DEBUGF("create parent: %d, name %s, mode: %o\n", parent, name, mode);
    const struct fuse_ctx *ctx = fuse_req_ctx(req);
    // 分配并初始化文件句柄
    LsfFileHandle *fh = new LsfFileHandle();
    if (!fh)
    {
        LOG_ERROR("lsmfs_create: Failed to allocate LsfFileHandle");
        delete reinterpret_cast<LsfFileHandle*>(fi->fh);
        fi->fh = 0;
        fuse_reply_err(req, ENOMEM);
        return;
    }

    // 初始化文件句柄
    fh->dirty = 0;
    //fi->direct_io = 1;
    // 赋值给 fi->fh
    fi->fh = reinterpret_cast<uint64_t>(fh);

    LOG_DEBUGF("lsmfs_create: fh assigned with address %p", (void *)fh);

    PinnableSlice inode_num_buf;
    PinnableSlice inode_buf;
    int64_t inode_num;
    Status s;

    std::string s1 = format_string("%ld_%s", parent, name);
    LOG_DEBUGF("create file_key = %s\n", s1.c_str());

    Slice file_key = s1;
    
    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->meta_opt);
    ScopedExecutor _1([tx]()
                      { delete tx; });
    ResourceManager _r;
    _r.push_back([tx]()
                 { tx->Rollback(); });

    // 开始事务
    // 使用 RAII 方式确保事务被正确删除
    // std::unique_ptr<Transaction> tx_guard(tx);

    /* 待对比
    PinnableSlice old_inode;
    s = tx->GetForUpdate(lsf_ctx->read_opt, lsf_ctx->meta_cf, file_key, &old_inode);
    if (s.ok())
    {
        LOG_ERRORF("File already exists, key:%s", file_key.data());
        //	S5LOG_ERROR("File already exists, key:%s", file_key.data());
        return -EEXIST;
    }
    */
    LsfInode *new_inode = lsf_calloc_inode();
    // InodeGuard inode_guard(new_inode);

    new_inode->links_count = 1;
    new_inode->mode = mode;
    new_inode->inode_num = lsf_ctx->generate_inode_num();
    new_inode->parent_dir_inode_num = parent;
    new_inode->block_size = BLOCK_SIZE;
    new_inode->atime = new_inode->ctime = new_inode->mtime = time(NULL);
    new_inode->uid = ctx->uid;
    new_inode->gid = ctx->gid;

    assert(sizeof(struct LsfInode) == LSF_INODE_SIZE);
    //	LOG_DEBUGF("file new_inode mode: %o", new_inode->mode);
    // 更新存储

    int64_t i_num = new_inode->inode_num;
    LOG_INFOF("Generated inode number: %lu", i_num);
    Slice inode_num_slice((const char *)&i_num, sizeof(i_num));
    s = tx->Put(lsf_ctx->meta_cf, file_key, inode_num_slice);
    if (!s.ok())
    {
        LOG_ERRORF("Failed put file inode:%s, for:%s", file_key.data(), s.ToString().c_str());
        delete reinterpret_cast<LsfFileHandle*>(fi->fh);
        fi->fh = 0;
        fuse_reply_err(req, EIO);
        return;
    }
    s = _lsf_persist_inode(lsf_ctx, tx, new_inode);
    if (!s.ok())
    {
        LOG_ERRORF("Failed put inode:%s, for:%s", file_key.data(), s.ToString().c_str());
        delete reinterpret_cast<LsfFileHandle*>(fi->fh);
        fi->fh = 0;
        fuse_reply_err(req, EIO);
        return;
    }

    LsfInode *parent_inode = getDentryByIno(parent)->inode();
    parent_inode->ctime = time(NULL);
    parent_inode->mtime = time(NULL);
    s = _lsf_persist_inode(lsf_ctx, tx, parent_inode);
    if (!s.ok())
    {
        //free(file_name);
        LOG_ERRORF("Failed to persist inode: %s", s.ToString().c_str());
        fuse_reply_err(req, EIO); // 目标不是目录
        return;
    }

    /*
        s = tx->Put(lsf_ctx->meta_cf, INODE_SEED_KEY, Slice((char *)&lsf_ctx->inode_seed, sizeof(lsf_ctx->inode_seed)));
        if (!s.ok())
        {
            LOG_ERRORF("Failed put seed:%s, for:%s", file_key.data(), s.ToString().c_str());
            return -EIO;
        }
    */

    s = tx->Commit();
    if (!s.ok())
    {
        LOG_ERRORF("Failed create file:%s on committing, for:%s", file_key.data(), s.ToString().c_str());
        delete reinterpret_cast<LsfFileHandle*>(fi->fh);
        fi->fh = 0;
        fuse_reply_err(req, EIO);
        return;
    }
    rocksdb::WriteBatch batch;
    batch.Put(lsf_ctx->meta_cf, INODE_SEED_KEY, Slice((char *)&lsf_ctx->inode_seed, sizeof(lsf_ctx->inode_seed)));
    rocksdb::WriteOptions write_options;
    write_options.disableWAL = true; // ✅ 禁用 WAL
    rocksdb::Status ss = lsf_ctx->db->Write(write_options, &batch);
    if (!ss.ok())
    {
        LOG_ERRORF("Failed put seed:%s, for:%s", file_key.data(), ss.ToString().c_str());
        delete reinterpret_cast<LsfFileHandle*>(fi->fh);
        fi->fh = 0;
        fuse_reply_err(req, EIO);
        return;
    }
    _r.releaseall();

    LsfDentry::Ptr new_dentry = cacheAdd(new_inode, parent, name);
    getDentryByIno(parent)->stats().st_ctime = parent_inode->ctime;
    getDentryByIno(parent)->stats().st_mtime = parent_inode->mtime;

    struct fuse_entry_param entry;
    memset(&entry, 0, sizeof(entry));
    entry.ino = new_inode->inode_num;
    struct stat attr = {}; // 初始化所有成员为 0
    attr.st_ino = new_inode->inode_num;
    attr.st_mode = new_inode->mode;
    attr.st_nlink = new_inode->links_count;
    attr.st_uid = new_inode->uid;
    attr.st_gid = new_inode->gid;
    attr.st_size = 0;
    attr.st_atime = new_inode->atime;
    attr.st_mtime = new_inode->mtime;
    attr.st_ctime = new_inode->ctime;
    entry.attr = attr; 

    entry.attr_timeout = 60;
    entry.entry_timeout = 60;

    // fh->dentry = new_dentry;
    // fh->inode = new_inode;
    // 9. 回复请求（同时返回 entry 和 file handle）
    fuse_reply_create(req, &entry, fi);
//    free(new_inode);
    LOG_INFO("lsmfs create end");
}

static void lsmfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,mode_t mode, dev_t rdev)
{
    PROFILE_FUNC();
    LOG_INFOF("mknod start, parent: %lu, name: %s, mode: %o, rdev: %lu",
              parent, name, mode, (unsigned long)rdev);
    if (strlen(name) > NAME_MAX)
    {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    const struct fuse_ctx *ctx = fuse_req_ctx(req);
     // 只支持 FIFO、块/字符设备、socket；普通文件走 create，目录走 mkdir
    if (S_ISREG(mode)) {
        fuse_reply_err(req, ENOSYS);
        return;
    }   
    if (!S_ISFIFO(mode) && !S_ISCHR(mode) && !S_ISBLK(mode) && !S_ISSOCK(mode)) {
        fuse_reply_err(req, EINVAL);
        return;
    }
    // 组装 key = parent_name
    std::string file_key = format_string("%lu_%s", parent, name);

    // 开事务
    lsf_ctx->meta_opt.disableWAL = true;
    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->meta_opt);
    ResourceManager rm;
    rm.push_back([tx]() { tx->Rollback(); });
    ScopedExecutor del_tx([tx]() { delete tx; });

    // 分配并填 inode
    LsfInode *new_inode = lsf_calloc_inode();
    new_inode->inode_num            = lsf_ctx->generate_inode_num();
    new_inode->parent_dir_inode_num = parent;
    new_inode->mode                 = mode;
    new_inode->links_count          = 1;
    new_inode->block_size           = BLOCK_SIZE;
    new_inode->atime = new_inode->mtime = new_inode->ctime = time(NULL);
    new_inode->uid = ctx->uid;
    new_inode->gid = ctx->gid;
    if (S_ISCHR(mode) || S_ISBLK(mode)) {
        new_inode->rdev = rdev;
    }

     // persist: key → inode_num
    int64_t ino = new_inode->inode_num;
    Slice ino_slice((char *)&ino, sizeof(ino));
    Status s = tx->Put(lsf_ctx->meta_cf, Slice(file_key), ino_slice);
    if (!s.ok()) {
        fuse_reply_err(req, EIO);
        return;
    }
    // persist inode 内容
    s = _lsf_persist_inode(lsf_ctx, tx, new_inode);
    if (!s.ok()) {
        fuse_reply_err(req, EIO);
        return;
    }

    LsfInode *parent_inode = getDentryByIno(parent)->inode();
    parent_inode->ctime = time(NULL);
    parent_inode->mtime = time(NULL);
    s = _lsf_persist_inode(lsf_ctx, tx, parent_inode);
    if (!s.ok())
    {
        //free(file_name);
        LOG_ERRORF("Failed to persist inode: %s", s.ToString().c_str());
        fuse_reply_err(req, EIO); // 目标不是目录
        return;
    }

    // 更新全局 seed
    s = tx->Put(lsf_ctx->meta_cf,
                INODE_SEED_KEY,
                Slice((char *)&lsf_ctx->inode_seed,
                      sizeof(lsf_ctx->inode_seed)));
    if (!s.ok()) {
        fuse_reply_err(req, EIO);
        return;
    }
    // 提交
    s = tx->Commit();
    if (!s.ok()) {
        fuse_reply_err(req, EIO);
        return;
    }
    rm.releaseall();

    // 更新缓存（可选）
    cacheAdd(new_inode, parent, name);
    getDentryByIno(parent)->stats().st_ctime = parent_inode->ctime;
    getDentryByIno(parent)->stats().st_mtime = parent_inode->mtime;
    // 构造 entry 直接返回
    struct fuse_entry_param e;
    memset(&e, 0, sizeof(e));
    e.ino = ino;
    e.attr.st_ino   = ino;
    e.attr.st_mode  = new_inode->mode;
    e.attr.st_nlink = new_inode->links_count;
    e.attr.st_uid   = new_inode->uid;
    e.attr.st_gid   = new_inode->gid;
    e.attr.st_size  = 0;
    e.attr.st_atime = new_inode->atime;
    e.attr.st_mtime = new_inode->mtime;
    e.attr.st_ctime = new_inode->ctime;
    e.attr.st_rdev  = new_inode->rdev;
    e.entry_timeout = 60.0;
    e.attr_timeout  = 60.0;

    LOG_INFOF("mknod done, ino=%lu", ino);
    fuse_reply_entry(req, &e);
}

// 判断 ctx 是否对 inode p_inodedir 拥有执行权限
static bool has_search_perm(const fuse_ctx *ctx, LsfInode *p) {
    mode_t m = p->mode;
    if (ctx->uid == 0) 
        return true;  // superuser
    if (ctx->uid == p->uid)
        return (m & S_IXUSR);
    if (ctx->gid == p->gid)
        return (m & S_IXGRP);
    return (m & S_IXOTH);
}

static int do_chmod(const fuse_ctx *ctx, fuse_ino_t ino, mode_t mode) {
    PROFILE_FUNC();
    LOG_INFOF("lsmfs_chmod start ino :%lu", ino);
    LsfInode *inode = getDentryByIno(ino)->inode();
    if (!inode) 
    {
        LOG_INFO("inode不存在");
        return -ENOENT;
    }
    LsfInode *p_inode = getDentryByIno(inode->parent_dir_inode_num)->inode();
    if (!has_search_perm(ctx, p_inode)) {
        LOG_DEBUG("chmod: EACCES because parent dir not searchable");
        return -EACCES;
    }
    // —— 检查通过，才继续下面的 owner／mode 逻辑 ——  
    // if (ctx->uid != 0 && ctx->uid != inode->uid) {
    //     return -EACCES;   // 非 owner 拒绝
    // }
    if (ctx->uid != 0 && ctx->uid != inode->uid) {
        LOG_DEBUGF("EPERM: uid=%d cannot chmod inode owned by %d",
                   ctx->uid, inode->uid);
        mode_t old_special = inode->mode & (S_ISUID|S_ISGID);
        mode_t new_special = mode        & (S_ISUID|S_ISGID);
        mode_t added_bits  = mode & ~inode->mode;

        // 如果它只是“清除”特殊位（内核隐式操作），允许通过
        if (old_special != 0        // 原来真有 SUID 或 SGID
            && new_special == 0     // 传进来的 mode 没有任何特殊位
            && added_bits == 0) {   // 没有增加其它权限位
            // **这里把 mode 重写为：原始权限去掉 SUID/SGID**
            mode = (inode->mode & ~old_special);
            // 然后 fall-through，让下面统一的 chmod 代码把新的 mode 写进去
        } else {
            // 其它一律拒绝
            return -EPERM;
        }
    }

    Status s;
    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->data_opt);
    ScopedExecutor _1([tx]()
                      { delete tx; });
    ResourceManager _r;
    _r.push_back([tx]()
                 { tx->Rollback(); });

    // Get inode for update
    // PinnableSlice inode_buf;
    // s = tx->GetForUpdate(lsf_ctx->read_opt, lsf_ctx->meta_cf,
    //                      Slice((char *)&inode->inode_num, sizeof(inode->inode_num)), &inode_buf);

    // if (!s.ok())
    // {
        
    //     if (s.IsNotFound())
    //     {
    //         LOG_INFO("Inode not found in database");
    //         return -ENOENT;
    //     }
    //     LOG_ERRORF("Failed to get inode:%ld for chmod, error:%s",
    //                inode->inode_num, s.ToString().c_str());
    //     return -EIO;
    // }
    // // Deserialize inode
    // LsfInode *tmp_inode = deserialize_inode(inode_buf.data());
    auto n = std::make_unique<LsfInode>(*inode);
    n->ref_count = 1;
    LsfInode *tmp_inode = n.get();
    if (tmp_inode == NULL)
    {
        free(tmp_inode);
        return -ENOMEM;
    }

    
    // POSIX setgid 清除逻辑
    if (ctx->uid != 0 &&                               // 非特权进程
        (inode->mode & S_IFMT) == S_IFREG &&           // 普通文件
        ctx->gid != inode->gid) {                      // 组不匹配
        mode &= ~S_ISGID;  // 清除 setgid 位
    }
    // ScopedExecutor _2([tmp_inode]()
    //                   { free(tmp_inode); });
    // 保存原始文件类型位
    mode_t file_type = inode->mode & S_IFMT;
    // 仅更新权限位，保持文件类型位不变
    mode_t new_mode = (mode & 07777) | file_type;


    // 确保新模式没有改变文件类型
    if ((new_mode & S_IFMT) != file_type)
    {
        tx->Rollback();
        
        return -EINVAL;
    }

    // Update mode, only keeping the permission bits
    // tmp_inode->mode = (tmp_inode->mode & ~07777) | (mode & 07777);
    tmp_inode->mode = new_mode;
    tmp_inode->ctime = time(NULL);

    LOG_DEBUGF("chmod: original mode: %o, new mode: %o, file type: %o",
               inode->mode, new_mode, file_type);

    // Persist updated inode
    s = _lsf_persist_inode(lsf_ctx, tx, tmp_inode);
    if (!s.ok())
    {
        LOG_ERRORF("Failed to persist inode:%ld for chmod, error:%s",
                   tmp_inode->inode_num, s.ToString().c_str());
        return -EIO;
    }

    // Commit transaction
    s = tx->Commit();
    if (!s.ok())
    {
        LOG_ERRORF("Failed to commit chmod for inode:%ld, error:%s",
                   tmp_inode->inode_num, s.ToString().c_str());
        return -EIO;
    }

    if (auto m_dentry = getDentryByIno(ino)) {
        m_dentry->inode()->mode = tmp_inode->mode;
        m_dentry->inode()->ctime = tmp_inode->ctime;
        m_dentry->stats().st_mode = tmp_inode->mode;
        m_dentry->stats().st_ctime =tmp_inode->ctime;
    }
    _r.releaseall();
    LOG_INFO("lsmfs_chmod end");
    return 0;
}

// 检查 pid 进程的 supplementary groups 中是否包含 gid
static bool is_user_in_group(pid_t pid, gid_t want_gid) {
    std::string path = "/proc/" + std::to_string(pid) + "/status";
    std::ifstream ifs(path);
    if (!ifs.is_open()) return false;
    std::string line;
    while (std::getline(ifs, line)) {
        // 格式形如 "Groups:\t0 4 20 27 30 46 110 113 1000"
        if (line.rfind("Groups:", 0) == 0) {
            std::istringstream iss(line.substr(7));
            gid_t g;
            while (iss >> g) {
                if (g == want_gid) return true;
            }
            break;
        }
    }
    return false;
}

static int do_chown(const fuse_ctx *ctx, fuse_ino_t ino, uid_t uid, gid_t gid) {
    PROFILE_FUNC();
    LOG_DEBUGF("_lsmfs_do_chown start: ino=%lu, uid=%d, gid=%d", ino, uid, gid);

    LsfInode *inode = getDentryByIno(ino)->inode();
    if (!inode) {
        LOG_ERRORF("Inode not found: ino=%lu", ino);
        return -ENOENT;
    }
    LsfInode *p_inode = getDentryByIno(inode->parent_dir_inode_num)->inode();
    if (!p_inode) {
        LOG_ERRORF("Parent Inode not found: ino=%lu", inode->parent_dir_inode_num);
        return -ENOENT;
    }
    mode_t m = p_inode->mode;

         // 先检查——非 super-user 且 不是文件属主，任何 chown/lchown 都拒绝
     if (ctx->uid != 0 && ctx->uid != inode->uid) {
         LOG_DEBUGF("EPERM: %d not owner of ino=%lu", ctx->uid, ino);
         return -EPERM;
     }

    bool ok = false;
            // 属主
    if (ctx->uid == p_inode->uid) {
        ok = (m & S_IXUSR);
    }
            // 属组
    else if (ctx->gid == p_inode->gid ||
        is_user_in_group(ctx->pid, p_inode->gid)) {
        ok = (m & S_IXGRP);
    }
    else {
        ok = (m & S_IXOTH);
    }
    if (!ok) {
        LOG_DEBUGF("EACCES for chown: no exec on parent ino=%lu", inode->parent_dir_inode_num);
        return -EACCES;
    }

    // 新增：只有 root(uid==0) 才能改 owner 或者改 group
    bool want_chown = (uid != (uid_t)-1 && uid != inode->uid);
    bool want_chgrp = (gid != (gid_t)-1 && gid != inode->gid);
    if (want_chown && ctx->uid != 0) {
        LOG_DEBUGF("EPERM: uid=%d not allowed to change owner of inode owned by %d",
                   ctx->uid, inode->uid);
        return -EPERM;
    }

    
    if (want_chgrp && ctx->uid != 0) {
        bool primary_ok   = (ctx->gid == gid);
        bool supplem_ok   = is_user_in_group(ctx->pid, gid);
        bool owner_ok     = (ctx->uid == inode->uid);
        if (!(owner_ok && (primary_ok || supplem_ok))) {
            LOG_DEBUGF("EPERM: uid=%d gid=%d not allowed to change group→%d of file owner=%d current groups primary=%d + supplementary…",
                       ctx->uid, ctx->gid, gid, inode->uid, ctx->gid);
            return -EPERM;
        }
    }

    Status s;
    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->data_opt);

    ScopedExecutor _1([tx]()
                      { delete tx; });
    ResourceManager _r;
    _r.push_back([tx]()
                 { tx->Rollback(); });

    // // Get inode for update
    // PinnableSlice inode_buf;
    // s = tx->GetForUpdate(lsf_ctx->read_opt, lsf_ctx->meta_cf,
    //                      Slice((char *)&inode->inode_num, sizeof(inode->inode_num)), &inode_buf);
    // if (!s.ok())
    // {
    //     if (s.IsNotFound())
    //     {
    //         return -ENOENT;
    //     }
    //     LOG_ERRORF("Failed to get inode:%ld for chown, error:%s",
    //                inode->inode_num, s.ToString().c_str());
    //     return -EIO;
    // }

    // // Deserialize inode
    // LsfInode *tmp_inode = deserialize_inode(inode_buf.data());
    auto n = std::make_unique<LsfInode>(*inode);
    n->ref_count = 1;
    LsfInode *tmp_inode = n.get();
    if (tmp_inode == NULL)
    {
        free(tmp_inode);
        LOG_ERROR("tmp_inode not exist");
        return -ENOMEM;
    }


    // Update ownership
    if (want_chown)
        tmp_inode->uid = uid;
    if (want_chgrp)
        tmp_inode->gid = gid;
    // tmp_inode->uid = uid;
    // tmp_inode->gid = gid;
    tmp_inode->ctime = time(NULL);

    LOG_DEBUGF("chown: inode: %ld, new uid: %d, new gid: %d",
               tmp_inode->inode_num, uid, gid);

    // Persist updated inode
    s = _lsf_persist_inode(lsf_ctx, tx, tmp_inode);
    if (!s.ok())
    {
        LOG_ERRORF("Failed to persist inode:%ld for chown, error:%s",
                   tmp_inode->inode_num, s.ToString().c_str());
        return -EIO;
    }

    // Commit transaction
    s = tx->Commit();
    if (!s.ok())
    {
        LOG_ERRORF("Failed to commit chown for inode:%ld, error:%s",
                   tmp_inode->inode_num, s.ToString().c_str());
        return -EIO;
    }

    if (auto m_dentry = getDentryByIno(ino)) {
        m_dentry->inode()->uid = tmp_inode->uid;
        m_dentry->inode()->gid = tmp_inode->gid;
        m_dentry->inode()->ctime = tmp_inode->ctime;
        m_dentry->stats().st_uid = tmp_inode->uid;
        m_dentry->stats().st_gid = tmp_inode->gid;
        m_dentry->stats().st_ctime = tmp_inode->ctime;
    }


    _r.releaseall();
    LOG_INFO("lsmfs_chown end");
    return 0;
}

static void lsmfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,int to_set, struct fuse_file_info *fi)
{
    PROFILE_FUNC();
    LOG_INFOF("setattr start, ino = %lu\n", (unsigned long)ino);
    LsfInode *inode = getDentryByIno(ino)->inode();
    if (!inode) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    int ret = 0;
    // 获取请求上下文
    const struct fuse_ctx *ctx = fuse_req_ctx(req);


    // 权限检查（示例：检查 CAP_FOWNER）
    // if ((to_set & (FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID)) &&
    //     !lsf_ctx->is_superuser()) {
    //     fuse_reply_err(req, EPERM);
    //     return;
    // }

    // 处理 chmod
    if (to_set & FUSE_SET_ATTR_MODE) {
        ret = do_chmod(ctx, ino, attr->st_mode);
        if (ret != 0) {
            fuse_reply_err(req, -ret);
            return;
        }
    }
    
    // 处理 chown
    if (to_set & (FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID)) {
        uid_t uid = (to_set & FUSE_SET_ATTR_UID) ? attr->st_uid : -1;
        gid_t gid = (to_set & FUSE_SET_ATTR_GID) ? attr->st_gid : -1;
        // 调用封装的 chown 函数
        ret = do_chown(ctx, ino, uid, gid);
        if (ret != 0) {
            fuse_reply_err(req, -ret); // 注意：错误码需取反（如 -EPERM → EPERM）
            return;
        }
    }

    // 更新时间戳（如果请求中包含）
    if (to_set  & (FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME)) {
        if (to_set  & FUSE_SET_ATTR_ATIME) {
            inode->atime = attr->st_atime;
            inode->atime_ns = attr->st_atim.tv_nsec;
        }
        if (to_set  & FUSE_SET_ATTR_MTIME) {
            inode->mtime = attr->st_mtime;
            inode->mtime_ns = attr->st_mtim.tv_nsec;
        }
        // 更新ctime（状态改变时间）
        inode->ctime = time(NULL);
        // 持久化inode数据到存储
        Status s = lsf_ctx->db->Put(lsf_ctx->meta_opt, lsf_ctx->meta_cf,
        Slice((char *)&inode->inode_num, sizeof(inode->inode_num)),
        Slice((char *)inode, LSF_INODE_SIZE));

        if (!s.ok())
        {
            LOG_ERROR("Failed to update inode in storage: " + s.ToString());
            fuse_reply_err(req, EIO); // 注意：错误码需取反（如 -EPERM → EPERM）
            return;
        }
    }

    if (to_set & FUSE_SET_ATTR_SIZE) {
        off_t new_size = attr->st_size;
        ret = do_truncate(inode, new_size, ino);
        if (ret != 0) {
            fuse_reply_err(req, -ret);
            return;
        }
    }

    // 返回更新后的属性
    struct stat new_attr = {};
    new_attr.st_ino = ino;
    new_attr.st_mode = inode->mode;
    new_attr.st_nlink = inode->links_count;
    new_attr.st_uid = inode->uid;
    new_attr.st_gid = inode->gid;
    new_attr.st_size = inode->file_size;
    new_attr.st_atime = inode->atime;
    new_attr.st_mtime = inode->mtime;
    new_attr.st_atim.tv_nsec = inode->atime_ns;
    new_attr.st_mtim.tv_nsec = inode->mtime_ns;

    new_attr.st_ctime = time(NULL); // 修改时间自动更新

    fuse_reply_attr(req, &new_attr, 0.0);
}

// static int lsmfs_utimens(const char *path, const struct timespec ts[2], struct fuse_file_info *fi)
// {
//     //   LOG_INFO("utimens start");

//     LsfInode *inode = getInode(path, lsf_ctx);
//     // InodeGuard inode_guard(inode);
//     if (!inode)
//         return -ENOENT;

//     if (ts)
//     {
//         // 将timespec转换为time_t
//         inode->atime = ts[0].tv_sec;
//         inode->mtime = ts[1].tv_sec;
//     }
//     else
//     {
//         time_t now = time(NULL);
//         inode->atime = now;
//         inode->mtime = now;
//     }

//     // 更新ctime（状态改变时间）
//     inode->ctime = time(NULL);

//     // 持久化inode数据到存储
//     Status s = lsf_ctx->db->Put(lsf_ctx->meta_opt, lsf_ctx->meta_cf,
//                                 Slice((char *)&inode->inode_num, sizeof(inode->inode_num)),
//                                 Slice((char *)inode, LSF_INODE_SIZE));

//     if (!s.ok())
//     {
//         LOG_ERROR("Failed to update inode in storage: " + s.ToString());
//         return -EIO;
//     }

//     //    LOG_INFO("utimens end");
//     return 0;
// }

static void lsmfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    PROFILE_FUNC();
    LOG_INFO("lsmfs open start ");
    LOG_DEBUGF("open ino =%lu", ino);

    //LsfInode *inode = getInode(path, lsf_ctx);
    LsfDentry::Ptr dentry = getDentryByIno(ino);
    if (!dentry) {
        LOG_ERROR("lsmfs_open: inode not found");
        fuse_reply_err(req, ENOENT);
        return;
    }

    // 如果带了 O_TRUNC，就把文件截成 0 长度，更新 inode 的 mtime/ctime
    if (fi->flags & O_TRUNC) {
        // 1) 修改内存里 inode 对象
        LsfInode *inode = dentry->inode();
        time_t now = time(NULL);
        inode->file_size  = 0;
        inode->mtime = now;
        inode->ctime = now;

        // 2) 事务写回 RocksDB
        Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->meta_opt);
        ScopedExecutor _del([tx]{ delete tx; });
        ResourceManager _r;
        _r.push_back([tx]()
                 { tx->Rollback(); });

        Status s = _lsf_persist_inode(lsf_ctx, tx, inode);
        if (!s.ok() || !(s = tx->Commit()).ok()) {
            fuse_reply_err(req, EIO);
            return;
        }
        _r.releaseall();

        // 3) 更新内存 cache 里的 stat 结构，供下一次 getattr 用
        dentry->stats().st_size   = 0;
        dentry->stats().st_mtime  = now;
        dentry->stats().st_ctime  = now;
    }

    if (S_ISDIR(dentry->stats().st_mode))
    {
        fuse_reply_err(req, ENOENT);
        return;
    }

     // 分配文件句柄
     LsfFileHandle *fh = new LsfFileHandle();
     if (!fh)
     {
        LOG_ERROR("lsmfs_open: Failed to allocate LsfFileHandle");
        delete reinterpret_cast<LsfFileHandle*>(fi->fh);
        fi->fh = 0;
        fuse_reply_err(req, ENOMEM);
        return;
     }
    //fi->direct_io = 1;
    //fi->keep_cache = 1;
     // 初始化文件句柄（根据需要）
    // fh->dentry = dentry;
    // fh->inode = dentry->inode();
    // fh->offset = 0;
    fh->dirty = 0;
    //	InodeGuard inode_guard(inode);    
    // 打印 fi->flags 的值
    LOG_DEBUGF("fi->flags: 0x%X (%d)\n", fi->flags, fi->flags);
    // 检查访问权限
    int access_mode = 0;
    switch (fi->flags & O_ACCMODE)
    {
    case O_RDONLY:
        access_mode = R_OK;
        break;
    case O_WRONLY:
        access_mode = W_OK;
        break;
    case O_RDWR:
        access_mode = R_OK | W_OK;
        break;
    default:
        LOG_ERROR("Unknown access mode");
        delete fh;
        fuse_reply_err(req, EINVAL);
        return;
    }

    // 打开文件并存储文件描述符
    // 赋值给 fi->fh
    fi->fh = reinterpret_cast<uint64_t>(fh);

    LOG_DEBUGF("lsmfs_open: fh assigned with address %p\n", (void *)fh);
    fuse_reply_open(req, fi);
    LOG_INFO("lsmfs open end");
}

static void do_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t offset, fuse_file_info *fi)
{
    PROFILE_FUNC();
    int merge_num = 0;
    LsfDentry::Ptr dentry = getDentryByIno(ino);
    if (!dentry) {
        LOG_INFO("in write no dentry");
        fuse_reply_err(req, ENOENT);
        return;
    }

    if (S_ISDIR(dentry->stats().st_mode))
    {
        fuse_reply_err(req, EISDIR);
        return;
    }
   // const int64_t old_size = dentry->inode()->file_size;
    int64_t start_blk = offset / BLOCK_SIZE;
    int64_t end_blk = (offset + size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    void *m_buf = malloc(BLOCK_SIZE + LSF_BLOCK_HEAD_SIZE);

    if (m_buf == NULL)
    {
        LOG_ERROR("Failed malloc memory");
        fuse_reply_err(req, ENOMEM);
        return;
    }

    ScopedExecutor _1([m_buf]()
                      { free(m_buf); });
    struct block_head *head = (struct block_head *)m_buf;
    //   lsf_ctx->data_opt.disableWAL = true;
    rocksdb::WriteOptions write_options;
    write_options.disableWAL = true;
    Transaction *tx = lsf_ctx->db->BeginTransaction(write_options);
    // std::unique_ptr<Transaction> tx_guard(tx);
    ScopedExecutor _2([tx]()
                      { delete tx; });
    ResourceManager _r;
    _r.push_back([tx]()
                 { tx->Rollback(); });
    Status s;
    int64_t buf_offset = 0;
    // 锁定以确保线程安全
    // pthread_mutex_lock(&db_mutex);
    for (int64_t index = start_blk; index < end_blk; index++)
    {
        block_key blk_key = {{{block_index : (uint64_t)index, inode_no : (uint64_t)dentry->ino()}}};
        int64_t start_off = (offset + buf_offset) % BLOCK_SIZE;
        size_t segment_len = std::min(size - buf_offset, (size_t)BLOCK_SIZE - start_off);
        *head = {0};
        memcpy((char *)m_buf + LSF_BLOCK_HEAD_SIZE, buf + buf_offset, segment_len);
        Slice segment_data((char *)m_buf, segment_len + LSF_BLOCK_HEAD_SIZE);
        //if (segment_len != BLOCK_SIZE && (index*BLOCK_SIZE + segment_len) <= old_size)
        if (segment_len != BLOCK_SIZE) 
        {
            head->merge_off = (uint16_t)start_off;
            s = tx->Merge(lsf_ctx->data_cf, Slice((const char *)&blk_key, sizeof(blk_key)), segment_data);
            LOG_DEBUG("Merge done");
        }
        else
        {
            head->data_bmp = (uint16_t)start_off;
            s = tx->Put(lsf_ctx->data_cf, Slice((const char *)&blk_key, sizeof(blk_key)), segment_data);
            LOG_DEBUG("put done");
        }
        if (!s.ok())
        {
            LOG_ERRORF("Failed write on key:%ld_%ld len:%ld, for:%s", blk_key.inode_no, blk_key.block_index, segment_len, s.ToString().c_str());
            fuse_reply_err(req, EIO);
            return;
        }
        buf_offset += segment_len;
    }

    dentry->stats().st_mtime = time(NULL);
    //  dirty = 1;
    LsfFileHandle *fh = reinterpret_cast<LsfFileHandle *>(fi->fh);
    if (!fh)
    {
        LOG_INFO("fh init failed");
        fuse_reply_err(req, EBADF);
        return;
    }
    fh->dirty = 1;

    if (offset + size > dentry->inode()->file_size)
    {
        dentry->inode()->file_size = offset + size;
        dentry->stats().st_size = dentry->inode()->file_size;
        LOG_INFO("write meta_cf put start");
        s = _lsf_persist_inode(lsf_ctx, tx, dentry->inode());
        if (!s.ok())
        {
            LOG_ERRORF("Failed to persist inode, for:%s", s.ToString().c_str());
            fuse_reply_err(req, EIO);
            return;
        }
        fh->dirty = 0;
    }

    s = tx->Commit();
    if (!s.ok())
    {
        LOG_ERRORF("Commit failed, for:%s", s.ToString().c_str());
        fuse_reply_err(req, EIO);
        return;
    }
    // 释放锁
    //  pthread_mutex_unlock(&db_mutex);
    _r.releaseall();
    LOG_DEBUGF("write end: ino=%d size=%zu offset=%ld file_size=%ld buf_offset=%ld\n", ino, size, offset, dentry->inode()->file_size, buf_offset);
    LOG_DEBUGF("lookup() on thread %lu", (unsigned long)pthread_self());
    fuse_reply_write(req, size);
}

static void do_write_wb(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t offset, fuse_file_info *fi)
{
    PROFILE_FUNC();
    int merge_num = 0;
    LsfDentry::Ptr dentry = getDentryByIno(ino);
    if (!dentry) {
        LOG_INFO("in write no dentry");
        fuse_reply_err(req, ENOENT);
        return;
    }

    if (S_ISDIR(dentry->stats().st_mode))
    {
        fuse_reply_err(req, EISDIR);
        return;
    }

    // LsfFileHandle *fh = reinterpret_cast<LsfFileHandle*>(fi->fh);
    // if (!fh) {
    //     LOG_ERROR("Invalid file handle");
    //     fuse_reply_err(req, EBADF);
    //     return;
    // }

    // 使用句柄中的信息进行写入操作
    LsfInode *inode = dentry->inode();
    //LsfDentry::Ptr dentry = fh->dentry;

   // const int64_t old_size = dentry->inode()->file_size;
    int64_t start_blk = offset / BLOCK_SIZE;
    int64_t end_blk = (offset + size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    void *m_buf = malloc(BLOCK_SIZE + LSF_BLOCK_HEAD_SIZE);

    if (m_buf == NULL)
    {
        LOG_ERROR("Failed malloc memory");
        fuse_reply_err(req, ENOMEM);
        return;
    }

    ScopedExecutor _1([m_buf]()
                      { free(m_buf); });
    struct block_head *head = (struct block_head *)m_buf;
    //   lsf_ctx->data_opt.disableWAL = true;
    rocksdb::WriteOptions write_options;
    write_options.disableWAL = true;

    rocksdb::WriteBatch batch;


    Status s;
    int64_t buf_offset = 0;
    // 锁定以确保线程安全
    // pthread_mutex_lock(&db_mutex);
    for (int64_t index = start_blk; index < end_blk; index++)
    {
        block_key blk_key = {{{block_index : (uint64_t)index, inode_no : (uint64_t)dentry->ino()}}};
        int64_t start_off = (offset + buf_offset) % BLOCK_SIZE;
        size_t segment_len = std::min(size - buf_offset, (size_t)BLOCK_SIZE - start_off);
        *head = {0};
        memcpy((char *)m_buf + LSF_BLOCK_HEAD_SIZE, buf + buf_offset, segment_len);
        Slice segment_data((char *)m_buf, segment_len + LSF_BLOCK_HEAD_SIZE);
        //if (segment_len != BLOCK_SIZE && (index*BLOCK_SIZE + segment_len) <= old_size)
        if (segment_len != BLOCK_SIZE) 
        {
            head->merge_off = (uint16_t)start_off;
            batch.Merge(lsf_ctx->data_cf, Slice((const char *)&blk_key, sizeof(blk_key)), segment_data);
            LOG_DEBUG("Merge done");
        }
        else
        {
            head->data_bmp = (uint16_t)start_off;
            batch.Put(lsf_ctx->data_cf, Slice((const char *)&blk_key, sizeof(blk_key)), segment_data);
            LOG_DEBUG("put done");
        }
        if (!s.ok())
        {
            LOG_ERRORF("Failed write on key:%ld_%ld len:%ld, for:%s", blk_key.inode_no, blk_key.block_index, segment_len, s.ToString().c_str());
            fuse_reply_err(req, EIO);
            return;
        }
        buf_offset += segment_len;
    }

    dentry->stats().st_mtime = time(NULL);
    //  dirty = 1;
    LsfFileHandle *fh = reinterpret_cast<LsfFileHandle *>(fi->fh);
    if (!fh)
    {
        LOG_INFO("fh init failed");
        fuse_reply_err(req, EBADF);
        return;
    }
    fh->dirty = 1;

    if (offset + size > dentry->inode()->file_size)
    {
        dentry->inode()->file_size = offset + size;
        dentry->stats().st_size = dentry->inode()->file_size;
        LOG_INFO("write meta_cf put start");
        Transaction *tx = lsf_ctx->db->BeginTransaction(write_options);
        s = _lsf_persist_inode(lsf_ctx, tx, dentry->inode());
        if (!s.ok())
        {
            LOG_ERRORF("Failed to persist inode, for:%s", s.ToString().c_str());
            fuse_reply_err(req, EIO);
            return;
        }
        s = tx->Commit();
        delete tx;  // 不管成功与否，都要 delete 掉
        if (!s.ok()) {
            LOG_ERRORF("事务提交失败: %s", s.ToString().c_str());
            fuse_reply_err(req, EIO);
            return;
        }

        fh->dirty = 0;
    }

    s = lsf_ctx->db->Write(write_options, &batch);
    if (!s.ok())
    {
        LOG_ERRORF("rocksdb WriteBatch failed:%s", s.ToString().c_str());
        fuse_reply_err(req, EIO);
        return;
    }
    // 释放锁
    //  pthread_mutex_unlock(&db_mutex);

    LOG_DEBUGF("write end: ino=%d size=%zu offset=%ld file_size=%ld buf_offset=%ld\n", ino, size, offset, dentry->inode()->file_size, buf_offset);
    LOG_DEBUGF("lookup() on thread %lu", (unsigned long)pthread_self());
    fuse_reply_write(req, size);
}


static int do_write_body(fuse_ino_t ino, const char *buf, size_t size, off_t offset, LsfFileHandle *fh)
{
    int merge_num = 0;
    LsfDentry::Ptr dentry = getDentryByIno(ino);
    if (!dentry) {
        LOG_INFO("in write no dentry");
        return -EISDIR;
    }

    if (S_ISDIR(dentry->stats().st_mode))
    {
        return -EISDIR;
    }

    int64_t start_blk = offset / BLOCK_SIZE;
    int64_t end_blk = (offset + size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    void *m_buf = malloc(BLOCK_SIZE + LSF_BLOCK_HEAD_SIZE);

    if (m_buf == NULL)
    {
        LOG_ERROR("Failed malloc memory");
        return -ENOMEM;
    }

    ScopedExecutor _1([m_buf]()
                      { free(m_buf); });
    struct block_head *head = (struct block_head *)m_buf;
    //   lsf_ctx->data_opt.disableWAL = true;
    rocksdb::WriteOptions write_options;
    write_options.disableWAL = true;
    Transaction *tx = lsf_ctx->db->BeginTransaction(write_options);
    // std::unique_ptr<Transaction> tx_guard(tx);
    ScopedExecutor _2([tx]()
                      { delete tx; });
    ResourceManager _r;
    _r.push_back([tx]()
                 { tx->Rollback(); });
    Status s;
    int64_t buf_offset = 0;
    // 锁定以确保线程安全
    // pthread_mutex_lock(&db_mutex);
    for (int64_t index = start_blk; index < end_blk; index++)
    {
        block_key blk_key = {{{block_index : (uint64_t)index, inode_no : (uint64_t)dentry->ino()}}};
        int64_t start_off = (offset + buf_offset) % BLOCK_SIZE;
        size_t segment_len = std::min(size - buf_offset, (size_t)BLOCK_SIZE - start_off);
        *head = {0};
        memcpy((char *)m_buf + LSF_BLOCK_HEAD_SIZE, buf + buf_offset, segment_len);
        Slice segment_data((char *)m_buf, segment_len + LSF_BLOCK_HEAD_SIZE);
        if (segment_len != BLOCK_SIZE)
        {
            head->merge_off = (uint16_t)start_off;
            s = tx->Merge(lsf_ctx->data_cf, Slice((const char *)&blk_key, sizeof(blk_key)), segment_data);
            LOG_DEBUG("Merge done");
        }
        else
        {
            head->data_bmp = (uint64_t)start_off;
            //head->data_bmp = (uint16_t)start_off;
            s = tx->Put(lsf_ctx->data_cf, Slice((const char *)&blk_key, sizeof(blk_key)), segment_data);
            LOG_DEBUG("put done");
        }
        if (!s.ok())
        {
            LOG_ERRORF("Failed write on key:%ld_%ld len:%ld, for:%s", blk_key.inode_no, blk_key.block_index, segment_len, s.ToString().c_str());
            return -EIO;
        }
        buf_offset += segment_len;
    }

    dentry->stats().st_mtime = time(NULL);
    //  dirty = 1;
    //LsfFileHandle *fh = reinterpret_cast<LsfFileHandle *>(fi->fh);
    if (!fh)
    {
        LOG_INFO("fh init failed");
        return -EBADF;
    }
    fh->dirty = 1;

    if (offset + size > dentry->inode()->file_size)
    {
        dentry->inode()->file_size = offset + size;
        dentry->stats().st_size = dentry->inode()->file_size;
        LOG_INFO("write meta_cf put start");
        s = _lsf_persist_inode(lsf_ctx, tx, dentry->inode());
        if (!s.ok())
        {
            LOG_ERRORF("Failed to persist inode, for:%s", s.ToString().c_str()); 
            return -EIO;
        }
        fh->dirty = 0;
    }

    s = tx->Commit();
    if (!s.ok())
    {
        LOG_ERRORF("Commit failed, for:%s", s.ToString().c_str());
        return -EIO;
    }
    // 释放锁
    //  pthread_mutex_unlock(&db_mutex);
    _r.releaseall();
    LOG_DEBUGF("write end: ino=%d size=%zu offset=%ld file_size=%ld buf_offset=%ld\n", ino, size, offset, dentry->inode()->file_size, buf_offset);
    
    return (int)size;
}

static void lsmfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t offset, fuse_file_info *fi)
{
    PROFILE_FUNC();
    LOG_INFOF("lsmfs write start, write ino = %d, write offset = %lld\n", ino, (long long)offset);
    // fuse_reply_write(req, size);
    // // 先把所有需要的内容拷贝出来
    // auto data = std::make_shared<std::vector<char>>(buf, buf + size);
    // LsfFileHandle *fh   = reinterpret_cast<LsfFileHandle*>(fi->fh);

    // g_pool->enqueue([req, ino, data, size, offset, fh]() {
    //     int ret = do_write_body(ino, data->data(), size, offset, fh);
    //     if (ret < 0) {
    //         // 记录错误码，后续 flush/release 会报告
    //         fh->error.store(ret, std::memory_order_relaxed);
    //     }
    // });
    do_write(req, ino, buf, size, offset, fi);
    //do_write_wb(req, ino, buf, size, offset, fi);
}

static void lsmfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t offset, struct fuse_file_info *fi)
{
    PROFILE_FUNC();
    LOG_INFOF("lsmfs read start ino =%lu, size=%zu, offset=%ld", ino, size, offset);
    LsfDentry::Ptr dentry = getDentryByIno(ino);
    if (!dentry) {
        LOG_ERROR("Dentry not found");
        fuse_reply_err(req, ENOENT); // 错误回复
        return;
    }

    if (S_ISDIR(dentry->stats().st_mode))
    {
        fuse_reply_err(req, EISDIR);
        return;
    }
    // LsfFileHandle *fh = reinterpret_cast<LsfFileHandle*>(fi->fh);
    // if (!fh) {
    //     LOG_ERROR("Invalid file handle");
    //     fuse_reply_err(req, EBADF);
    //     return;
    // }

    // 使用句柄中的信息进行读取操作
    LsfInode *inode = dentry->inode();
    //LsfDentry::Ptr dentry = fh->dentry;

    //	LsfFile *f = (LsfFile *)fi->fh;
    if (offset >= dentry->inode()->file_size) {
        LOG_INFOF("lsmfs_read eof: offset=%ld, file_size=%zu, return 0",
              offset, dentry->inode()->file_size);
        fuse_reply_buf(req, NULL, 0);
        return;
    }
    
    if (offset + size > dentry->inode()->file_size)
        size = dentry->inode()->file_size - offset;

    char *buf = (char *)malloc(size); // 动态分配内存
    if (!buf) {
        LOG_ERROR("Memory allocation failed");
        fuse_reply_err(req, ENOMEM);
        return;
    }

    int64_t start_blk = offset / dentry->inode()->block_size;
    int64_t end_blk = (offset + size + dentry->inode()->block_size - 1) / dentry->inode()->block_size;
    int64_t mtest = dentry->inode()->block_size;
    LOG_INFOF("offset=%ld,size=%zu,BLOCK_SIZE=%d,mtest=%zu", offset, size, dentry->inode()->block_size,mtest);
    int64_t buf_offset = 0;

    for (int64_t index = start_blk; index < end_blk; index++)
    {

        block_key blk_key = {{{block_index : (uint64_t)index, inode_no : (uint64_t)dentry->ino()}}};
        int64_t start_off = (offset + buf_offset) % dentry->inode()->block_size; // offset in extent
        size_t segment_len = std::min(size - buf_offset, (size_t)dentry->inode()->block_size - start_off);
        LOG_DEBUGF("start_blk = %d,end_blk = %d, buf_offset = %d, segment_len = %zu", start_blk,end_blk, buf_offset,segment_len);
        Status s;
        PinnableSlice segment_data;

        // TODO: Is it better to use MultiGet or Iterator?
        s = lsf_ctx->db->Get(lsf_ctx->read_opt, lsf_ctx->data_cf, Slice((const char *)&blk_key, sizeof(blk_key)), &segment_data);
        if (!s.ok())
        {
            LOG_ERRORF("Failed read on key:%s len:%ld, for:%s", blk_key.to_string(), segment_len, s.ToString().c_str());
            free(buf);
            fuse_reply_err(req, EIO);
            return;
        }
        if (segment_data.size() > LSF_BLOCK_HEAD_SIZE) {
            const char* block_payload = segment_data.data() + LSF_BLOCK_HEAD_SIZE;
            memcpy(buf + buf_offset, block_payload + start_off, segment_len);
        }
        // if (segment_data.size() > 0)
        //     memcpy(buf + buf_offset, segment_data.data() + LSF_BLOCK_HEAD_SIZE, segment_len);
        else
            memset(buf + buf_offset, 0, segment_len);
        buf_offset += segment_len;
    }
    /*
        if (!f->noatime)
        {
            f->inode->atime = time(NULL);
            f->dirty = 1;
        }
    */

    fuse_reply_buf(req, buf, buf_offset);
    free(buf); // 修复内存泄漏
    LOG_INFOF("lsmfs read end, return size = %ld", buf_offset);
}

static void lsmfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    PROFILE_FUNC();
    LOG_DEBUGF("To unlink file:%ld_%s ", parent, name);
    if (strlen(name) > NAME_MAX)
    {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    struct LsfContext *lsf_ctx = LsfContext::getInstance();
    //	struct LsfInode *inode;
    LsfDentry::Ptr dentry = getDentry(parent, name);
    if (!dentry) {
        LOG_DEBUGF("Dentry not found for parent %lu, name %s", parent, name);
        fuse_reply_err(req, ENOENT);
        return;
    }
    
    int64_t inode_no = dentry->ino();
    if (inode_no < 0)
    {
        fuse_reply_err(req, ENOENT);
        return;
    }
    LsfInode *inode_ptr = dentry->inode();
    
    ScopedExecutor _0([inode_ptr]() { lsf_dec_inode_ref(inode_ptr); });
    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->meta_opt);
    std::string s1 = format_string("%lu_%s", parent, name);
    Slice file_key = s1;
    tx->Delete(lsf_ctx->meta_cf, file_key);

    ScopedExecutor _1([tx]() { delete tx; });
    ResourceManager _r;
    _r.push_back([tx]() { tx->Rollback(); });
    // bool if_lceq0 = false;
    // if(--inode_ptr->links_count==0) 
    // {
    //     LOG_INFO("links_count == 0 true");
        //free(file_name);
        // int64_t blk_cnt = (dentry->inode()->file_size + dentry->inode()->block_size - 1) / dentry->inode()->block_size;
        // for (int64_t i = 0; i < blk_cnt; i++)
        // {
        //     block_key blk_k = {{{block_index : (__le64)i, inode_no : (__le64)dentry->ino()}}};
        //     tx->Delete(lsf_ctx->data_cf, Slice((const char *)&blk_k, sizeof(blk_k)));
        // }
        

        // 删除 inode 序列化元数据
    //     Slice ino_key((char*)&inode_no, sizeof(inode_no));
    //     tx->Delete(lsf_ctx->meta_cf, Slice((char*)&inode_no, sizeof(inode_no)));

    //     // 如果是 symlink，还要删 target
    //     if (S_ISLNK(dentry->inode()->mode)) {
    //         std::string sym_key;
    //         sym_key.reserve(1 + sizeof(inode_no));
    //         sym_key.push_back('L');
    //         sym_key.append(reinterpret_cast<char*>(&inode_no), sizeof(inode_no));
    //         tx->Delete(lsf_ctx->meta_cf, Slice(sym_key.data(), sym_key.size()));
    //     }
    //     // if_lceq0 = true;
    // } else {
    //    LOG_INFO("links_count == 0 false");
        inode_ptr->links_count--;
        inode_ptr->ctime=time(NULL);
        Status s = _lsf_persist_inode(lsf_ctx, tx, inode_ptr);
        if (!s.ok())
        {
            LOG_ERRORF("Failed to persist inode, for:%s", s.ToString().c_str());
            fuse_reply_err(req, EIO);
            return;
        }
        dentry->stats().st_ctime = inode_ptr->ctime;
        //if_lceq0 = false;
    // }

    LsfInode *p_inode = getDentryByIno(parent)->inode();
    p_inode->ctime = time(NULL);
    p_inode->mtime = time(NULL);
    s = _lsf_persist_inode(lsf_ctx, tx, p_inode);
    if (!s.ok())
    {
        //free(file_name);
        LOG_ERRORF("Failed to persist inode: %s", s.ToString().c_str());
        fuse_reply_err(req, EIO); // 目标不是目录
        return;
    }
    getDentryByIno(parent)->stats().st_mtime = p_inode->mtime;
    getDentryByIno(parent)->stats().st_ctime = p_inode->ctime;

    s = tx->Commit();
    if (!s.ok())
    {
        LOG_ERRORF("Failed unlink file:%ld_%s on committing, for:%s", parent, name, s.ToString().c_str());
        fuse_reply_err(req, EIO);
        return;
    }
   // delete inode;
    _r.releaseall();

    eraseOldDentry(dentry, false);
    LOG_INFO("lsmfs unlink end ");
    fuse_reply_err(req, 0); // 成功响应
}

bool is_empty_directory(const LsfDentry::Ptr &dentry) {
    std::string dir_prefix = format_string("%ld_", dentry->ino());

    // 创建数据库迭代器（假设 lsf_ctx->db 提供 NewIterator() 接口）
    auto* it = lsf_ctx->db->NewIterator(lsf_ctx->read_opt, lsf_ctx->meta_cf);
    if (it == nullptr) {
        // 如果迭代器创建失败，可以认为目录不是空的或返回错误，根据实际情况处理
        return false;
    }

    // 从前缀位置开始扫描
    it->Seek(dir_prefix);
    while (it->Valid() && it->key().starts_with(dir_prefix)) {
        // 获取 key 对应的文件名部分
        std::string key = it->key().ToString();
        std::string filename = key.substr(dir_prefix.length());
        // 排除掉特殊条目 "." 和 ".."
        if (!filename.empty()) {
            delete it; // 释放迭代器资源
            return false;  // 找到非特殊的子项，说明目录不为空
        }
        it->Next();
    }
    if (!it->status().ok()) {
        // 底层 I/O 或 compaction 出了问题，为了安全也当成“非空”
        return false;
    }
    delete it;
    return true;  // 遍历完没有找到非 "."、".." 的条目，说明目录为空
}

static void lsmfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    PROFILE_FUNC();
    LOG_INFOF("lsmfs rmdir start for parent ino %lu, name %s\n", parent, name);
    if (strlen(name) > NAME_MAX)
    {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    LsfDentry::Ptr dentry = getDentry(parent, name);   
    if (!dentry)
    {
        fuse_reply_err(req, ENOENT);
        return;
    }

    // 如果目标不是一个目录，返回错误 ENOTDIR
    if (!S_ISDIR(dentry->stats().st_mode)) {
        LOG_ERRORF("Target %s is not a directory", name);
        fuse_reply_err(req, ENOTDIR);
        return;
    }

    // 若目录不为空，则不能删除，返回 ENOTEMPTY
    if (!is_empty_directory(dentry)) {
        LOG_ERRORF("Directory %s is not empty", name);
        fuse_reply_err(req, ENOTEMPTY);
        return;
    }

    int64_t inode_no = dentry->ino();
    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->meta_opt);
    ScopedExecutor _1([tx]() { delete tx; });
    ResourceManager _r;
    _r.push_back([tx]() { tx->Rollback(); });

    std::string s1 = format_string("%ld_%s", parent, name);
    Slice file_key = s1;
    if (!S_ISDIR(dentry->stats().st_mode)) {
        if (dentry->inode()->block_size > 0) {
            int64_t blk_cnt = (dentry->inode()->file_size + dentry->inode()->block_size - 1) / dentry->inode()->block_size;
            for (int64_t i = 0; i < blk_cnt; i++)
            {
                block_key blk_k = {{{block_index : (__le64)i, inode_no : (__le64)dentry->ino()}}};
                tx->Delete(lsf_ctx->data_cf, Slice((const char *)&blk_k, sizeof(blk_k)));
            }
        }
    }
    tx->Delete(lsf_ctx->meta_cf, file_key);

    LsfInode *p_inode = getDentryByIno(parent)->inode();
    p_inode->links_count--;
    p_inode->ctime = time(NULL);
    p_inode->mtime = time(NULL);

    // 持久化父 inode
    Status s = _lsf_persist_inode(lsf_ctx, tx, p_inode);
    if (!s.ok()) {

        delete tx;
        fuse_reply_err(req, EIO);
        return;
    }
    getDentryByIno(parent)->stats().st_nlink = p_inode->links_count;
    getDentryByIno(parent)->stats().st_mtime = getDentryByIno(parent)->stats().st_ctime = p_inode->mtime;

    s = tx->Commit();
    if (!s.ok())
    {
        LOG_ERRORF("Failed rmdir:%ld_%s on committing, for:%s", parent, name, s.ToString().c_str());
        fuse_reply_err(req, EIO);
        return;
    }



    _r.releaseall();

    eraseOldDentry(dentry, true);

    LOG_INFO("lsmfs rmdir end ");
    
    fuse_reply_err(req, 0); // 成功返回 0
}

// 释放与单个打开文件相关的资源
static void lsmfs_release(fuse_req_t req, fuse_ino_t ino,struct fuse_file_info *fi)
{
    PROFILE_FUNC();
    LOG_DEBUG("release start");

    // 清理自定义句柄
    lsf_ctx->release_count++;

    //        my_fsync(path, fi, lsf_ctx, inode);
    LsfFileHandle *fh = reinterpret_cast<LsfFileHandle *>(fi->fh);
    if (!fh)
    {
        fuse_reply_err(req, EBADF);
        return;
    }
    LsfDentry::Ptr dentry = getDentryByIno(ino);
    if (!dentry) {
        LOG_ERRORF("dentry not found: ino=%lu", ino);
        fuse_reply_err(req, ENOENT); // 错误回复
        return;
    }
    LsfInode *inode = dentry->inode();
    int64_t inode_no = dentry->ino();
    int err = fh ? fh->error.load(std::memory_order_relaxed) : 0;

    bool do_persist = fh->dirty;
    bool do_delete = (inode->links_count == 0);
    if (!do_persist && !do_delete) {
        fuse_reply_err(req, err < 0 ? -err : 0);
        return;
    }
    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->data_opt);

    ScopedExecutor _1([tx]()
                          { delete tx; });
        ResourceManager _r;
        _r.push_back([tx]()
                     { tx->Rollback(); });


    if (fh->dirty)
    {

       
        Status s = _lsf_persist_inode(lsf_ctx, tx, inode);
        if (!s.ok())
        {
            LOG_ERRORF("Failed to persist inode, for:%s", s.ToString().c_str());
            fuse_reply_err(req, EIO);
            return;
        }


        fh->dirty = 0;
    }
    //delete fh;
    
    fi->fh = 0;
    if (err < 0) {
        fuse_reply_err(req, -err);
    }

    if (do_delete) {
        LOG_INFO("links_count == 0 true");
        int64_t blk_cnt = (dentry->inode()->file_size + dentry->inode()->block_size - 1) / dentry->inode()->block_size;
        for (int64_t i = 0; i < blk_cnt; i++)
        {
            block_key blk_k = {{{block_index : (__le64)i, inode_no : (__le64)dentry->ino()}}};
            tx->Delete(lsf_ctx->data_cf, Slice((const char *)&blk_k, sizeof(blk_k)));
        }
        

        //删除 inode 序列化元数据
        Slice ino_key((char*)&inode_no, sizeof(inode_no));
        tx->Delete(lsf_ctx->meta_cf, Slice((char*)&inode_no, sizeof(inode_no)));

        // 如果是 symlink，还要删 target
        if (S_ISLNK(dentry->inode()->mode)) {
            std::string sym_key;
            sym_key.reserve(1 + sizeof(inode_no));
            sym_key.push_back('L');
            sym_key.append(reinterpret_cast<char*>(&inode_no), sizeof(inode_no));
            tx->Delete(lsf_ctx->meta_cf, Slice(sym_key.data(), sym_key.size()));
        }
    }
    Status s = tx->Commit();
    if (!s.ok())
    {
        LOG_ERRORF("Commit failed in fsync for:%s", s.ToString().c_str());
        fuse_reply_err(req, EIO);
        return;
    } else {
        _r.releaseall();
        if (do_delete) 
        {
            DentryCache& cache = DentryCache::instance();  
            cache.eraseInoDentry(ino);
        }
    }
    LOG_DEBUG("release end");
    fuse_reply_err(req, 0);
    //	LsfFile *f = (LsfFile *)fi->fh;
    //	lsf_fsync(lsf_ctx, f);
    //	delete f;
    // LOG_INFOF("[STATS] getattr called %d times", lsf_ctx->getattr_count.load());
    // LOG_INFOF("[STATS] mkdir called %d times", lsf_ctx->mkdir_count.load());
    // LOG_INFOF("[STATS] create called %d times", lsf_ctx->create_count.load());
    // LOG_INFOF("[STATS] write called %d times", lsf_ctx->write_count.load());
    // LOG_INFOF("[STATS] release called %d times", lsf_ctx->release_count.load());
}

// 在文件系统卸载时清理整个文件系统的资源
static void lsmfs_destroy(void *private_data)
{
    PROFILE_FUNC();
    LOG_INFO("lsmfs_destroy start");

    if (lsf_ctx->db)
    {
        delete lsf_ctx->db;
    }
    free_context(lsf_ctx);
    if (g_pool) {
      delete g_pool;
      g_pool = nullptr;
    }

    if (profiler::enabled)
        profiler::shutdown();
    

    LOG_INFO("lsmfs_destroy end");
}

static void lsmfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi)
{
    PROFILE_FUNC();
    LOG_INFO("lsmfs_fsync start");
    LOG_INFO("my_fsync start");
    //  struct LsfInode *inode;
    LsfInode *inode = getDentryByIno(ino)->inode();
    if (!inode) {
        LOG_ERRORF("Inode not found: ino=%lu", ino);
        fuse_reply_err(req, ENOENT); // 错误回复
        return;
    }

    LsfFileHandle *fh = reinterpret_cast<LsfFileHandle *>(fi->fh);
    if (!fh)
    {
        fuse_reply_err(req, EBADF);
        return;
    }
    Status s;
    LOG_DEBUGF("Sync file ino:%ld", ino);
    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->data_opt);

    ScopedExecutor _1([tx]()
                      { delete tx; });
    ResourceManager _r;
    _r.push_back([tx]()
                 { tx->Rollback(); });

    if (fh->dirty)
    {
        s = _lsf_persist_inode(lsf_ctx, tx, inode);
        if (!s.ok())
        {
            LOG_ERRORF("Failed to persist inode, for:%s", s.ToString().c_str());
            fuse_reply_err(req, EIO);
            return;
        }
        fh->dirty = 0;
    }
    s = tx->Commit();
    if (!s.ok())
    {
        LOG_ERRORF("Commit failed in fsync for:%s", s.ToString().c_str());
        fuse_reply_err(req, EIO);
        return;
    }
    s = lsf_ctx->db->FlushWAL(true);
    if (!s.ok())
    {
        LOG_ERRORF("Failed sync for:%s", s.ToString().c_str());
        fuse_reply_err(req, EIO);
        return;
    }

    // LOG_INFO("manual flush start");
    // s = lsf_ctx->db->Flush(FlushOptions(), lsf_ctx->data_cf);
    // if (!s.ok())
    // {
    //     // 处理flush错误
    //     // 注意：此时事务已经提交，数据一致性由WAL保证
    //     LOG_ERRORF("Flush failed, for:%s", s.ToString().c_str());
    //     fuse_reply_err(req, EIO);
    //     return;
    // }
    // LOG_INFO("manual flush end");

    // uint64_t parent_dir_inode_num = get_parent_inode_num(lsf_ctx, path);

    // std::string s1 = format_string("%ld_%s", parent_dir_inode_num, file_name);
    // Slice file_key = s1;

    // CompactRangeOptions options;

    // options.exclusive_manual_compaction = true; // 允许与自动 Compaction 并行

    // s = lsf_ctx->db->CompactRange(options, lsf_ctx->meta_cf, &file_key, &file_key);
    // if (!s.ok())
    // {
    //     // 打印完整错误信息（包括错误类型和原因）
    //     LOG_ERRORF("DATA_CF CompactRange failed for path [%s]: %s", path, s.ToString().c_str()); // 包含 RocksDB 状态码和错误描述
    //     return -EIO;                                                                             // 或自定义错误码
    // }

    // s = lsf_ctx->db->CompactRange(options, lsf_ctx->data_cf, nullptr, nullptr);
    // if (!s.ok())
    // {
    //     // 打印完整错误信息（包括错误类型和原因）
    //     LOG_ERRORF("DATA_CF CompactRange failed for path [%s]: %s", path, s.ToString().c_str()); // 包含 RocksDB 状态码和错误描述
    //     return -EIO;                                                                             // 或自定义错误码
    // }
    fuse_reply_err(req, 0); // 0 表示无错误（成功）
}

static int my_fsync(const char *path, struct fuse_file_info *fi, struct LsfContext *lsf_ctx, struct LsfInode *inode)
{
    PROFILE_FUNC();
    LOG_INFO("my_fsync start");
    //  struct LsfInode *inode;
    std::string file_name = get_filename_from_path(path);

    LsfFileHandle *fh = reinterpret_cast<LsfFileHandle *>(fi->fh);
    if (!fh)
        return -EBADF;

    Status s;
    LOG_DEBUGF("Sync file ino:%ld name:%s", inode->inode_num, file_name.c_str());
    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->data_opt);

    ScopedExecutor _1([tx]()
                      { delete tx; });
    ResourceManager _r;
    _r.push_back([tx]()
                 { tx->Rollback(); });

    if (fh->dirty)
    {
        s = _lsf_persist_inode(lsf_ctx, tx, inode);
        if (!s.ok())
        {
            LOG_ERRORF("Failed to persist inode, for:%s", s.ToString().c_str());
            return -EIO;
        }
        fh->dirty = 0;
    }
    s = tx->Commit();
    if (!s.ok())
    {
        LOG_ERRORF("Commit failed in fsync for:%s", s.ToString().c_str());
        return -EIO;
    }
    s = lsf_ctx->db->FlushWAL(true);
    if (!s.ok())
    {
        LOG_ERRORF("Failed sync for:%s", s.ToString().c_str());
        return -EIO;
    }

    LOG_INFO("manual flush start");
    s = lsf_ctx->db->Flush(FlushOptions(), lsf_ctx->data_cf);
    if (!s.ok())
    {
        // 处理flush错误
        // 注意：此时事务已经提交，数据一致性由WAL保证
        LOG_ERRORF("Flush failed, for:%s", s.ToString().c_str());
    }
    LOG_INFO("manual flush end");

    uint64_t parent_dir_inode_num = get_parent_inode_num(lsf_ctx, path);

    std::string s1 = format_string("%ld_%s", parent_dir_inode_num, file_name);
    Slice file_key = s1;

    CompactRangeOptions options;

    options.exclusive_manual_compaction = true; // 允许与自动 Compaction 并行

    s = lsf_ctx->db->CompactRange(options, lsf_ctx->meta_cf, &file_key, &file_key);
    if (!s.ok())
    {
        // 打印完整错误信息（包括错误类型和原因）
        LOG_ERRORF("DATA_CF CompactRange failed for path [%s]: %s", path, s.ToString().c_str()); // 包含 RocksDB 状态码和错误描述
        return -EIO;                                                                             // 或自定义错误码
    }

    s = lsf_ctx->db->CompactRange(options, lsf_ctx->data_cf, nullptr, nullptr);
    if (!s.ok())
    {
        // 打印完整错误信息（包括错误类型和原因）
        LOG_ERRORF("DATA_CF CompactRange failed for path [%s]: %s", path, s.ToString().c_str()); // 包含 RocksDB 状态码和错误描述
        return -EIO;                                                                             // 或自定义错误码
    }

    LOG_INFO("my_fsync end");
    return 0;
}

static void lsmfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname, unsigned int flags)
{
    PROFILE_FUNC();
    LOG_INFOF("rename start, parent = %d, name = %s, newparent = %d, newname = %s",parent, name, newparent, newname);
    if (strlen(name) > NAME_MAX || strlen(newname) > NAME_MAX )
    {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    LsfDentry::Ptr dentry = getDentry(parent, name);
    
    // 开始事务
    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->meta_opt);
    // 解析地址并判断是否存在
    Status s;

    std::string s1 = format_string("%ld_%s", parent, name);
    // LOG_DEBUGF("create file_key = %s\n", s1.c_str());
    Slice file_key = s1;
    PinnableSlice old_inode_buf;
    s = tx->GetForUpdate(lsf_ctx->read_opt, lsf_ctx->meta_cf, file_key, &old_inode_buf);
    if (!s.ok())
    {
        //free(file_name);
        delete tx;
        fuse_reply_err(req, ENOENT);
        return;
    }
    // 检查目标路径
    std::string s2 = format_string("%ld_%s", newparent, newname);
    Slice file_new_key = s2;
    PinnableSlice new_inode_buf;
    s = tx->GetForUpdate(lsf_ctx->read_opt, lsf_ctx->meta_cf, file_new_key, &new_inode_buf);
    if (s.ok())
    {
        LOG_INFO("new_key found");
        int64_t new_inum;
        memcpy(&new_inum, new_inode_buf.data(), sizeof(new_inum));
        // 获取目标文件的 inode 信息
        PinnableSlice dst_inode_buf;
        s = tx->Get(lsf_ctx->read_opt, lsf_ctx->meta_cf,
                    Slice((char *)&new_inum, sizeof(new_inum)),
                    &dst_inode_buf);
        if (!s.ok())
        {
            delete tx;
            fuse_reply_err(req, EIO);
            return;
        }
        
        LsfInode *dst_inode = deserialize_inode(dst_inode_buf.data());
        if (!dst_inode)
        {
            delete tx;
            fuse_reply_err(req, ENOMEM);
            return;
        }
        if (S_ISDIR(dst_inode->mode)) {
            bool nonempty = false;
            std::string dir_prefix = format_string("%ld_", dst_inode->inode_num);
            auto it = tx->GetIterator(lsf_ctx->read_opt, lsf_ctx->meta_cf);
            for (it->Seek(dir_prefix); it->Valid(); it->Next()) {
                std::string key = it->key().ToString();
                if (key.compare(0, dir_prefix.size(), dir_prefix) != 0)
                    break;
                std::string child = key.substr(dir_prefix.size());
                if (child != "." && child != "..") {
                    nonempty = true;
                    break;
                }
            }
            delete it;
            if (nonempty) {
                free(dst_inode);
                delete tx;
                fuse_reply_err(req, ENOTEMPTY);
                return;
            }
        }
        //LOG_INFOF("rename nlink=%hu, ino=%d", dst_inode->links_count, dst_inode->inode_num);
        dst_inode->links_count--;
        dst_inode->ctime = time(NULL);
        
        if (dst_inode->links_count > 0) {
            // 持久化更新后的 inode
            s = _lsf_persist_inode(lsf_ctx, tx, dst_inode);
            if (!s.ok()) {
                free(dst_inode);
                delete tx;
                fuse_reply_err(req, EIO);
                return;
            }
        }
        
        LOG_INFOF("ln_inode nlink=%hu, ino=%d", dst_inode->links_count, dst_inode->inode_num);

        getDentryByIno(dst_inode->inode_num)->stats().st_nlink = dst_inode->links_count;
        getDentryByIno(dst_inode->inode_num)->stats().st_ctime = dst_inode->ctime;

        // 删除目标文件
        free(dst_inode);
        tx->Delete(lsf_ctx->meta_cf, file_new_key);
    }
    // 移动文件
    int64_t old_inum;
    memcpy(&old_inum, old_inode_buf.data(), sizeof(old_inum));
    // 更新源文件的 inode 信息
    PinnableSlice src_inode_buf;
    s = tx->Get(lsf_ctx->read_opt, lsf_ctx->meta_cf,
                Slice((char *)&old_inum, sizeof(old_inum)),
                &src_inode_buf);
    if (!s.ok())
    {
        delete tx;
        fuse_reply_err(req, EIO);
        return;
    }

    LsfInode *src_inode = deserialize_inode(src_inode_buf.data());
    if (!src_inode)
    {
        delete tx;
        fuse_reply_err(req, ENOMEM);
        return;
    }

    // 更新时间戳
    src_inode->ctime = src_inode->mtime = time(NULL);

    // 执行重命名操作
    // 删除旧目录项
    s = tx->Delete(lsf_ctx->meta_cf, file_key);
    if (!s.ok())
    {
        delete tx;
        free(src_inode);
        fuse_reply_err(req, EIO);
        return;
    }
    // 创建新目录项
    s = tx->Put(lsf_ctx->meta_cf, file_new_key,
                Slice((const char *)&old_inum, sizeof(old_inum)));
    if (!s.ok())
    {
        delete tx;
        free(src_inode);
        fuse_reply_err(req, EIO);
        return;
    }

    // 更新 inode
    s = _lsf_persist_inode(lsf_ctx, tx, src_inode);
    if (!s.ok())
    {
        delete tx;
        free(src_inode);
        fuse_reply_err(req, EIO);
        return;
    }

    LsfInode *p_inode = getDentryByIno(parent)->inode();
    p_inode->links_count--;
    p_inode->ctime = time(NULL);
    p_inode->mtime = time(NULL);
    s = _lsf_persist_inode(lsf_ctx, tx, p_inode);
    if (!s.ok())
    {
        delete tx;
        //free(file_name);
        LOG_ERRORF("Failed to persist inode: %s", s.ToString().c_str());
        fuse_reply_err(req, EIO); // 目标不是目录
        return;
    }
    getDentryByIno(parent)->stats().st_nlink = p_inode->links_count;
    getDentryByIno(parent)->stats().st_mtime = p_inode->mtime;
    getDentryByIno(parent)->stats().st_ctime = p_inode->ctime;

    LsfInode *np_inode = getDentryByIno(newparent)->inode();
    np_inode->links_count++;
    np_inode->ctime = time(NULL);
    np_inode->mtime = time(NULL);
    s = _lsf_persist_inode(lsf_ctx, tx, np_inode);
    if (!s.ok())
    {
        delete tx;
        //free(file_name);
        LOG_ERRORF("Failed to persist inode: %s", s.ToString().c_str());
        fuse_reply_err(req, EIO); // 目标不是目录
        return;
    }
    getDentryByIno(newparent)->stats().st_nlink = np_inode->links_count;
    getDentryByIno(newparent)->stats().st_mtime = np_inode->mtime;
    getDentryByIno(newparent)->stats().st_ctime = np_inode->ctime;

    // 提交事务
    s = tx->Commit();
    if (!s.ok())
    {
        delete tx;
        free(src_inode);
        fuse_reply_err(req, EIO);
        return;
    }
    delete tx;

    if(dentry)
    {
        LOG_INFO("开始清理旧dentry");
        eraseOldDentry(dentry, true);
        LOG_INFO("开始添加rename后的dentry");
        cacheAdd(src_inode, newparent, newname);  
        // new_dentry->stats().st_ctime = src_inode->ctime;
        // new_dentry->stats().st_mtime = src_inode->mtime;

    }
    //free(src_inode);
    LOG_INFO("lsmfs_rename end");
    fuse_reply_err(req, 0);
}

// bool is_valid_uid(uid_t uid)
// {
//     struct passwd *pwd = getpwuid(uid);
//     return (pwd != nullptr);
// }

// bool is_valid_gid(gid_t gid)
// {
//     struct group *grp = getgrgid(gid);
//     return (grp != nullptr);
// }

static void lsmfs_flush(fuse_req_t req, fuse_ino_t ino,struct fuse_file_info *fi)
{
    PROFILE_FUNC();
    LOG_INFO("lsmfs_flush start");
    LsfDentry::Ptr dentry = getDentryByIno(ino);
    if (!dentry) {
        LOG_ERRORF("dentry not found: ino=%lu", ino);
        fuse_reply_err(req, ENOENT); // 错误回复
        return;
    }
    LsfInode *inode = dentry->inode();
    if (!inode) {
        LOG_ERRORF("Inode not found: ino=%lu", ino);
        fuse_reply_err(req, ENOENT); // 错误回复
        return;
    }

    LsfFileHandle *fh = reinterpret_cast<LsfFileHandle *>(fi->fh);
    if (!fh)
    {
        fuse_reply_err(req, EBADF);
        return;
    }
    // int err = fh ? fh->error.load(std::memory_order_relaxed) : 0;
    // if (err < 0) {
    //     // 后台写失败，返回第一次失败的 errno
    //     fuse_reply_err(req, -err);
    //     return;
    // }
    Status s;
    LOG_DEBUGF("Sync file ino:%ld", ino);
    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->data_opt);

    ScopedExecutor _1([tx]()
                      { delete tx; });
    ResourceManager _r;
    _r.push_back([tx]()
                 { tx->Rollback(); });

    if (fh->dirty)
    {
        s = _lsf_persist_inode(lsf_ctx, tx, inode);
        if (!s.ok())
        {
            LOG_ERRORF("Failed to persist inode, for:%s", s.ToString().c_str());
            fuse_reply_err(req, EIO);
            return;
        }
        fh->dirty = 0;
    }
    s = tx->Commit();
    if (!s.ok())
    {
        LOG_ERRORF("Commit failed in fsync for:%s", s.ToString().c_str());
        fuse_reply_err(req, EIO);
        return;
    }
    s = lsf_ctx->db->FlushWAL(true);
    if (!s.ok())
    {
        LOG_ERRORF("Failed sync for:%s", s.ToString().c_str());
        fuse_reply_err(req, EIO);
        return;
    }
    _r.releaseall();
    fuse_reply_err(req, 0);
}
    // LOG_INFO("manual flush start");
    // s = lsf_ctx->db->Flush(FlushOptions(), lsf_ctx->data_cf);
    // if (!s.ok())
    // {
    //     // 处理flush错误
    //     // 注意：此时事务已经提交，数据一致性由WAL保证
    //     LOG_ERRORF("Flush failed, for:%s", s.ToString().c_str());
    //     fuse_reply_err(req, EIO);
    //     return;
    // }
    // LOG_INFO("manual flush end");

    // uint64_t parent_dir_inode_num = get_parent_inode_num(lsf_ctx, path);

    // std::string s1 = format_string("%ld_%s", parent_dir_inode_num, file_name);
    // Slice file_key = s1;

    // CompactRangeOptions options;

    // options.exclusive_manual_compaction = true; // 允许与自动 Compaction 并行

    // s = lsf_ctx->db->CompactRange(options, lsf_ctx->meta_cf, &file_key, &file_key);
    // if (!s.ok())
    // {
    //     // 打印完整错误信息（包括错误类型和原因）
    //     LOG_ERRORF("DATA_CF CompactRange failed for path [%s]: %s", path, s.ToString().c_str()); // 包含 RocksDB 状态码和错误描述
    //     return -EIO;                                                                             // 或自定义错误码
    // }

    // s = lsf_ctx->db->CompactRange(options, lsf_ctx->data_cf, nullptr, nullptr);
    // if (!s.ok())
    // {
    //     // 打印完整错误信息（包括错误类型和原因）
    //     LOG_ERRORF("DATA_CF CompactRange failed for path [%s]: %s", path, s.ToString().c_str()); // 包含 RocksDB 状态码和错误描述
    //     return -EIO;                                                                             // 或自定义错误码
    // }


static void lsmfs_fallocate(fuse_req_t req, fuse_ino_t ino, int mode, off_t offset, off_t length, struct fuse_file_info *fi)
{
    PROFILE_FUNC();
    LOG_INFO("lsmfs_fallocate start");
    const int KEEP_SIZE = FALLOC_FL_KEEP_SIZE;
    // 不支持其他 fancy 模式
    if (mode & ~KEEP_SIZE) 
    {
        fuse_reply_err(req, EOPNOTSUPP);
        return;
    }
    if (length == 0) {
        fuse_reply_err(req, 0);
        return;
    }
        
    LsfInode *inode = getDentryByIno(ino)->inode();

    // 计算要 pre‐allocate 的块范围
    const int64_t BS = inode->block_size;
    int64_t start_blk = offset / BS;
    int64_t end_blk   = (offset + length + BS - 1) / BS;

    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->meta_opt);
    ScopedExecutor  tx_del([tx](){ delete tx; });
    ResourceManager tx_rb;
    tx_rb.push_back([tx](){ tx->Rollback(); });

    for (int64_t i = start_blk; i < end_blk; i++) {
        block_key key = {{{ .block_index = (uint64_t)i,
                            .inode_no    = (uint64_t)inode->inode_num }}};
        // 用 Get 检查是否已有这个块
        PinnableSlice existing;
        Status s = tx->Get(lsf_ctx->read_opt,
                           lsf_ctx->data_cf,
                           Slice((char*)&key, sizeof(key)),
                           &existing);
        if (s.IsNotFound()) {
            // 构造一个全零的 block_head
            void *buf = calloc(1, LSF_BLOCK_HEAD_SIZE);
            if (!buf) {
                fuse_reply_err(req, ENOMEM);
                return;
            }
            struct block_head *head = (struct block_head*)buf;
            // head->data_bmp 默认为 0，表示“已分配但全零”
            Slice val((char*)buf, LSF_BLOCK_HEAD_SIZE);
            s = tx->Put(lsf_ctx->data_cf,
                        Slice((char*)&key, sizeof(key)),
                        val);
            free(buf);
            if (!s.ok()) {
                    fuse_reply_err(req, EIO);
                    return;
            }
        }
        else if (!s.ok()) {
            fuse_reply_err(req, EIO);
            return;
        }
    }

    if (!(mode & KEEP_SIZE)) {
        off_t want = offset + length;
        inode->file_size = std::max<off_t>(inode->file_size, want);
        Status s = _lsf_persist_inode(lsf_ctx, tx, inode);
        if (!s.ok()) {
            fuse_reply_err(req, EIO);
            return;
        }
            
    }

    Status s = tx->Commit();
    if (!s.ok()) {
        fuse_reply_err(req, EIO);
        return;
    }
    tx_rb.releaseall();
    LOG_INFO("lsmfs_fallocate completed successfully");

    
    fuse_reply_err(req, 0);
}

static void lsmfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,const char *newname)
{
    PROFILE_FUNC();
    LOG_INFOF("start link ,ino = %d, newparent = %d, newname = %s\n",ino, newparent, newname);
    LsfInode *inode = getDentryByIno(ino)->inode();
    if (!inode) {
        LOG_ERRORF("Inode not found: ino=%lu", ino);
        fuse_reply_err(req, ENOENT); // 错误回复
        return;
    }

    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->meta_opt);
    ScopedExecutor _1([tx]()
                      { delete tx; });
    ResourceManager _r;
    _r.push_back([tx]()
                 { tx->Rollback(); });
    inode->links_count++;
    inode->ctime = time(NULL);
    inode->mtime = time(NULL);
    Status s = _lsf_persist_inode(lsf_ctx, tx, inode);
    if (!s.ok())
    {
        //free(file_name);
        LOG_ERRORF("Failed to persist inode: %s", s.ToString().c_str());
        fuse_reply_err(req, EIO); // 目标不是目录
        return;
    }

    LsfInode *p_inode = getDentryByIno(inode->parent_dir_inode_num)->inode();
    p_inode->ctime = time(NULL);
    p_inode->mtime = time(NULL);
    s = _lsf_persist_inode(lsf_ctx, tx, p_inode);
    if (!s.ok())
    {
        //free(file_name);
        LOG_ERRORF("Failed to persist inode: %s", s.ToString().c_str());
        fuse_reply_err(req, EIO); // 目标不是目录
        return;
    }

    uint64_t new_ino = ino;
    // 假设 uint64_t 对应于 unsigned long
    LOG_DEBUGF("new_ino = %lu", (unsigned long)new_ino);

    std::string file_key = format_string("%ld_%s", newparent, newname);
    Slice file_key_slice = file_key;
    Slice inode_no_slice((const char *)&new_ino, sizeof(new_ino));

    s = tx->Put(lsf_ctx->meta_cf, file_key_slice, inode_no_slice);
    if (!s.ok())
    {
        //free(file_name);
        LOG_ERRORF("Failed to create hard link: %s", s.ToString().c_str());
        fuse_reply_err(req, EIO); // 目标不是目录
        return;
    }

    s = tx->Commit();
    if (!s.ok())
    {
        //free(file_name);
        LOG_ERRORF("Failed to commit hard link transaction: %s", s.ToString().c_str());
        fuse_reply_err(req, EIO); // 目标不是目录
        return;
    }
    //free(file_name);
    _r.releaseall();
    LOG_INFO("lsmfs_link end");

    /*
        int res = link(oldpath, newpath);
        if (res == -1)
            return -errno;
      */
      // 准备回复entry
    getDentryByIno(ino)->stats().st_nlink = inode->links_count;
    getDentryByIno(ino)->stats().st_mtime = inode->mtime;
    getDentryByIno(ino)->stats().st_ctime = inode->ctime;

    getDentryByIno(inode->parent_dir_inode_num)->stats().st_ctime = p_inode->ctime;
    getDentryByIno(inode->parent_dir_inode_num)->stats().st_mtime = p_inode->mtime;

    struct fuse_entry_param entry;
    memset(&entry, 0, sizeof(entry));
    entry.ino = inode->inode_num;
    struct stat attr = {}; // 初始化所有成员为 0
    attr.st_ino = inode->inode_num;
    attr.st_mode = inode->mode;
    attr.st_nlink = inode->links_count;
    attr.st_uid = inode->uid;
    attr.st_gid = inode->gid;
    attr.st_atime = inode->atime;
    attr.st_mtime = inode->mtime;
    attr.st_ctime = inode->ctime;
    attr.st_size  = inode->file_size;
    entry.attr = attr;

    entry.attr_timeout = 1.0;
    entry.entry_timeout = 1.0;
    cacheAdd(inode, newparent, newname);

    fuse_reply_entry(req, &entry);
}

static void lsmfs_symlink(fuse_req_t req, const char *link, fuse_ino_t parent, const char *name)
{
    PROFILE_FUNC();
    LOG_INFOF("start symlink ,parent ino = %d, name = %s\n",parent, name);
    if (strlen(name) > NAME_MAX)
    {
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    LsfInode *new_inode = lsf_calloc_inode();
    // InodeGuard inode_guard(new_inode);

    new_inode->links_count = 1;
    new_inode->mode = S_IFLNK | 0777;
    new_inode->inode_num = lsf_ctx->generate_inode_num();
    new_inode->parent_dir_inode_num = parent;
    new_inode->block_size = BLOCK_SIZE;
    new_inode->atime = new_inode->ctime = new_inode->mtime = time(NULL);
    new_inode->uid = getuid();
    new_inode->gid = getgid();

    int64_t i_num = new_inode->inode_num;
    LOG_INFOF("Generated inode number: %lu", i_num);

    assert(sizeof(struct LsfInode) == LSF_INODE_SIZE);
    //  LOG_DEBUGF("file new_inode mode: %o", new_inode->mode);
    // 更新存储
    Transaction *tx = lsf_ctx->db->BeginTransaction(lsf_ctx->meta_opt);
    ScopedExecutor _1([tx]()
                      { delete tx; });
    ResourceManager _r;
    _r.push_back([tx]()
                 { tx->Rollback(); });

    std::string file_key = format_string("%ld_%s", parent, name);
    Slice file_key_slice = file_key;
    Slice inode_no_slice((const char *)&new_inode->inode_num, sizeof(new_inode->inode_num));

    PinnableSlice tmp;
    Status s = tx->GetForUpdate(lsf_ctx->read_opt,
                                lsf_ctx->meta_cf,
                                Slice(file_key), &tmp);
    if (s.ok()) {
        LOG_INFO("同名数据已存在");
        fuse_reply_err(req, EEXIST);
        return;
    } else if (!s.IsNotFound()) {
        LOG_INFO("读元数据出错");
        fuse_reply_err(req, EIO);
        return;
    }
    s = tx->Put(lsf_ctx->meta_cf, file_key_slice, inode_no_slice);
    if (!s.ok())
    {
        //free(file_name);
        LOG_ERRORF("Failed to create symlink: %s", s.ToString().c_str());
        lsf_dec_inode_ref(new_inode);
        fuse_reply_err(req, EIO);
        return;
    }

    s = _lsf_persist_inode(lsf_ctx, tx, new_inode);
    if (!s.ok())
    {
        //free(file_name);
        LOG_ERRORF("Failed put inode:%s, for:%s", file_key.data(), s.ToString().c_str());
        lsf_dec_inode_ref(new_inode);
        fuse_reply_err(req, EIO);
        return;
    }

    Slice symlink_target(link, strlen(link) + 1);
    std::string symlink_key;
    symlink_key.reserve(1 + sizeof(new_inode->inode_num));
    symlink_key.push_back('L');
    symlink_key.append(reinterpret_cast<const char*>(&new_inode->inode_num),
                    sizeof(new_inode->inode_num));
    
    s = tx->Put(lsf_ctx->meta_cf, Slice(symlink_key.data(), symlink_key.size()), symlink_target);
    if (!s.ok())
    {
        //free(file_name);
        LOG_ERRORF("Failed to put symlink target:%s, for:%s", file_key_slice.data(), s.ToString().c_str());
        lsf_dec_inode_ref(new_inode);
        fuse_reply_err(req, EIO);
        return;
    }

    s = tx->Put(lsf_ctx->meta_cf, INODE_SEED_KEY, Slice((char *)&lsf_ctx->inode_seed, sizeof(lsf_ctx->inode_seed)));
    if (!s.ok())
    {
        //free(file_name);
        LOG_ERRORF("Failed put seed:%s, for:%s", file_key.data(), s.ToString().c_str());
        lsf_dec_inode_ref(new_inode);
        fuse_reply_err(req, EIO);
        return;
    }

    LsfInode *p_inode = getDentryByIno(parent)->inode();
    p_inode->ctime = time(NULL);
    p_inode->mtime = time(NULL);

    // 持久化父 inode
    s = _lsf_persist_inode(lsf_ctx, tx, p_inode);
    if (!s.ok()) {
        delete tx;
        fuse_reply_err(req, EIO);
        return;
    }
    getDentryByIno(parent)->stats().st_mtime = getDentryByIno(parent)->stats().st_ctime = p_inode->mtime;

    s = tx->Commit();
    if (!s.ok())
    {
        //free(file_name);
        LOG_ERRORF("Failed create file:%s on committing, for:%s", file_key.data(), s.ToString().c_str());
        lsf_dec_inode_ref(new_inode);
        fuse_reply_err(req, EIO);
        return;
    }

    _r.releaseall();
    LOG_INFO("lsmfs_symlink end");
    // 构造回复条目
    struct fuse_entry_param entry;
    memset(&entry, 0, sizeof(entry));
    entry.ino = new_inode->inode_num;
    struct stat attr = {}; // 初始化所有成员为 0
    attr.st_ino = new_inode->inode_num;
    attr.st_mode = new_inode->mode;
    attr.st_nlink = new_inode->links_count;
    attr.st_uid = new_inode->uid;
    attr.st_gid = new_inode->gid;
    attr.st_atime = new_inode->atime;
    attr.st_mtime = new_inode->mtime;
    attr.st_ctime = new_inode->ctime;
    new_inode->file_size = strlen(link);
    attr.st_size  = strlen(link);
    entry.attr = attr;

    entry.attr_timeout = 1.0;
    entry.entry_timeout = 1.0;
    // 回复请求

    cacheAdd(new_inode, parent, name);

    std::string read_back;
    auto s2 = lsf_ctx->db->Get(lsf_ctx->read_opt,
                           lsf_ctx->meta_cf,
                           Slice((char*)&new_inode->inode_num, sizeof(new_inode->inode_num)),
                           &read_back);
    LOG_INFOF("  post-commit check Get → code=%s, got=\"%s\"",
          s2.ToString().c_str(),
          s2.ok() ? read_back.c_str() : "<none>");
    uint64_t i2 = new_inode->inode_num;
LOG_DEBUGF("  [symlink] key‐ptr=%p, key‐bytes=%*ph, key‐u64=%lu",
           (void*)&i2,
           (int)sizeof(i2), (char*)&i2,
           i2);
LOG_DEBUGF("  [symlink] CF handle ptr=%p", lsf_ctx->data_cf);
    fuse_reply_entry(req, &entry);
    //free(file_name);
    //lsf_dec_inode_ref(new_inode);
}

static const char *file_type(mode_t mode) {
    switch (mode & S_IFMT) {
    case S_IFREG:  return "regular file";
    case S_IFDIR:  return "directory";
    case S_IFLNK:  return "symlink";
    case S_IFCHR:  return "character device";
    case S_IFBLK:  return "block device";
    case S_IFIFO:  return "FIFO/pipe";
    case S_IFSOCK: return "socket";
    default:       return "unknown";
    }
}

static void lsmfs_readlink(fuse_req_t req, fuse_ino_t ino)
{
    PROFILE_FUNC();
    LOG_INFOF("readlink start, ino = %d", ino);
    LsfDentry::Ptr dentry = getDentryByIno(ino);
    if (!dentry) {
        LOG_ERRORF("dentry not found: ino=%lu", ino);
        fuse_reply_err(req, ENOENT); // 错误回复
        return;
    }
    LsfInode *inode = dentry->inode();
    if (!inode) {
        LOG_ERRORF("Inode %lu not found", ino);
        fuse_reply_err(req, ENOENT);
        return;
    }
    const char *type = file_type(inode->mode);
    LOG_ERRORF("Inode %lu type=%s", ino, type);
    // 检查是否为符号链接
    if (!S_ISLNK(inode->mode)) {    
        LOG_ERRORF("Inode %lu is not a symlink", ino);
        lsf_dec_inode_ref(inode);
        fuse_reply_err(req, EINVAL);
        return;
    }
    uint64_t i2 = inode->inode_num;
LOG_DEBUGF("  [readlink] key‐ptr=%p, key‐bytes=%*ph, key‐u64=%lu",
           (void*)&i2,
           (int)sizeof(i2), (char*)&i2,
           i2);
LOG_DEBUGF("  [readlink] CF handle ptr=%p", lsf_ctx->data_cf);
    PinnableSlice inode_buf;
    std::string symlink_key;
    symlink_key.reserve(1 + sizeof(inode->inode_num));
    symlink_key.push_back('L');
    symlink_key.append(reinterpret_cast<const char*>(&inode->inode_num),
                    sizeof(inode->inode_num));
    //Slice inode_key((char *)&inode->inode_num, sizeof(inode->inode_num));
    Status s = lsf_ctx->db->Get(lsf_ctx->read_opt, lsf_ctx->meta_cf, Slice(symlink_key.data(), symlink_key.size()), &inode_buf);
    
    if (!s.ok())
    {
        if (s.IsNotFound()) {
            LOG_ERRORF("Failed to find symlink target for inode:%ld, for:%s", inode->inode_num, s.ToString().c_str());
            // 这个 inode 下压根没有 symlink target
            fuse_reply_err(req, ENOENT);
            
        }
        else {
            LOG_ERRORF("Failed to read symlink target for inode:%ld, readlink Status code=%d, msg=%s", inode->inode_num, static_cast<int>(s.code()),s.ToString().c_str());
            // 其余一律 EIO
            fuse_reply_err(req, EIO);
        }

        lsf_dec_inode_ref(inode);
        return;
    }

    size_t target_len = inode_buf.size();
    if (target_len >= PATH_MAX)
    {
        LOG_ERRORF("Symlink target too long for inode:%ld", inode->inode_num);
        lsf_dec_inode_ref(inode);
        fuse_reply_err(req, ENAMETOOLONG);
        return;
    }
    char buf[PATH_MAX];
    memcpy(buf, inode_buf.data(), target_len);
    buf[target_len] = '\0';


    //fuse_reply_readlink(req, inode_buf.data());
    fuse_reply_readlink(req, buf);
}

    // 释放资源
    //lsf_dec_inode_ref(inode);
    /*
        // 获取 inode 并更新时间
        PinnableSlice inode_meta_buf;
        s = lsf_ctx->db->Get(lsf_ctx->read_opt, lsf_ctx->meta_cf, inode_key, &inode_meta_buf);
        if (!s.ok()) {
            LOG_ERRORF("Failed to read inode metadata for inode:%ld, for:%s", inode->inode_num, s.ToString().c_str());
            return -EIO;
        }

        ViveInode* inode = deserialize_inode(inode_meta_buf.data());
        if (inode == NULL)
            return -EIO;

        if (!inode->noatime) {
            inode->i_atime = time(NULL);
            inode->dirty = 1;
            vn_persist_inode(ctx, inode);
        }
    */
//}

// fuse_operations init_lsmfs_oper()
void init_lsmfs_oper(fuse_lowlevel_ops &lsmfs_oper)
{

    // fuse_operations lsmfs_oper = {};
    memset(&lsmfs_oper, 0, sizeof(fuse_lowlevel_ops));

    lsmfs_oper.init = lsmfs_init;
    lsmfs_oper.lookup = lsmfs_lookup;
    lsmfs_oper.getattr = lsmfs_getattr;
    lsmfs_oper.forget = lsmfs_forget;
    lsmfs_oper.setattr = lsmfs_setattr;
    lsmfs_oper.opendir = lsmfs_opendir;
    lsmfs_oper.readdir = lsmfs_readdir;
    lsmfs_oper.mkdir = lsmfs_mkdir;
    lsmfs_oper.create = lsmfs_create;
    lsmfs_oper.mknod = lsmfs_mknod;
    lsmfs_oper.open = lsmfs_open;
    lsmfs_oper.write = lsmfs_write;
    lsmfs_oper.read = lsmfs_read;
    lsmfs_oper.rename = lsmfs_rename;
    lsmfs_oper.unlink = lsmfs_unlink;
    lsmfs_oper.rmdir = lsmfs_rmdir;
    //lsmfs_oper.utimens = lsmfs_utimens;
    lsmfs_oper.release = lsmfs_release;
    lsmfs_oper.destroy = lsmfs_destroy;

    lsmfs_oper.fallocate = lsmfs_fallocate;
    lsmfs_oper.link = lsmfs_link;
    lsmfs_oper.symlink = lsmfs_symlink;
    lsmfs_oper.readlink = lsmfs_readlink;
    lsmfs_oper.flush = lsmfs_flush;
    lsmfs_oper.fsync = lsmfs_fsync;
    lsmfs_oper.releasedir = lsmfs_releasedir;
    //	return lsmfs_oper;
}

static __always_inline Status _lsf_persist_inode(LsfContext *ctx, Transaction *tx, LsfInode *inode)
{
    // LOG_DEBUGF("persist inode inode_num :%ld", inode->inode_num);
    Status s = tx->Put(ctx->meta_cf, Slice((char *)&inode->inode_num, sizeof(inode->inode_num)), Slice((char *)inode, LSF_INODE_SIZE));

    return s;
}

const char *block_key::to_string() const
{
    static __thread char str[64];
    snprintf(str, sizeof(str), "[inode_no:%lld, index:%lld]", inode_no, block_index);
    return str;
}

LsfFile::LsfFile() : inode(NULL), inode_num(0), inode_parent_dir_num(0), noatime(0), nomtime(0), dirty(0)
{
}

LsfFile::~LsfFile()
{
    if (inode)
        lsf_dec_inode_ref(inode);
}

LsfFileHandle::LsfFileHandle() : error(0),dirty(0)
{
}

LsfFileHandle::~LsfFileHandle()
{
}

static void *lsmfs_getDBStats(DB *db, struct LsfContext *lsf_ctx)
{

    // 启动后台线程，定期打印统计信息
    std::thread([db, lsf_ctx]()
                {
                    while (true)
                    {
                        std::this_thread::sleep_for(std::chrono::seconds(60)); // 每 10 秒打印一次
                                                                               //            if (lsf_ctx->statistics) {
                                                                               //
                        std::string stats;
                        if (lsf_ctx->db->GetProperty(lsf_ctx->data_cf, "rocksdb.stats", &stats))
                        {
                            LOG_DEBUGF("RocksDB Stats:\n%s", stats.c_str());
                        }

                        LOG_DEBUGF("meta disableWAL = %d", lsf_ctx->meta_opt.disableWAL);
                        LOG_DEBUGF("data disableWAL = %d", lsf_ctx->data_opt.disableWAL);
                        std::string vvalue;
                        if (db->GetProperty(lsf_ctx->default_cf, "rocksdb.compaction-pending", &vvalue))
                        {
                            LOG_DEBUGF("default CF - Pending Compactions: %s\n", vvalue.c_str());
                        }
                        //
                        // 检查 L0 层文件数量
                        if (db->GetProperty(lsf_ctx->default_cf, "rocksdb.num-files-at-level0", &vvalue))
                        {
                            LOG_DEBUGF("default CF - Number of files at L0: %s\n", vvalue.c_str());
                        }
                        if (db->GetProperty(lsf_ctx->default_cf, "rocksdb.num-immutable-mem-table", &vvalue))
                        {
                            LOG_DEBUGF("default CF - num-immutable-mem-table: %s\n", vvalue.c_str());
                        }

                        if (db->GetProperty(lsf_ctx->default_cf, "rocksdb.mem-table-flush-pending", &vvalue))
                        {
                            LOG_DEBUGF("default CF - mem-table-flush-pending: %s\n", vvalue.c_str());
                        }
                        if (db->GetProperty(lsf_ctx->default_cf, "rocksdb.size-all-mem-tables", &vvalue))
                        {
                            LOG_DEBUGF("default CF - size-all-mem-tables: %s\n", vvalue.c_str());
                        }

                        if (db->GetProperty(lsf_ctx->default_cf, "rocksdb.cf-file-histogram", &vvalue))
                        {
                            LOG_DEBUGF("default CF - cf-file-histogram: %s\n", vvalue.c_str());
                        }

                        uint64_t i_vvalue;
                        if (db->GetIntProperty(lsf_ctx->default_cf, "rocksdb.num-running-flushes", &i_vvalue))
                        {
                            LOG_DEBUGF("default CF - rocksdb.num-running-flushes: %d\n", i_vvalue);
                        }
                        if (db->GetIntProperty(lsf_ctx->default_cf, "rocksdb.background-errors", &i_vvalue))
                        {
                            LOG_DEBUGF("default CF - rocksdb.background-errors: %d\n", i_vvalue);
                        }
                        if (db->GetIntProperty(lsf_ctx->default_cf, "rocksdb.actual-delayed-write-rate", &i_vvalue))
                        {
                            LOG_DEBUGF("default CF - rocksdb.actual-delayed-write-rate: %d\n", i_vvalue);
                        }
                        if (db->GetIntProperty(lsf_ctx->default_cf, "rocksdb.is-write-stopped", &i_vvalue))
                        {
                            LOG_DEBUGF("default CF - rocksdb.is-write-stopped: %d\n", i_vvalue);
                        }

                        if (db->GetProperty(lsf_ctx->default_cf, "rocksdb.levelstats", &vvalue))
                        {
                            LOG_DEBUGF("default CF - rocksdb.levelstats: \n%s", vvalue.c_str());
                        }

                        //
                        //                 // 检查 L0 层文件数量
                        //                 if (lsf_ctx->db->GetProperty(lsf_ctx->meta_cf, "rocksdb.num-files-at-level0", &value)) {
                        //                     LOG_DEBUGF("Meta CF - Number of files at L0: \n%s", value.c_str());
                        //                 }
                        //
                        //                 // 检查 Block Cache 使用情况
                        //                 if (lsf_ctx->db->GetProperty(lsf_ctx->meta_cf, "rocksdb.block-cache-usage", &value)) {
                        //                     LOG_DEBUGF("Meta CF - Block Cache Usage: \n%s", value.c_str());
                        //                 }
                        //
                        //                 // 检查是否有未完成的 Compaction
                        //
                        //                 if (lsf_ctx->db->GetProperty(lsf_ctx->data_cf, "rocksdb.options-statistics", &value)) {
                        //                       LOG_DEBUGF("Data CF - options-statistics: %s\n", value.c_str());
                        //                   }
                        //
                        std::string value;
                        if (db->GetProperty(lsf_ctx->data_cf, "rocksdb.compaction-pending", &value))
                        {
                            LOG_DEBUGF("Data CF - Pending Compactions: %s\n", value.c_str());
                        }
                        //
                        // 检查 L0 层文件数量
                        if (db->GetProperty(lsf_ctx->data_cf, "rocksdb.num-files-at-level0", &value))
                        {
                            LOG_DEBUGF("Data CF - Number of files at L0: %s\n", value.c_str());
                        }
                        if (db->GetProperty(lsf_ctx->data_cf, "rocksdb.num-immutable-mem-table", &value))
                        {
                            LOG_DEBUGF("Data CF - num-immutable-mem-table: %s\n", value.c_str());
                        }

                        if (db->GetProperty(lsf_ctx->data_cf, "rocksdb.mem-table-flush-pending", &value))
                        {
                            LOG_DEBUGF("Data CF - mem-table-flush-pending: %s\n", value.c_str());
                        }
                        if (db->GetProperty(lsf_ctx->data_cf, "rocksdb.size-all-mem-tables", &value))
                        {
                            LOG_DEBUGF("Data CF - size-all-mem-tables: %s\n", value.c_str());
                        }

                        if (db->GetProperty(lsf_ctx->data_cf, "rocksdb.cf-file-histogram", &value))
                        {
                            LOG_DEBUGF("Data CF - cf-file-histogram: %s\n", value.c_str());
                        }

                        uint64_t i_value;
                        if (db->GetIntProperty(lsf_ctx->data_cf, "rocksdb.num-running-flushes", &i_value))
                        {
                            LOG_DEBUGF("Data CF - rocksdb.num-running-flushes: %d\n", i_value);
                        }
                        if (db->GetIntProperty(lsf_ctx->data_cf, "rocksdb.background-errors", &i_value))
                        {
                            LOG_DEBUGF("Data CF - rocksdb.background-errors: %d\n", i_value);
                        }
                        if (db->GetIntProperty(lsf_ctx->data_cf, "rocksdb.actual-delayed-write-rate", &i_value))
                        {
                            LOG_DEBUGF("Data CF - rocksdb.actual-delayed-write-rate: %d\n", i_value);
                        }
                        if (db->GetIntProperty(lsf_ctx->data_cf, "rocksdb.is-write-stopped", &i_value))
                        {
                            LOG_DEBUGF("Data CF - rocksdb.is-write-stopped: %d\n", i_value);
                        }

                        if (db->GetProperty(lsf_ctx->data_cf, "rocksdb.levelstats", &value))
                        {
                            LOG_DEBUGF("Data CF - rocksdb.levelstats: \n%s", value.c_str());
                        }

                        rocksdb::CompactRangeOptions opts;
                        opts.exclusive_manual_compaction = true;
                    
                        LOG_INFO("Periodic compaction start ");

                        // data_cf
                        db->CompactRange(opts, lsf_ctx->data_cf, nullptr, nullptr);
                        // 添加到日志代码之前
                        uint64_t compaction_bytes_flushed = 0;
                        uint64_t compaction_bytes_in = 0;
                        uint64_t compaction_bytes_out = 0;

                        // 获取刷新的字节数
                        db->GetIntProperty(lsf_ctx->data_cf, "rocksdb.bytes.flushed", &compaction_bytes_flushed);

                        // 获取压缩输入输出
                        db->GetIntProperty(lsf_ctx->data_cf, "rocksdb.compaction.bytes.read", &compaction_bytes_in);
                        db->GetIntProperty(lsf_ctx->data_cf, "rocksdb.compaction.bytes.written", &compaction_bytes_out);

                        // 添加到日志
                        LOG_INFOF("Compacted data: Flushed=%lluB, Read=%lluB, Written=%lluB", 
                                compaction_bytes_flushed, compaction_bytes_in, compaction_bytes_out);

                                rocksdb::ColumnFamilyMetaData cf_meta;
                        db->GetColumnFamilyMetaData(lsf_ctx->data_cf, &cf_meta);

                        std::string level_stats = "Level stats after compaction:\n";
                        char buffer[128];  // 适当大小的缓冲区
                        for (const auto& level_meta : cf_meta.levels) {
                            snprintf(
                                buffer, sizeof(buffer),
                                "L%d: Files=%zu, Size=%.2fMB\n", 
                                level_meta.level,
                                level_meta.files.size(),
                                static_cast<double>(level_meta.size) / (1024 * 1024)
                            );
                            level_stats += buffer;
                        }

                        LOG_INFO(level_stats.c_str());
                    } })
        .detach(); // 将线程设置为后台线程
    return nullptr;
}

void thread_function(DB *db)
{
    std::thread([db]()
                {
    while(true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(5));
		  std::string value;
                   uint64_t i_value;

    if (db->GetProperty("rocksdb.compaction-pending", &value)) {
                    LOG_DEBUGF("Data CF - Pending Compactions: %s\n", value.c_str());
                   }
//
                   // 检查 L0 层文件数量
                   if (db->GetProperty("rocksdb.num-files-at-level0", &value)) {
	                    LOG_DEBUGF("Data CF - Number of files at L0: %s\n", value.c_str()); 
                   }
                   if (db->GetProperty("rocksdb.num-immutable-mem-table", &value)) {
					LOG_DEBUGF("Data CF - num-immutable-mem-table: %s\n", value.c_str());
                   }
                   if (db->GetProperty("rocksdb.estimate-compression-ratio", &value)) {
						LOG_DEBUGF("Data CF - estimate-compression-ratio: %s\n", value.c_str());
                   }

                   if (db->GetProperty("rocksdb.mem-table-flush-pending", &value)) {
						LOG_DEBUGF("Data CF - mem-table-flush-pending: %s\n", value.c_str());                    
                   }
                   if (db->GetProperty("rocksdb.size-all-mem-tables", &value)) {
                   		 LOG_DEBUGF("Data CF - size-all-mem-tables: %s\n", value.c_str());
				   }
                   if (db->GetProperty("rocksdb.flush-micros", &value)) {
                   		LOG_DEBUGF("Data CF - flush-micros: %s\n", value.c_str());
				   }
                   if (db->GetProperty("rocksdb.flush-count", &value)) {
					    LOG_DEBUGF("Data CF - flush-count: %s\n", value.c_str());
                   }


                   if (db->GetProperty("rocksdb.cf-file-histogram", &value)) {
                   		LOG_DEBUGF("Data CF - cf-file-histogram: %s\n", value.c_str());
				   }
                   if (db->GetProperty("rocksdb.total-wal-size", &value)) {
						LOG_DEBUGF("Data CF - total-wal-size: %s\n", value.c_str());
                   }


                   if (db->GetIntProperty("rocksdb.num-running-flushes", &i_value)) {
						LOG_DEBUGF("Data CF - rocksdb.num-running-flushes: %d\n", i_value);
                   }
                   if (db->GetIntProperty("rocksdb.background-errors", &i_value)) {
						LOG_DEBUGF("Data CF - rocksdb.background-errors: %d\n", i_value);
                   }
                   if (db->GetIntProperty("rocksdb.actual-delayed-write-rate", &i_value)) {
						LOG_DEBUGF("Data CF - rocksdb.actual-delayed-write-rate: %d\n", i_value);
                   }
                   if (db->GetIntProperty("rocksdb.is-write-stopped", &i_value)) {
						LOG_DEBUGF("Data CF - rocksdb.is-write-stopped: %d\n", i_value);
                   }
                   if (db->GetProperty("rocksdb.levelstats", &value)) {
						LOG_DEBUGF("Data CF - rocksdb.levelstats: \n%s", value.c_str());
                   }
		} })
        .detach();
}