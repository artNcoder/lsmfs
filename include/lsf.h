#ifndef LSM_FUSE_H
#define LSM_FUSE_H

// #define FUSE_USE_VERSION 26
#include "lsf_dentry.h"
#include "lsf_common.h"
#include <fuse3/fuse_lowlevel.h>
#include <string>
#include <cstring>
#include <iostream>
#include <linux/types.h>
#include "lsf_utils.h"
#include <nlohmann/json.hpp>
#include <map>
#include <rocksdb/db.h>
#include <pwd.h>
#include <grp.h>


#ifdef __cplusplus
extern "C"
{
#endif

#define LSF_INODE_SIZE 256
#define INODE_SEED_KEY "__inode_key__"


    typedef int64_t off_t;
    struct LsfInode;
    struct LsfContext;
    struct LsfFile;
    class LsfDentry;

    struct LsfInode
    {
        uint64_t inode_num;   // 唯一的inode编号
        uint64_t parent_dir_inode_num;  // 父目录的inode号
        uint64_t file_size;   // 文件大小（字节）
		mode_t mode;
	    uid_t uid;
    	gid_t gid;        	
		time_t atime, mtime, ctime;
        uint32_t  atime_ns,mtime_ns;
		uint16_t links_count; // 硬链接数
        uint32_t flags;      // 文件系统特定的标志
        uint32_t block_size; // 数据块的大小
        uint64_t rdev;                  // 设备号（仅 S_IFCHR/S_IFBLK 有意义）

        uint64_t _reserve[19];
        //uint32_t _reserve_1;
        uint32_t ref_count; // 引用计数
    };

    struct InodeResult {
        struct LsfInode* inode;         // 文件/目录的inode
        bool found;
    };
 // 使用RAII方式管理inode内存
    struct InodeGuard {
        LsfInode* inode;
        InodeGuard(LsfInode* i) : inode(i) {}
        ~InodeGuard() { if(inode) free(inode); }

		InodeGuard(const InodeGuard&) = delete;
        InodeGuard& operator=(const InodeGuard&) = delete;
    };


    static __always_inline void lsf_add_inode_ref(struct LsfInode *n)
 	{   
        ++n->ref_count;
	//	n->ctime = time(NULL);
    }

    static __always_inline void lsf_dec_inode_ref(struct LsfInode *n)
    {
        if (--n->ref_count == 0)
        {
            free(n);
        } 		
    }

    /*
    const char *get_filename(const char *path) {
        const char *filename = strrchr(path, '/');
        if (filename == NULL) {
            return path; // 没有斜杠，整个就是文件名
        } else {
            return filename + 1; // 跳过斜杠，指向文件名
        }
    }
    */

    static __always_inline struct LsfInode *lsf_calloc_inode()
    {
        struct LsfInode *lsn = (struct LsfInode *)calloc(1, sizeof(struct LsfInode));
        lsn->ref_count = 1;
        return lsn;
    }


	bool is_valid_uid(uid_t uid);
	bool is_valid_gid(gid_t gid);

    struct LsfInode *getInode(const char *path, struct LsfContext *ctx);
    struct LsfInode *getInodeFromParentIno(const char *file_name, uint64_t get_parent_inode_num, struct LsfContext *ctx);
    std::shared_ptr<LsfDentry> getDentry(fuse_ino_t parent, const char *name, bool ifCheckDisk);
    std::shared_ptr<LsfDentry> getDentryByIno(fuse_ino_t ino);
    std::shared_ptr<LsfDentry> cacheAdd(struct LsfInode *new_inode, uint64_t parent_ino, const std::string& name);
    void eraseOldDentry(const LsfDentry::Ptr& dentry);
    static void lsmfs_init(void *userdata, struct fuse_conn_info *conn);
    static void *lsmfs_getDBStats(rocksdb::DB *db, struct LsfContext *lsf_ctx);
    static void lsmfs_lookup (fuse_req_t req, fuse_ino_t parent, const char *name);
    static void lsmfs_getattr(fuse_req_t req, fuse_ino_t ino,struct fuse_file_info *fi);
    static void lsmfs_forget (fuse_req_t req, fuse_ino_t ino, uint64_t nlookup);
	static void lsmfs_opendir(fuse_req_t req, fuse_ino_t ino,struct fuse_file_info *fi);
    static void lsmfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t offset,struct fuse_file_info *fi);
    static void lsmfs_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
    static void lsmfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,mode_t mode);
    static void lsmfs_create(fuse_req_t req, fuse_ino_t parent, const char *name,mode_t mode, struct fuse_file_info *fi);
    static void lsmfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,mode_t mode, dev_t rdev);
    static void lsmfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,int to_set, struct fuse_file_info *fi);
    static void lsmfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
    static void lsmfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t offset, fuse_file_info *fi);
    static void lsmfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t offset, struct fuse_file_info *fi);
    static void lsmfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name);
    static void lsmfs_rmdir (fuse_req_t req, fuse_ino_t parent, const char *name);
    //static int lsmfs_utimens(const char *path, const struct timespec ts[2],struct fuse_file_info *fi);
    static void lsmfs_release(fuse_req_t req, fuse_ino_t ino,struct fuse_file_info *fi);
    static void lsmfs_destroy(void *private_data);
    static void lsmfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi);
    static int my_fsync(const char *path, struct fuse_file_info *fi,struct LsfContext *lsf_ctx, struct LsfInode *inode);
	static void lsmfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname, unsigned int flags);


	static void lsmfs_flush(fuse_req_t req, fuse_ino_t ino,struct fuse_file_info *fi);
    static void lsmfs_fallocate(fuse_req_t req, fuse_ino_t ino, int mode, off_t offset, off_t length, struct fuse_file_info *fi);

	static void lsmfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,const char *newname);
	static void lsmfs_symlink(fuse_req_t req, const char *link, fuse_ino_t parent, const char *name);
	static void lsmfs_readlink(fuse_req_t req, fuse_ino_t ino);
    
    void init_lsmfs_oper(fuse_lowlevel_ops &lsmfs_oper);
    extern struct fuse_operations lsmfs_oper;

    // 辅助函数：分割路径
   // std::vector<std::string> split_path(const std::string &path);
    // 辅助函数：从路径获取文件名
    std::string get_filename_from_path(const std::string& path);
    // 辅助函数：格式化字符串
    std::string format_string(const char *format, ...);

#ifdef __cplusplus
}
#endif
#endif
