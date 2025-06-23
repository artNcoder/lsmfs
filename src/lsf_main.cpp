#define FUSE_USE_VERSION 31

#include <fuse3/fuse_lowlevel.h>
#include <fuse3/fuse_opt.h>
#include <rocksdb/db.h>
#include <cstring>
#include <string>
#include <iostream>
#include "lsf.h"
#include "lsf_i.h"
#include "lsf_utils.h"
#include <assert.h>
#include "lsf_mkfs.h"
#include "lsf_log.h"

using namespace LSF;
int main(int argc, char *argv[])
{
    // 设置日志级别
    Logger::getInstance()->setLogLevel(Logger::DEBUG);
    // 开启日志
 //    Logger::getInstance()->setLoggingEnabled(false);
    Logger::getInstance()->setLoggingEnabled(true);

    /*
        options.create_if_missing = true;
        rocksdb::Status status = rocksdb::DB::Open(options, "mydb", &db);
        if (!status.ok()) {
            std::cerr << "Error opening RocksDB: " << status.ToString() << std::endl;
            return -1;
        }
    */
    // 检查参数
    if (argc < 3)
    {
        std::cerr << "Usage: " << argv[0] << " <mount_point> <db_path>" << std::endl;
        return 1;
    }

    // 获取数据库路径参数
    const char *mount_point = argv[1];
    const char *db_path = argv[2];

    // 获取数据库绝对路径
    char abs_db_path[PATH_MAX + 1];

    if (db_path[0] != '/')
    {
        char cwd[PATH_MAX];
        if (getcwd(cwd, sizeof(cwd)) != nullptr)
        {
            snprintf(abs_db_path, sizeof(abs_db_path), "%s/%s", cwd, db_path);
        }
        else
        {
            std::cerr << "Failed to get current directory" << std::endl;
            return 1;
        }
    }
    else
    {
        strncpy(abs_db_path, db_path, PATH_MAX - 1);
    }

    // 获取绑定文件夹绝对路径
    char abs_mount_path[PATH_MAX + 1];

    if (mount_point[0] != '/')
    {
        char cwd_1[PATH_MAX];
        if (getcwd(cwd_1, sizeof(cwd_1)) != nullptr)
        {
            snprintf(abs_mount_path, sizeof(abs_mount_path), "%s/%s", cwd_1, mount_point);
        }
        else
        {
            std::cerr << "Failed to get current directory" << std::endl;
            return 1;
        }
    }
    else
    {
        strncpy(abs_mount_path, mount_point, PATH_MAX - 1);
    }

    LOG_INFOF("Using database path: %s", abs_db_path);

    // 为了让FUSE正常工作，需要修改参数
    // 创建新的参数数组
    char *fuse_argv[3];
    fuse_argv[0] = argv[0];
//    fuse_argv[3] = argv[1];
    fuse_argv[1] = strdup("-o");
    fuse_argv[2] = strdup("allow_other,default_permissions");


    int fuse_argc = 3;

    LOG_INFO("LSF started");

    LsfContext *ctx = LsfContext::getInstance();
    ctx->db_path = abs_db_path;
    ctx->mount_point = abs_mount_path;
    //    lsf_mkfs(abs_db_path); // mkfs用于创建文件系统的初始化
    LOG_INFO("lsmfs_oper start");
    //    LsfContext *ctx = init_context("mydb");      // init_context用于每次mount时进行初始化设置

//    fuse_operations lsmfs_oper;
    struct fuse_lowlevel_ops lsmfs_oper = {0};
    init_lsmfs_oper(lsmfs_oper);
    LOG_INFO("lsmfs_oper end");

    //	std::string stats;
    //	if (ctx->db->GetProperty("rocksdb.stats", &stats)) {
    //       LOG_INFOF("RocksDB Stats:\n%s", stats.c_str());
    //    }
// <<<<<<<<<<<<<<<<<
//    int ret = fuse_main(fuse_argc, fuse_argv, &lsmfs_oper, nullptr);
// >>>>>>>>>>>>>>>>>>
    struct fuse_args args = FUSE_ARGS_INIT(fuse_argc, fuse_argv);
    struct fuse_session *se = fuse_session_new(&args, &lsmfs_oper, sizeof(lsmfs_oper), nullptr);
    if (!se) {
        LOG_ERROR("Failed to create FUSE session");
        exit(1);
    }
    // struct LsfContext *lsf_ctx = LsfContext::getInstance();
    // lsf_ctx->se = se;
    if (fuse_session_mount(se, abs_mount_path) != 0) {
        LOG_ERROR("Failed to mount FUSE filesystem");
        fuse_session_destroy(se);
        exit(1);
    }
    // 显式转为守护进程（确保在挂载成功后调用）
    daemon(0, 0); // 参数：nochdir=0（切换到根目录），noclose=0（关闭标准流）
   // fuse_session_loop_mt(se, -1);
    fuse_session_loop(se);
    
    // 清理资源
    fuse_session_unmount(se);
    fuse_session_destroy(se);
    fuse_opt_free_args(&args);
    

    // delete db;
    return 0;
}
