#include "lsf_i.h"
#include <thread>
using namespace nlohmann;
using namespace LSF;

// 在cpp文件中定义静态成员
LsfContext *LsfContext::instance = nullptr;
std::mutex LsfContext::mutex;

void to_json(json &j, const LsfSuperBlock &sb)
{
	j = json{
		{"magic_str", sb.magic_str},
		{"version", sb.version}};
}
void from_json(const json &j, LsfSuperBlock &sb)
{
	j.at("magic_str").get_to(sb.magic_str);
	j.at("version").get_to(sb.version);
}

void deserialize_superblock(const char *buf, LsfSuperBlock &sb)
{
	json j = json::parse(buf);
	j.get_to(sb);
}

std::string serialize_superblock(const LsfSuperBlock &sb)
{
	json jsb = sb;
	return jsb.dump();
}

/*
	"读取"元数据来加载文件系统

*/

void init_context(const char *db_path, LsfContext *ctx)
{
	/*
		char buffer[PATH_MAX];
		if (getcwd(buffer, sizeof(buffer)) != nullptr) {
			LOG_DEBUGF("Current path:  %s ",buffer);
		}
	*/
	if (ctx == nullptr)
	{
		LOG_ERROR("ctx is null");
		return;
	}
	bool db_exists = (access(db_path, F_OK) == 0);
	if (!db_exists)
	{
		LOG_ERRORF("Database %s not exists, exiting...", db_path);
		return;
	}
	else
	{
		LOG_INFOF("Database %s exists, mounting...", db_path);
	}

	LOG_DEBUG("init context start");

	static_assert(sizeof(struct LsfInode) == LSF_INODE_SIZE, "LsfInode size error");

	// LsfContext *ctx = new LsfContext();
	rocksdb::Options options;

	// 创建并配置 statistics
	//    ctx->statistics = rocksdb::CreateDBStatistics();
	//    options.statistics = ctx->statistics;
	//	options.statistics = rocksdb::CreateDBStatistics();

	// options.stats_dump_period_sec = 60;  // 每10分钟导出统计信息

	setup_db_options(options);
        
	rocksdb::TransactionDBOptions tx_opt;
	// open DB with two column families
	std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
	// have to open default column family
	column_families.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
	rocksdb::ColumnFamilyOptions meta_cf_opt;
	setup_meta_cf_options(meta_cf_opt);
	// open the new one, too
	column_families.push_back(rocksdb::ColumnFamilyDescriptor("meta_cf", meta_cf_opt));
	rocksdb::ColumnFamilyOptions data_cf_opt;
	setup_data_cf_options(data_cf_opt);

	// data_cf_opt.merge_operator.reset(new ViveDataMergeOperator());
	data_cf_opt.merge_operator.reset(new FuseMergeOperator());
	column_families.push_back(rocksdb::ColumnFamilyDescriptor("data_cf", data_cf_opt));

	std::vector<rocksdb::ColumnFamilyHandle *> handles;
	ctx->db_path = db_path;
	// 禁用WAL自动刷新
	options.manual_wal_flush = true;
        options.allow_mmap_writes = false;

	// 启用禁用压缩
	// options.compression = rocksdb::kNoCompression;

	LOG_DEBUGF("db_path = %s", db_path);
	LOG_DEBUGF("ctx->db_path.c_str() = %s", ctx->db_path.c_str());

	rocksdb::Status s = rocksdb::TransactionDB::Open(options, tx_opt, ctx->db_path.c_str(), column_families, &handles, &ctx->db);
	if (!s.ok())
	{
		LOG_ERRORF("Failed open db: %s, %s\n", ctx->db_path.c_str(), s.ToString().c_str());
		//	delete ctx;
		//	std::cerr << "Failed open db:" << ctx->db_path << ", " << s.ToString() << std::endl;
		return;
	}
	else
	{
		LOG_INFOF("Succeed open db:%s, ptr %p", ctx->db_path.c_str(), ctx->db);
	}
	/*
		LOG_DEBUG("init context getproperty start");

		std::string stats;
		if (ctx->db->GetProperty("rocksdb.stats", &stats)) {
		   LOG_DEBUGF("RocksDB Stats:\n%s", stats.c_str());
		}
		LOG_DEBUG("init context getproperty end");
	*/
	ctx->default_cf = handles[0];
	ctx->meta_cf = handles[1];
	ctx->data_cf = handles[2];

	ctx->meta_opt.sync = false;
	ctx->meta_opt.disableWAL = true;
        ctx->data_opt.disableWAL = true;
	// not work, data still lost, it's problem of merge
	// ctx->data_opt.sync = true; //to test auto flush

	memset(&ctx->root_inode, 0, sizeof(ctx->root_inode));
	ctx->root_inode.inode_num = LSF_ROOT_INUM;
	ctx->root_inode.parent_dir_inode_num = LSF_ROOT_INUM;
	ctx->root_inode.mode = S_IFDIR | 00777; // NOTE: S_IFDIR  0040000 is defined in octet, not hex number
	ctx->root_inode.links_count = 2;
	ctx->root_inode.file_size = 4096;
	ctx->root_inode.atime = ctx->root_inode.ctime = ctx->root_inode.mtime = time(NULL);
	ctx->root_inode.ref_count = 1;
	ctx->root_inode.uid = getuid();
	ctx->root_inode.gid = getgid();

	std::string v;
	s = ctx->db->Get(ctx->read_opt, ctx->meta_cf, INODE_SEED_KEY, &v);
	if (!s.ok())
	{
		return;
	}
	ctx->inode_seed = deserialize_int64(v.data());
	s = ctx->db->Get(ctx->read_opt, ctx->meta_cf, "lsf_sb", &v);
	if (!s.ok())
	{
		return;
	}

	LsfSuperBlock sb;
	deserialize_superblock(v.data(), sb);
	if (sb.magic_str != LSF_MAGIC)
	{
		LOG_ERROR("Not a LSF file system");
		return;
	}
	if (sb.version != LSF_VER)
	{
		LOG_ERROR("LSF version not match");
		return;
	}

	LsfFile *f = new LsfFile;
	if (f == NULL)
	{
		LOG_ERROR("Failed alloc memory for ViveFile");
		return;
	}
	f->inode_num = LSF_ROOT_INUM;
	lsf_add_inode_ref(&ctx->root_inode);
	f->inode = &ctx->root_inode;
	f->file_name = "/";
	ctx->root_file.reset(f);
	//	_lsf_c.cancel_all();
	//	LOG_INFO("Mount ViveFS:%s succeed", db_path);
	// lsf_ctx = ctx;
	/*
		// 启动后台线程，定期打印统计信息
		std::thread([&ctx]() {
			while (true) {
				std::this_thread::sleep_for(std::chrono::seconds(10));  // 每 10 秒打印一次
				if (ctx->statistics) {
	//                std::cout << "RocksDB Statistics:" << std::endl;
					LOG_DEBUGF("RocksDB Statistics: ",ctx->statistics->ToString());
				}
			}
		}).detach();  // 将线程设置为后台线程
	*/

	//	std::string stats = ctx->statistics->ToString();
	//	LOG_DEBUGF("mystats: %s\n", stats.c_str());
	//       LOG_INFOF("rocksdb.num_levels : %d", options.num_levels);
	/*
		std::string stats;
		if (ctx->db->GetProperty("rocksdb.stats", &stats))
		{
			LOG_INFOF("mystatss: %s\n", stats.c_str());
		}
	*/
	LOG_DEBUG("init context end");
	return;
}

LsfContext::LsfContext() : is_stack_object_(true), db(NULL), default_cf(NULL), meta_cf(NULL), data_cf(NULL)
{
	if (instance == nullptr)
	{
		instance = this;
	}

	meta_opt = rocksdb::WriteOptions();
	meta_opt.sync = false;
        meta_opt.disableWAL=true;
	data_opt = rocksdb::WriteOptions();
	data_opt.sync = false;
        data_opt.disableWAL=true;
	read_opt = rocksdb::ReadOptions();
	mount_time = time(nullptr);
}

LsfContext *LsfContext::getInstance()
{
	std::lock_guard<std::mutex> lock(mutex);
	if (instance == nullptr)
	{
		instance = new LsfContext();
		instance->is_stack_object_ = false;
	}
	return instance;
}

int64_t LsfContext::generate_inode_num()
{
	return inode_seed++;
}

std::vector<std::string> split_path(const std::string &path)
{
	std::vector<std::string> components;
	std::string::size_type start = 0;
	std::string::size_type end = 0;

	while ((end = path.find('/', start)) != std::string::npos)
	{
		if (end != start)
		{
			components.push_back(path.substr(start, end - start));
		}
		start = end + 1;
	}

	if (start < path.length())
	{
		components.push_back(path.substr(start));
	}

	return components;
}

uint64_t get_parent_inode_num(LsfContext *ctx, const std::string &path)
{
	//LOG_INFO("get_parent_inode_num start");
	if (path == "/" || path.empty())
	{
		//		LOG_INFO("get_parent_inode_num end, its root inode");

		return LSF_ROOT_INUM; // 根目录的 inode number
	}

	//        uint64_t cached_inode;

	std::vector<std::string> components = split_path(path);
	uint64_t current_inode = LSF_ROOT_INUM;

	// 遍历路径组件，直到倒数第二个（最后一个是文件名或当前目录名）
	for (size_t i = 0; i < components.size() - 1; ++i)
	{
		const std::string &component = components[i];

		// 构造 key 来查找子目录的 inode
		std::string key = std::to_string(current_inode) + "_" + component;
		LOG_DEBUGF("key = %s\n", key.c_str());
		/*
					  if (ctx->path_cache.get(key, cached_inode)) {
							  current_inode = cached_inode;
							  continue;
					  }
	  */
		rocksdb::Slice file_key = key;
		rocksdb::PinnableSlice inode_num_buf;
		rocksdb::Status s = ctx->db->Get(ctx->read_opt, ctx->meta_cf, file_key, &inode_num_buf);

		if (!s.ok())
		{
			// 目录不存在
			LOG_ERROR("DIR not exist");
			return -1;
		}

		// 更新当前 inode 为找到的子目录的 inode
		memcpy(&current_inode, inode_num_buf.data(), sizeof(uint64_t));
		//              ctx->path_cache.put(key, current_inode);
	}
		LOG_DEBUGF("get_parent_inode_num end, parent_inode_num = %d\n", current_inode);
	return current_inode;
}

void free_context(LsfContext *ctx)
{
	if (ctx)
	{
		delete ctx->db;
		delete ctx;
	}
}
