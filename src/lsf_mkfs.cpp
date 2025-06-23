#include "lsf_mkfs.h"
#include <rocksdb/options.h>

using namespace ROCKSDB_NAMESPACE;
using namespace LSF;
using ROCKSDB_NAMESPACE::ConfigOptions;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Env;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

/*
	mkfs用于创建一个全新的文件系统，为文件系统创建一个干净可用的结构。
	"写入"元数据为主
*/

int lsf_mkfs(const char *db_path)
{
	/*
		char buffer[PATH_MAX];
		if (getcwd(buffer, sizeof(buffer)) != nullptr) {
			LOG_DEBUGF("Current path:  %s ",buffer);
		}
	*/

	LOG_DEBUG("mkfs start");
	Options options;
	ConfigOptions config_options;
	setup_db_options(options);

	options.create_if_missing = true;
	// open DB
	TransactionDBOptions tx_opt;
	LsfContext ctx;

	// 先删除已存在的数据库
	Status s = DestroyDB(db_path, options);
	if (!s.ok())
	{
		LOG_ERRORF("Failed to destroy existing database: %s", s.ToString().c_str());
		// 继续执行，因为数据库可能不存在
	}

	LOG_DEBUG("TransactionDB::Open start");
	s = TransactionDB::Open(options, tx_opt, db_path, &ctx.db);
	if (!s.ok())
	{
		LOG_ERRORF("Failed to open database:%s, %s", db_path, s.ToString().c_str());
		return -s.code();
	}

	ColumnFamilyOptions meta_opts, data_opts;

	setup_meta_cf_options(meta_opts);
	setup_data_cf_options(data_opts);

	LOG_DEBUG("CreateColumnFamily meta_opts start");
	s = ctx.db->CreateColumnFamily(meta_opts, "meta_cf", &ctx.meta_cf);
	if (!s.ok())
	{
		LOG_ERRORF("Failed to create CF meta_cf, %s", s.ToString().c_str());
		return -s.code();
	}

	LOG_DEBUG("CreateColumnFamily data_opts start");
	s = ctx.db->CreateColumnFamily(data_opts, "data_cf", &ctx.data_cf);
	if (!s.ok())
	{
		LOG_ERRORF("Failed to create CF data_cf, %s", s.ToString().c_str());
		return -s.code();
	}

	LOG_DEBUG("BeginTransaction");
	bool committed = false;
        ctx.meta_opt.disableWAL = true;
	Transaction *tx = ctx.db->BeginTransaction(ctx.meta_opt);

	if (!tx)
	{
		LOG_ERROR("Failed to begin transaction");
		return -1;
	}
	std::unique_ptr<Transaction> tx_guard(tx);

	/*
			DeferCall _2([tx]() {delete tx; });
			Cleaner _c;
			_c.push_back([tx]() {tx->Rollback(); });
	*/
	try
	{
		ctx.inode_seed = LSF_FIRST_USER_INUM;
		LsfSuperBlock sb = {LSF_MAGIC, LSF_VER};

		if (!ctx.meta_cf || !ctx.data_cf)
		{
			LOG_ERROR("Column families not properly initialized");
			return -1;
		}

		// DO_FUNC(s = tx->Put(ctx.meta_cf, "lsf_sb", serialize_superblock(sb)));
		s = tx->Put(ctx.meta_cf, "lsf_sb", serialize_superblock(sb));
		if (!s.ok())
		{
			throw std::runtime_error("Failed to put lsf_sb: " + s.ToString());
		}
		// DO_FUNC(s = tx->Put(ctx.meta_cf, INODE_SEED_KEY, Slice((char*)&ctx.inode_seed, sizeof(ctx.inode_seed))));
		s = tx->Put(ctx.meta_cf, INODE_SEED_KEY, Slice((char *)&ctx.inode_seed, sizeof(ctx.inode_seed)));
		if (!s.ok())
		{
			throw std::runtime_error("Failed to put INODE_SEED_KEY: " + s.ToString());
		}

		LOG_DEBUG("Before commit, checking transaction state");
		if (!tx)
		{
			LOG_ERROR("Transaction pointer is null");
			return -1;
		}
		// 验证数据
		std::string value;
		s = tx->Get(ReadOptions(), ctx.meta_cf, "lsf_sb", &value);
		if (!s.ok())
		{
			LOG_ERROR("Failed to verify data before commit");
			return -s.code();
		}
		LOG_DEBUGF("lsf_sb value = %s", value.c_str());
		LOG_DEBUG("commit meta_cf start");
		s = tx->Commit();
		if (!s.ok())
		{
			throw std::runtime_error("Failed to commit transaction: " + s.ToString());
		}
		committed = true;
		LOG_DEBUG("commit meta_cf end");
	}
	catch (const std::exception &e)
	{
		tx->Rollback();
		LOG_ERRORF("Transaction failed: %s. \n", e.what());
	}
	std::vector<ColumnFamilyHandle *> cfh;

	cfh.push_back(ctx.meta_cf);
	cfh.push_back(ctx.data_cf);

	PinnableSlice v;

	LOG_DEBUG("get ctx.meta_cf");
	s = ctx.db->Get(ReadOptions(), ctx.meta_cf, INODE_SEED_KEY, &v);
	if (!s.ok())
	{
		LOG_DEBUGF("Failed GET key: %s, %s\n", INODE_SEED_KEY, s.ToString());
		// LOG_ERRORF(); << "Failed GET key:" << INODE_SEED_KEY << ", " << s.ToString() << std::endl;
	}
	else
	{
		LOG_DEBUGF("Succeed get key:%s\n", INODE_SEED_KEY);
	}
	bool db_exists = (access(db_path, F_OK) == 0);
	if (!db_exists)
	{
		LOG_ERRORF("Database %s not exists, exiting...", db_path);
		return -1;
	}
	else
	{
		LOG_INFO("Database exists, mounting...");
	}

	ctx.db->Flush(FlushOptions(), cfh);

	//	_c.cancel_all();
	//	delete ctx.db;

	LOG_DEBUG("mkfs end");

	return 0;
}
