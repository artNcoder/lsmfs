#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/convenience.h>
#include <rocksdb/table.h>
#include <rocksdb/slice_transform.h>
#include <cstring>
#include <string>
#include <iostream>
#include "lsf.h"
#include "lsf_i.h"
#include "lsf_utils.h"
#include <assert.h>

int lsf_mkfs(const char *db_path);
