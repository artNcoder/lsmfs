#pragma once

#define FUSE_USE_VERSION 31
#include "lsf_log.h"
#define LSF_MAGIC "lsmfs_0"
#define LSF_VER 0x00010000
#define LSF_ROOT_INUM 1
#define LSF_FIRST_USER_INUM 16
#define BLOCK_SIZE (64<<10)
