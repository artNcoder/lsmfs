CMAKE_MINIMUM_REQUIRED(VERSION 3.0.0)

PROJECT(myfs VERSION 0.1.0)

SET(CMAKE_BUILD_TYPE "Debug")
SET(EXECUTABLE_NAME "lsmfs")
SET(SOURCE_FILE_NAME "lsf_main.cpp")
#设置C++标准
SET(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

#添加编译选项
#add_compile_definitions(_FILE_OFFSET_BITS=64)

find_package(PkgConfig REQUIRED)
pkg_check_modules(FUSE REQUIRED fuse3)
pkg_check_modules(LIBUV REQUIRED libuv)
find_library(LZ4_LIB lz4 REQUIRED)
if (NOT LZ4_LIB)
  message(FATAL_ERROR "lz4 library not found, please install liblz4-dev")
endif()

include_directories(${FUSE_INCLUDE_DIRS})

SET(ROCKSDB_DIR "${PROJECT_SOURCE_DIR}/../rocksdb")

INCLUDE_DIRECTORIES("${ROCKSDB_DIR}/include")
INCLUDE_DIRECTORIES("/usr/local/include")
INCLUDE_DIRECTORIES("${PROJECT_SOURCE_DIR}/include")
include_directories(${LIBUV_INCLUDE_DIRS})
link_directories(${LIBUV_LIBRARY_DIRS})

add_custom_target(BUILD_ROCKSDB
        COMMAND make -j32 shared_lib USE_RTTI=1 USE_LZ4=1 -C ${ROCKSDB_DIR}
#        COMMAND make -j32 all -C ${ROCKSDB_DIR}

        COMMENT "Building RocksDB"
)

SET(PROJECT_SOURCES 
	${PROJECT_SOURCE_DIR}/src/${SOURCE_FILE_NAME}
	${PROJECT_SOURCE_DIR}/src/lsf_operations.cpp
	${PROJECT_SOURCE_DIR}/src/lsf_i.cpp
	${PROJECT_SOURCE_DIR}/src/lsf_mkfs.cpp
	${PROJECT_SOURCE_DIR}/src/lsf_env.cpp
	${PROJECT_SOURCE_DIR}/src/lsf_log.cpp
	${PROJECT_SOURCE_DIR}/src/lsf_data_merge.cpp
    ${PROJECT_SOURCE_DIR}/src/lsf_dentry.cpp
	${PROJECT_SOURCE_DIR}/src/lsf_profiler.cpp
	)

ADD_EXECUTABLE(${EXECUTABLE_NAME} ${PROJECT_SOURCES})
ADD_DEPENDENCIES(${EXECUTABLE_NAME} BUILD_ROCKSDB)
FIND_LIBRARY(LIB_GFLAGS gflags)
FIND_LIBRARY(LIB_DL dl)


if(NOT LIB_GFLAGS)
    message(FATAL_ERROR "gflags library not found")
endif()

if(NOT LIB_DL)
    message(FATAL_ERROR "dl library not found")
endif()

TARGET_LINK_LIBRARIES(${EXECUTABLE_NAME} PRIVATE ${ROCKSDB_DIR}/librocksdb.so ${LZ4_LIB} ${LIB_DL} ${LIB_GFLAGS} pthread ${FUSE_LIBRARIES} ${LIBUV_LIBRARIES} -lstdc++fs)

#添加编译选项
TARGET_COMPILE_DEFINITIONS(${EXECUTABLE_NAME} PRIVATE -D_FILE_OFFSET_BITS=64)
