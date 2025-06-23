#!/bin/bash

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd -P)
BUILD_DIR=${SCRIPT_DIR}/build
if [[ ! -d ${BUILD_DIR} ]]; then
	mkdir ${BUILD_DIR}
fi

cd ${BUILD_DIR}
rm -rf *
cmake ..
make -j16

