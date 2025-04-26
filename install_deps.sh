#!/usr/bin/env bash

sudo apt install -y curl cmake build-essential autoconf libtool liburing-dev libyaml-cpp-dev pkg-config

export MY_INSTALL_DIR=$HOME/.local
mkdir -p $MY_INSTALL_DIR
export PATH="$MY_INSTALL_DIR/bin:$PATH"

grpc_tmp_dir="/tmp/grpc"
git clone --recurse-submodules -b v1.46.0 --depth 1 --shallow-submodules https://github.com/grpc/grpc ${grpc_tmp_dir}
pushd ${grpc_tmp_dir}
mkdir -p cmake/build
cd cmake/build
cmake -DgRPC_INSTALL=ON \
      -DgRPC_BUILD_TESTS=OFF \
      -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR \
      ../..
make -j32
make install
popd

# install rocksdb
rocks_tmp_dir="/tmp/rocksdb"
git clone https://github.com/facebook/rocksdb.git -b v8.1.1 ${rocks_tmp_dir}
pushd ${rocks_tmp_dir}
make static_lib ldb -j32
sudo make install
popd

# install libnvme
libnvme_tmp_dir="/tmp/libnvme"
git clone https://github.com/linux-nvme/libnvme.git --depth 1 -b v1.11 ${libnvme_tmp_dir}
pushd ${libnvme_tmp_dir}
meson setup .build
meson compile -C .build
sudo meson install -C .build
popd
