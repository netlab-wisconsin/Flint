cmake_minimum_required(VERSION 3.12)
project(Flint CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -march=native -Wno-format-security")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O2")
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

find_package(RocksDB QUIET)
if(NOT RocksDB_FOUND)
  message(STATUS "RocksDB not found using find_package, trying pkg-config...")
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(ROCKSDB REQUIRED rocksdb)

  if(NOT ROCKSDB_FOUND)
    message(FATAL_ERROR "RocksDB not found")
  endif()
endif()

message(STATUS "RPC implementation: gRPC")

# Find protoc
if(CMAKE_CROSSCOMPILING)
  find_program(PROTOBUF_PROTOC protoc)
else()
  set(PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
endif()

# Find grpc dependency
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

# Find grpc cpp plugin
if(CMAKE_CROSSCOMPILING)
  find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else()
  set(GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()

get_filename_component(proto_message "src/protos/message.proto" ABSOLUTE)
get_filename_component(proto_service "src/protos/service.proto" ABSOLUTE)
set(proto_files ${proto_message} ${proto_service})
get_filename_component(proto_dir "src/protos" ABSOLUTE)
foreach(proto ${proto_files})
  get_filename_component(proto_name ${proto} NAME_WE)

  set(proto_srcs "${CMAKE_CURRENT_LIST_DIR}/src/common/grpc/${proto_name}.pb.cc")
  set(proto_hdrs "${CMAKE_CURRENT_LIST_DIR}/src/common/grpc/${proto_name}.pb.h")
  set(grpc_srcs "${CMAKE_CURRENT_LIST_DIR}/src/common/grpc/${proto_name}.grpc.pb.cc")
  set(grpc_hdrs "${CMAKE_CURRENT_LIST_DIR}/src/common/grpc/${proto_name}.grpc.pb.h")

  add_custom_command(
    OUTPUT "${proto_srcs}" "${proto_hdrs}" "${grpc_srcs}" "${grpc_hdrs}"
    COMMAND ${PROTOBUF_PROTOC}
    ARGS --grpc_out "${CMAKE_CURRENT_LIST_DIR}/src/common/grpc"
    --cpp_out "${CMAKE_CURRENT_LIST_DIR}/src/common/grpc"
    -I "${proto_dir}"
    --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN_EXECUTABLE}"
    "${proto}"
    DEPENDS "${proto}"
  )

  list(APPEND protobuf_generated_srcs ${proto_srcs} ${grpc_srcs})
endforeach()

add_library(grpc_proto SHARED ${protobuf_generated_srcs})
target_link_libraries(grpc_proto gRPC::grpc++ protobuf::libprotobuf)

set(COMMON_SRCS
  src/common/logging.cc
  src/common/ssd.cc
  src/common/io_defs.cc
  src/common/shadow_view.cc
)

set(FLINT_LIB flint)
set(FLINT_SOURCES
  ${COMMON_SRCS}
  src/client/global.cc
  src/client/local_scheduler.cc
  src/client/view_agent.cc
  src/client/volume_agent.cc
  src/client/volume.cc
  src/client/io_executor.cc
)

add_library(${FLINT_LIB} SHARED ${FLINT_SOURCES})
target_include_directories(${FLINT_LIB} PUBLIC src)
target_link_libraries(${FLINT_LIB} PUBLIC grpc_proto numa uring nvme)

set(ARBITER_LIB flint_arbiter)
set(ARBITER_SOURCES
  ${COMMON_SRCS}
  src/arbiter/internal.cc
  src/arbiter/scheduler.cc
  src/arbiter/ssd_manager.cc
  src/arbiter/db.cc
  src/arbiter/ext_allocator.cc
  src/arbiter/volume_meta_manager.cc
  src/arbiter/volume_service.cc
  src/arbiter/view_controller.cc
  src/arbiter/scheduler_service.cc
)

add_library(${ARBITER_LIB} SHARED ${ARBITER_SOURCES})
target_include_directories(${ARBITER_LIB} PUBLIC src)
target_link_libraries(${ARBITER_LIB} grpc_proto yaml-cpp nvme uring rocksdb)

add_executable(arbiter src/arbiter/main.cc)
target_link_libraries(arbiter ${ARBITER_LIB})

add_subdirectory(tools)
