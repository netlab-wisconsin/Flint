#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include "common/io_defs.h"

constexpr static int kSizeDistGranularity = 10;

namespace flint {

/// IO size distribution buckets
/// 0-4kB
/// 4-8kB
/// 8-16kB
/// 16-32kB
/// 32-64kB
/// 64-128kB
/// 128-256kB
/// 256-512kB
/// 512-1MB
/// 1-2MB

class PortView {
 public:
  int port_id = -1;
  float read_bw = 0.0;
  float write_bw = 0.0;
  float read_iops = 0.0;
  float write_iops = 0.0;
  uint32_t read_size_dist[kSizeDistGranularity];
  uint32_t write_size_dist[kSizeDistGranularity];
  uint64_t recency_counter = 0;

  PortView() = default;

  PortView(int port_id) : port_id(port_id) {}

  void RecordIO(size_t size, IOType type);

 private:
  long last_update_tp_ = 0;
  size_t windowed_read_bytes_ = 0;
  size_t windowed_write_bytes_ = 0;
};

class NetPortView : public PortView {
 public:
  NetPortView() = default;

  NetPortView(int net_port_id) : PortView(net_port_id) {}
};

class IOPortView : public PortView {
 public:
  IOPortView() = default;

  IOPortView(int io_port_id) : PortView(io_port_id) {}
};

class PipeView {
 public:
  int pipe_id = -1;
  float read_bw = 0.0;
  float write_bw = 0.0;
  float read_iops = 0.0;
  float write_iops = 0.0;
  uint64_t recency_counter = 0;

  PipeView() = default;

  PipeView(int pipe_id) : pipe_id(pipe_id) {}

  void RecordIO(size_t size, IOType type);

 private:
  long last_update_tp_ = 0;
  size_t windowed_read_bytes_ = 0;
  size_t windowed_write_bytes_ = 0;
};

class NetPipeView : public PipeView {
 public:
  NetPipeView() = default;

  NetPipeView(int net_pipe_id) : PipeView(net_pipe_id) {}
};

class IOPipeView : public PipeView {
 public:
  IOPipeView() = default;

  IOPipeView(int io_pipe_id) : PipeView(io_pipe_id) {}
};

class SsdView {
 public:
  int ssd_id;
  float read_bw_used = 0.0;
  float write_bw_used = 0.0;
  float read_bw_free = 0.0;
  float write_bw_free = 0.0;

  float frag_degree = 1.0;
  float write_cost = 3.4;

  SsdView() = default;

  SsdView(int ssd_id) : ssd_id(ssd_id) {}

  uint64_t recency_counter = 0;

  void RecordIO(size_t size, IOType type, int latency);

  size_t AvailBw() const { return kMaxBwIdeal * frag_degree; }

 private:
  long last_update_tp_ = 0;
  size_t windowed_read_bytes_ = 0;
  size_t windowed_write_bytes_ = 0;
  size_t write_lat_;

  static constexpr int kWriteLatMax = 2000;
  static constexpr int kWriteLatMin = 300;
  static constexpr size_t kMaxBwIdeal = 2200000000;
  static constexpr float kWcBaseline = 3.4;
  static constexpr float kWcDescfactor = 0.5;
};

struct ShadowView {
  std::map<int, NetPortView> net_port_views;
  std::map<int, NetPipeView> net_pipe_views;
  std::map<int, IOPortView> io_port_views;
  std::map<int, IOPipeView> io_pipe_views;
  std::map<int, SsdView> ssd_views;
};

struct PartialShadowView {
  NetPortView net_port_view;
  NetPipeView net_pipe_view;
  std::map<int, IOPortView> io_port_views;
  std::map<int, IOPipeView> io_pipe_views;
  std::map<int, SsdView> ssd_views;
};

struct NvmeofSession {
  std::string hostname;
  int ebof_port;
  int target_ssd_id;
  int client_id;
};
};  // namespace flint
