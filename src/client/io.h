#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include "common/io_defs.h"
#include "common/ssd.h"
#include "internal.h"

namespace flint {
class IOUnit;
class AsyncIOCb;

using IOUnitPtr = std::shared_ptr<IOUnit>;
using AsyncIOCbPtr = std::shared_ptr<AsyncIOCb>;

class IOUnit {
 public:
  Ssd* ssd = nullptr;
  char* user_buf = nullptr;
  uint32_t len = 0;
  uint64_t paddr = 0;
  IOType io_type;
  ChainRepRole rep_role;

  AsyncIOCb* iocb = nullptr;
  size_t id = 0;
  float rank = 0.0;
  uint16_t executor_id = 0;

  /// when the io unit is issued by the executor
  long issued_tp = 0;
  /// when the io unit is scheduled to be submitted to disk
  long submitted_tp = 0;
  /// when the io unit is completed by the disk
  long completed_tp = 0;

  long ExecutionLatency() const { return completed_tp - submitted_tp; }

  long QueueingLatency() const { return submitted_tp - issued_tp; }

  IOUnit() {}

  IOUnit(int executor_id, IOType io_type, ChainRepRole rep_role, Ssd* ssd,
         char* buf, uint32_t len, uint64_t paddr);

  void Complete();

  inline void Admit() {
    ready_ = true;
  }

  inline bool IsReady() { return ready_.load(); }

#ifdef CONFIG_DEBUG
  /**
   * @brief Dump the information of the io_unit. Only available when CONFIG_DEBUG is on.
   * 
   * @return The string holding debugging information.
  */
  std::string Dump() {
    std::stringstream ss;
    ss << "id: " << id << ", len: " << len << ", paddr: " << paddr
       << ", nqn: " << ssd->nqn
       << ", io type: " << (io_type == IOType::kRead ? "read" : "write")
       << ", queueing latency: " << QueueingLatency() << "us"
       << ", execution latency: " << ExecutionLatency() << "us";
    if (internal::g_pifo_enabled) {
      ss << ", id: " << id << ", request rank: " << rank;
    }
    return ss.str();
  }
#endif

 private:
  std::atomic_bool ready_{false};
};

struct IOCompletion {
  IOType io_type;
  uint32_t len;
  long lat;
  int ssd_id;

  IOCompletion() : len(0), lat(0), ssd_id(-1) {}

  IOCompletion(IOType io_type, uint32_t len, long lat, int ssd_id)
      : io_type(io_type), len(len), lat(lat), ssd_id(ssd_id) {}
};

class AsyncIOCb {
 public:
  IOType io_type;
  char* buf;
  size_t len;
  size_t addr;
  int res = -1;
  int nr_ios = 0;
  int nr_ios_submitted = 0;
  int nr_ios_done = 0;
  int64_t issued_tp = 0;
  int64_t completed_tp = 0;
  // sum of queueing latency of all enclosed io units
  uint64_t queueing_lat = 0;
  // sum of execution latency of all enclosed io units
  uint64_t exec_lat = 0;

  AsyncIOCb() {}

  AsyncIOCb(char* buf, size_t len, size_t addr)
      : buf(buf), len(len), addr(addr), res(0) {}
};

}  // namespace flint