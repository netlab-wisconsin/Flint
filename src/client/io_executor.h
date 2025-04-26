#pragma once

#include <liburing.h>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include "common/io_defs.h"
#include "common/utils/semaphore.h"
#include "common/utils/spsc_queue.h"
#include "internal.h"
#include "io.h"
#include "volume.h"

namespace flint {
using namespace rigtorp;  // SPSC queue

class IOExecutor {
 protected:
  struct io_uring io_ring_;
  Volume *volume_;

  int id_ = -1;
  int dev_fds_[256];
  int nr_devs_ = 0;
  std::map<int, int>
      fd_offset_map_;  ///< Map actual fd to offset in the fd array.
  bool closed_ = true;
  int flags_;
  SsdPool ssd_pool_;
  VolumeExtLocatorMap vol_ext_locator_map_;

  void lookup_phys_extents(char *buf, uint32_t len, uint64_t addr,
                           IOType io_type,
                           std::vector<IOUnitPtr> &standalone_ios);

  int register_dev_fds();

  int unregister_dev_fds();

  virtual int init();

 public:
  IOExecutor(Volume *volume, int flags);

  virtual ~IOExecutor();

  virtual void Close();

  size_t Id() const { return id_; }
};

class SyncIOExecutor : public IOExecutor {
 private:
  SyncIOExecutor(Volume *volume, int flags);

  SyncIOExecutor(const SyncIOExecutor &) = delete;

  SyncIOExecutor &operator=(const SyncIOExecutor &) = delete;

  SyncIOExecutor(SyncIOExecutor &&) = delete;

  SyncIOExecutor &operator=(SyncIOExecutor &&) = delete;

  int rw_rep_nodes_scheduled(std::vector<IOUnitPtr> &scheduled_io_units);

  int init() override;

 public:
  friend class Volume;

  ~SyncIOExecutor();

  int Read(char *buf, uint32_t len, uint64_t addr);

  int Write(char *buf, uint32_t len, uint64_t addr);
};

class AsyncIOExecutor : public IOExecutor {
 private:
  int io_uring_cur_qd_ = 0;
  int qd_ = 0;
  int cur_qd_ = 0;

  /// hold io unit shared ptrs that are submitted to io_uring
  /// to avoid them being deleted when the shared ptr goes out of scope
  std::unordered_map<size_t, IOUnitPtr> iou_ptr_pool_;

  AsyncIOExecutor(Volume *volume, int depth, int flags);

  int rw_rep_nodes_scheduled(std::vector<IOUnitPtr> &io_us);

  std::queue<IOUnitPtr> sq_;

  int init() override;

  auto &sq() { return sq_; }

 public:
  friend class Volume;

  void Close() override;

  ~AsyncIOExecutor();

  int MaxDepth() const { return qd_; }

  int CurrentDepth() const { return cur_qd_; }

  int Read(AsyncIOCb *iocb);

  int Write(AsyncIOCb *iocb);

  int Submit();

  int PeekCompletions(unsigned peek_nr, AsyncIOCb **iocbs);
};

};  // namespace flint