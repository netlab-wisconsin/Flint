#include "io_executor.h"
#include <liburing.h>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <memory>
#include "common/logging.h"
#include "common/ssd.h"
#include "common/utils/misc.h"
#include "internal.h"
#include "io.h"
#include "local_scheduler.h"
#include "volume_agent.h"

namespace flint {

static constexpr int kDefaultQD = 256;

using internal::g_local_scheduler;
using internal::g_shutdown_requested;
using internal::g_ssd_id_map;
using internal::g_volume_agent;

static bool io_sanity_check(Volume *volume, char *buf, uint64_t addr,
                            uint32_t len) {
  if ((volume->GetFlags() & O_DIRECT) && !check_aligned(buf)) {
    LOG_ERROR("unaligned buffer in direct io mode\n");
    return false;
  }

  if (addr + len > volume->GetSize()) {
    LOG_ERROR(
        "data exceeds volume address boundary, data addr: %lu, size: %lu\n",
        addr + len, volume->GetSize());
    return false;
  }

  return true;
}

static std::atomic<int> g_io_executor_id = 0;

IOExecutor::IOExecutor(Volume *volume, int flags)
    : volume_(volume), closed_(false), flags_(flags) {}

int IOExecutor::init() {
  int res = 0;

  res = SsdPool::Create(ssd_pool_, g_ssd_id_map, std::nullopt);
  if (res < 0) {
    LOG_ERROR("failed to create ssd pool\n");
    return res;
  }
  res = ssd_pool_.Open(flags_);
  if (res < 0) {
    LOG_ERROR("failed to open ssd pool\n");
    return res;
  }
  const auto &ssds = ssd_pool_.ssds;
  int i = 0;
  for (const auto &p : ssds) {
    const auto &ssd = p.second;
    int fd = ssd->fd;
    dev_fds_[i] = fd;
    fd_offset_map_[fd] = i;
    nr_devs_++;
    i++;
  }
  id_ = g_io_executor_id.fetch_add(1);

  res = g_local_scheduler->AddIOExecutor(*this);
  if (res < 0)
    return res;
  return 0;
}

void IOExecutor::Close() {
  closed_ = true;
  g_local_scheduler->RemoveIOExecutor(*this);
}

IOExecutor::~IOExecutor() {
  if (!closed_)
    Close();
  io_uring_queue_exit(&io_ring_);
}

/// @brief lookup physical extents for a single IO request
/// and collect them into an IO unit vector.
/// @note it is possible that the logical extent has not been
/// allocated yet due to lazy allocation, in this case, the lookup
/// will be blocked until the arbiter volume service allocates the
/// relevant extents and gets back to us.
void IOExecutor::lookup_phys_extents(char *buf, uint32_t len, uint64_t addr,
                                     IOType io_type,
                                     std::vector<IOUnitPtr> &ios) {
  uint32_t start_lext_num = addr / kExtentSize;
  uint32_t start_lext_offset = addr % kExtentSize;
  uint32_t end_lext_num = (addr + len) / kExtentSize;
  uint32_t end_lext_offset = (addr + len) % kExtentSize;
  if (end_lext_offset == 0) {
    end_lext_num--;
    end_lext_offset = kExtentSize;
  }

  for (uint32_t lext_num = start_lext_num; lext_num <= end_lext_num;
       lext_num++) {
    uint32_t ext_offset;
    uint32_t buf_len;

    if (lext_num == start_lext_num) {
      ext_offset = start_lext_offset;
      buf_len = std::min(kExtentSize - start_lext_offset, len);
    } else if (lext_num == end_lext_num) {
      ext_offset = 0;
      buf_len = end_lext_offset;
    } else {
      ext_offset = 0;
      buf_len = kExtentSize;
    }

    auto locators = volume_->lookup_logical_extent(lext_num);
    if (locators == std::nullopt) {
      std::vector<size_t> missing_lexts;
      for (uint32_t missing_lext_num = lext_num;
           missing_lext_num <= end_lext_num; missing_lext_num++) {
        missing_lexts.push_back(missing_lext_num);
      }
      g_volume_agent->HandleVolumeExtentFault(*volume_, missing_lexts);
      locators = volume_->lookup_logical_extent(lext_num);
      rt_assert(locators.has_value(), "failed to lookup logical extent %lu",
                lext_num);
    }

    // For writes, io units in a chain are already sorted by their role,
    // i.e. head -> mid -> tail.
    // For reads, we pick the last locator from the chain as it's the tail.
    if (io_type == IOType::kWrite) {
      for (const auto &locator : locators.value()) {
        auto ssd = ssd_pool_.GetSsd(locator.ssd_id);
        size_t paddr = ExtentToAddr(locator.pext_num, ext_offset);
        auto io = std::make_shared<IOUnit>(id_, io_type, locator.rep_role,
                                           ssd.get(), buf, buf_len, paddr);
        ios.push_back(io);
      }
    } else {
      const auto &tail_locator = locators.value().back();
      auto ssd = ssd_pool_.GetSsd(tail_locator.ssd_id);
      size_t paddr = ExtentToAddr(tail_locator.pext_num, ext_offset);
      auto io = std::make_shared<IOUnit>(id_, io_type, tail_locator.rep_role,
                                         ssd.get(), buf, buf_len, paddr);
      ios.push_back(io);
    }

    buf += buf_len;
  }
}

int IOExecutor::register_dev_fds() {
  int ret = io_uring_register_files(&io_ring_, dev_fds_, nr_devs_);
  if (ret < 0) {
    LOG_ERROR("io_uring_register_files: %s\n", strerror(errno));
    return ret;
  }
  return 0;
}

int IOExecutor::unregister_dev_fds() {
  int ret = io_uring_unregister_files(&io_ring_);
  if (ret < 0) {
    LOG_ERROR("io_uring_unregister_files: %s\n", strerror(errno));
    return ret;
  }
  return 0;
}

SyncIOExecutor::SyncIOExecutor(Volume *volume, int flags)
    : IOExecutor(volume, flags) {}

int SyncIOExecutor::init() {
  int ret = IOExecutor::init();
  if (ret != 0)
    return ret;
  ret = io_uring_queue_init(kDefaultQD, &io_ring_, 0);
  if (ret < 0) {
    LOG_ERROR("io_uring_queue_init: %s\n", strerror(errno));
    return ret;
  }
  ret = register_dev_fds();
  return ret;
}

SyncIOExecutor::~SyncIOExecutor() {
  unregister_dev_fds();
}

int SyncIOExecutor::rw_rep_nodes_scheduled(
    std::vector<IOUnitPtr> &scheduled_io_units) {
  struct io_uring_sqe *sqe;
  uint16_t io_cnt = 0;
  int ret = 0;

  for (auto &io_u : scheduled_io_units) {
    while (!io_u->IsReady() && !g_shutdown_requested) {
      g_local_scheduler->ScheduleIO();
    }

    if (g_shutdown_requested)
      return -EINTR;

    sqe = io_uring_get_sqe(&io_ring_);
    assert(sqe != nullptr);
    switch (io_u->io_type) {
      case IOType::kRead:
        io_uring_prep_read(sqe, fd_offset_map_[io_u->ssd->fd], io_u->user_buf,
                           io_u->len, io_u->paddr);
        break;
      case IOType::kWrite:
        io_uring_prep_write(sqe, fd_offset_map_[io_u->ssd->fd], io_u->user_buf,
                            io_u->len, io_u->paddr);
        break;
    }
    io_uring_sqe_set_data(sqe, static_cast<void *>(&io_u));
    sqe->flags |= IOSQE_FIXED_FILE;

    ret = io_uring_submit(&io_ring_);
    if (ret != 1) {
      LOG_ERROR("io_uring_submit: %s", strerror(errno));
      return -1;
    }
    io_u->submitted_tp = GetCurrentTimeMicro();
    io_cnt++;
  }

  struct io_uring_cqe *cqes[io_cnt], *cqe;
  ret = io_uring_wait_cqe_nr(&io_ring_, cqes, io_cnt);
  if (ret < 0) {
    LOG_ERROR("io_uring_wait_cqe_nr: %s", strerror(errno));
    return -1;
  }
  for (int i = 0; i < io_cnt; i++) {
    cqe = cqes[i];
    IOUnit *io_u = static_cast<IOUnit *>(io_uring_cqe_get_data(cqe));
    rt_assert((uint64_t)cqe->res == io_u->len, "io error: %s",
              strerror(-cqe->res));
    if ((uint64_t)cqe->res != io_u->len) {
      LOG_ERROR("io error: %s\n", strerror(-cqe->res));
      return cqe->res;
    }
    io_uring_cqe_seen(&io_ring_, cqe);

    io_u->Complete();
    g_local_scheduler->ReportCmpl(std::vector<IOCompletion>{
        {io_u->io_type, io_u->len, io_u->ExecutionLatency(), io_u->ssd->id}});
  }
  return 0;
}

int SyncIOExecutor::Read(char *buf, uint32_t len, uint64_t addr) {
  FLINT_ALIVE_OR_RET(-1);
  int ret;

  if (!io_sanity_check(volume_, buf, addr, len))
    return -EINVAL;

  std::vector<IOUnitPtr> scheduled_io_us;
  lookup_phys_extents(buf, len, addr, IOType::kRead, scheduled_io_us);

  ret = rw_rep_nodes_scheduled(scheduled_io_us);
  if (unlikely(ret != 0 && ret != -EINTR))
    LOG_ERROR("error reading tail nodes\n");

  return len;
}

int SyncIOExecutor::Write(char *buf, uint32_t len, uint64_t addr) {
  FLINT_ALIVE_OR_RET(-1);
  int ret;

  if (!io_sanity_check(volume_, buf, addr, len))
    return -EINVAL;

  if (unlikely(volume_->rep_factor_ == 1)) {
    std::vector<IOUnitPtr> scheduled_io_us;
    lookup_phys_extents(buf, len, addr, IOType::kWrite, scheduled_io_us);
    ret = rw_rep_nodes_scheduled(scheduled_io_us);
    if (unlikely(ret != 0 && ret != -EINTR)) {
      LOG_ERROR("error writing standalone node\n");
      return ret;
    }
  } else {
  }

  return len;
}

int AsyncIOExecutor::Read(AsyncIOCb *iocb) {
  FLINT_ALIVE_OR_RET(-1);
  auto buf = iocb->buf;
  auto len = iocb->len;
  auto addr = iocb->addr;

  if (!io_sanity_check(volume_, buf, addr, len))
    return -EINVAL;
  if (cur_qd_ == qd_)
    return -EBUSY;

  iocb->io_type = IOType::kRead;
  iocb->issued_tp = GetCurrentTimeMicro();
  std::vector<IOUnitPtr> io_us;

  lookup_phys_extents(buf, len, addr, IOType::kRead, io_us);

  for (size_t i = 0; i < io_us.size(); i++) {
    auto &io_u = io_us[i];
    io_u->iocb = iocb;
    io_u->issued_tp = GetCurrentTimeMicro();
    iocb->nr_ios++;
    sq_.push(io_u);
  }
  g_local_scheduler->EnqueueIO(id_, io_us);
  cur_qd_++;
  return 0;
}

int AsyncIOExecutor::Write(AsyncIOCb *iocb) {
  FLINT_ALIVE_OR_RET(-1);
  auto buf = iocb->buf;
  auto len = iocb->len;
  auto addr = iocb->addr;

  iocb->io_type = IOType::kWrite;
  iocb->issued_tp = GetCurrentTimeMicro();
  if (!io_sanity_check(volume_, buf, addr, len))
    return -EINVAL;
  if (cur_qd_ == qd_)
    return -EBUSY;

  std::vector<IOUnitPtr> io_us;
  lookup_phys_extents(buf, len, addr, IOType::kWrite, io_us);
  for (size_t i = 0; i < io_us.size(); i++) {
    auto &io_u = io_us[i];
    io_u->iocb = iocb;
    io_u->issued_tp = GetCurrentTimeMicro();
    iocb->nr_ios++;
    sq_.push(io_u);
  }
  g_local_scheduler->EnqueueIO(id_, io_us);

  cur_qd_++;
  return 0;
}

AsyncIOExecutor::AsyncIOExecutor(Volume *volume, int depth, int flags)
    : IOExecutor(volume, flags), qd_(depth) {}

int AsyncIOExecutor::init() {
  int ret = IOExecutor::init();
  if (ret != 0)
    return ret;
  // In case of io unit splits, we need extra qd
  int depth = std::min(8 * qd_, 1024);
  ret = io_uring_queue_init(depth, &io_ring_, 0);
  if (ret < 0) {
    LOG_ERROR("io_uring_queue_init: %s", strerror(errno));
    return ret;
  }
  ret = register_dev_fds();
  if (ret < 0) {
    return ret;
  }
  return 0;
}

AsyncIOExecutor::~AsyncIOExecutor() {
  unregister_dev_fds();
}

void AsyncIOExecutor::Close() {
  closed_ = true;
}

int AsyncIOExecutor::Submit() {
  FLINT_ALIVE_OR_RET(-1);
  int nr_submitted = 0;
  struct io_uring_sqe *sqe;
  AsyncIOCb *iocb;
  int ios_to_submit = sq_.size();

  while (!sq_.empty()) {
    auto io_u = sq_.front();
    while (!io_u->IsReady() && !g_shutdown_requested) {
      g_local_scheduler->ScheduleIO();
    }

    do {
      sqe = io_uring_get_sqe(&io_ring_);
    } while (sqe == nullptr);

    if (io_u->io_type == IOType::kRead)
      io_uring_prep_read(sqe, fd_offset_map_.at(io_u->ssd->fd), io_u->user_buf,
                         io_u->len, io_u->paddr);
    else
      io_uring_prep_write(sqe, fd_offset_map_.at(io_u->ssd->fd), io_u->user_buf,
                          io_u->len, io_u->paddr);
    io_uring_sqe_set_data(sqe, static_cast<void *>(io_u.get()));
    sqe->flags |= IOSQE_FIXED_FILE;

    sq_.pop();

    io_uring_cur_qd_++;

    iocb = io_u->iocb;
    iocb->nr_ios_submitted++;
    if (iocb->nr_ios_submitted == iocb->nr_ios)
      nr_submitted++;

    io_u->submitted_tp = GetCurrentTimeMicro();

    iou_ptr_pool_.insert({io_u->id, io_u});
  }

  int ret = io_uring_submit(&io_ring_);
  if (unlikely(ret != ios_to_submit)) {
    LOG_ERROR("io_uring_submit: %s\n", strerror(errno));
    return -1;
  }

  return nr_submitted;
}

int AsyncIOExecutor::PeekCompletions(unsigned peek_nr, AsyncIOCb **iocbs) {
  FLINT_ALIVE_OR_RET(-1);

  struct io_uring_cqe *cqes[io_uring_cur_qd_], *cqe;
  IOUnit *io_u;
  AsyncIOCb *iocb;
  int nr_done = 0;
  std::vector<IOCompletion> cmpls;

  unsigned ret = io_uring_peek_batch_cqe(&io_ring_, cqes, io_uring_cur_qd_);
  if (unlikely(ret < 0 || ret > (unsigned)io_uring_cur_qd_)) {
    LOG_ERROR("io_uring_peek_batch_cqe: %s\n", strerror(errno));
    return -1;
  }

  for (unsigned i = 0; i < ret && (unsigned)nr_done < peek_nr; i++) {
    cqe = cqes[i];
    io_u = static_cast<IOUnit *>(io_uring_cqe_get_data(cqe));
    if (unlikely((unsigned)cqe->res != io_u->len)) {
      LOG_ERROR("io_uring error: %s, cqe->res: %d, desired io len: %lu\n",
                strerror(-cqe->res), cqe->res, io_u->len);
      return -1;
    }
    io_uring_cqe_seen(&io_ring_, cqe);
    io_uring_cur_qd_--;

    io_u->Complete();

    iocb = io_u->iocb;
    iocb->nr_ios_done++;
    iocb->exec_lat += io_u->completed_tp - io_u->submitted_tp;
    assert(iocb->nr_ios_done == 1 && iocb->nr_ios == 1);
    if (iocb->nr_ios_done == iocb->nr_ios) {
      iocb->res = iocb->len;
      iocb->completed_tp = GetCurrentTimeMicro();
      cur_qd_--;
      iocbs[nr_done] = iocb;
      nr_done++;
    }

    cmpls.emplace_back(io_u->io_type, io_u->len, io_u->ExecutionLatency(),
                       io_u->ssd->id);
    iou_ptr_pool_.erase(io_u->id);
  }
  g_local_scheduler->ReportCmpl(cmpls);
  return nr_done;
}

}  // namespace flint
