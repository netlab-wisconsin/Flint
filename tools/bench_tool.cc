#include "client/flint.h"
#include "common/logging.h"
#include "common/utils/misc.h"
#include "common/utils/numautils.h"
#include "common/utils/rand.h"
#include "common/utils/timing.h"

#include <fcntl.h>
#include <getopt.h>
#include <liburing.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <unistd.h>
#include <yaml-cpp/yaml.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#define O_DIRECT __O_DIRECT

static constexpr int kDataChunksNum = 16;
static constexpr size_t kBlockSize = 4096;

#define ASSERT_OR_EXIT(cond, fmt, ...) \
  do {                                 \
    if (!(cond)) {                     \
      LOG_FATAL(fmt, ##__VA_ARGS__);   \
      exit(1);                         \
    }                                  \
  } while (0)

class Bench;

using namespace flint;
using time_point = std::chrono::time_point<std::chrono::steady_clock>;

/* device type */
enum class DeviceType {
  kRawBdev,
  kEVOL,
};

enum class AccessPattern {
  kSequential,
  kRandom,
};

enum class Workload {
  kRead,
  kWrite,
  kMixRW,
};

static volatile std::atomic_int alive_bench_threads = 0;
static volatile std::atomic_bool bench_keep_running = false;
static volatile std::atomic_bool bench_erroed = false;

void bench_alarm(int signum) {
  bench_keep_running = false;
}

class BenchDevice {
 public:
  class RawBdev {
   public:
    std::string sys_path;
    size_t size = 0;
    size_t block_size = 0;
  };

  DeviceType dev_type;
  std::vector<RawBdev> raw_bdevs;  // if more than one, then do it round-robin
  std::shared_ptr<Volume> vol;
};

struct IOStats {
  IOType io_type;
  int64_t queued_time = 0;
  int64_t submitted_time = 0;
  int64_t completed_time = 0;
  size_t len;

  void mark_queued() { queued_time = GetCurrentTimeMicro(); }

  void mark_submitted() { submitted_time = GetCurrentTimeMicro(); }

  void mark_completed() { completed_time = GetCurrentTimeMicro(); }
};

class BenchOptions {
 public:
  int erpc_udp_port = 31850;
  int read_threads_num = 0;
  int write_threads_num = 0;
  int mixrw_threads_num = 0;
  int runtime = 60;
  bool direct_io = true;
  size_t size = 0;
  DeviceType dev_type;
  std::vector<std::string> bdev_paths;
  VolumeOptions vol_options;
  bool poll_mode = true;
  int io_batch_submit = 1;
  std::vector<int> cores;
  bool group_based = false;
  bool scoped_read = false;
  std::string client_id;
  int dev_roundrobin_interval = 1;
};

static BenchOptions g_bench_options;

static const std::string kDefaultGroup = "default";

class BenchThreadOptions {
 public:
  int queue_depth; /* currently not support async io */
  uint32_t io_size_read;
  uint32_t io_size_write;
  double read_ratio = 1.0;

  Workload workload;
  AccessPattern access_pattern = AccessPattern::kSequential;
  std::string group = kDefaultGroup;
  int core = -1;

  inline std::string ToString() const {
    std::stringstream ss;
    auto s_workload = [this]() -> std::string {
      switch (workload) {
        case Workload::kRead:
          return "read";
        case Workload::kWrite:
          return "write";
        case Workload::kMixRW:
          return "mixrw";
      }
      return "";
    }();
    auto s_access_pattern = [this]() -> std::string {
      switch (access_pattern) {
        case AccessPattern::kRandom:
          return "rand";
        case AccessPattern::kSequential:
          return "seq";
      }
      return "";
    }();

    ss << "BenchThreadOptions: [";
    ss << "queue_depth = " << queue_depth << ", io_size_read = " << io_size_read
       << ", io_size_write = " << io_size_write
       << ", read_ratio = " << read_ratio << ", workload = " << s_workload
       << ", access_pattern = " << s_access_pattern << ", core = " << core;
    ;
    ss << "]";
    return ss.str();
  }
};
class BenchResult {
 public:
  uint64_t total_xfer_len_read = 0;
  uint64_t total_io_cnt_read = 0;
  std::vector<uint32_t> lats_read;
  std::vector<uint32_t> slats_read;
  std::vector<uint32_t> clats_read;
  uint64_t total_xfer_len_write = 0;
  uint64_t total_io_cnt_write = 0;
  std::vector<uint32_t> lats_write;
  std::vector<uint32_t> slats_write;
  std::vector<uint32_t> clats_write;
};

class BenchThread {
 private:
  std::vector<char*> data_chunks_for_read_;
  std::vector<char*> data_chunks_for_write_;
  std::thread thread_handle_;
  size_t cur_addr_; /* for sequential io */
  FastRand fast_rand_;
  BinaryRand* br_;
  BenchDevice& device_;
  int core_ = -1;
  void alloc_data_chunks();

  char* get_data_chunk(IOType io_type);

  size_t next_addr(IOType io_type);

  IOType next_action();

  /// TODO: Handle the case where CONFIG_DEBUG is undefined
  void update_evol_result(const AsyncIOCb* iocb);

  void update_raw_bdev_result(const IOStats* stats);

 public:
  BenchThreadOptions thread_options;
  BenchResult result;

  bool Init();

  void Run();

  void Run(int core);

  void BindCore(int core);

  void Finish();

  void BenchRawBdev();

  void BenchEvol();

  void BenchEvolSync();

  BenchThread(const BenchThreadOptions& options, BenchDevice& device)
      : cur_addr_(0), device_(device), thread_options(options) {
    br_ = new BinaryRand(options.read_ratio);
  }

  ~BenchThread();
};

bool BenchThread::Init() {
  alloc_data_chunks();
  cur_addr_ = ROUND_UP(fast_rand_() % g_bench_options.size, kBlockSize);
  return true;
}

BenchThread::~BenchThread() {
  for (size_t i = 0; i < data_chunks_for_read_.size(); i++)
    free(data_chunks_for_read_[i]);
  for (size_t i = 0; i < data_chunks_for_write_.size(); i++)
    free(data_chunks_for_write_[i]);
  delete br_;
}

void BenchThread::BindCore(int core) {
  BindToCore(thread_handle_, core);
  std::string thread_name = "bench-thread-" + std::to_string(core);
  pthread_setname_np(thread_handle_.native_handle(), thread_name.c_str());
  pthread_setschedprio(thread_handle_.native_handle(), THREAD_HIGHESTPRIO);
}

void BenchThread::Finish() {
  if (thread_handle_.joinable())
    thread_handle_.join();
}

void BenchThread::BenchEvolSync() {
  thread_handle_ = std::thread([this] {
    auto vol = device_.vol;
    int flags = O_RDWR;
    if (g_bench_options.direct_io)
      flags |= O_DIRECT;
    auto io_exec = vol->CreateSyncIOExecutor(flags);
    alive_bench_threads++;
    /* busy waiting until all threads are ready */
    while (!bench_keep_running) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    char* data_chunk;
    uint64_t addr;
    size_t io_size_read = thread_options.io_size_read;
    size_t io_size_write = thread_options.io_size_write;
    size_t io_size;
    IOType io_type;
    int ret;

    int64_t start_tp, end_tp;
    while (bench_keep_running) {
      io_type = next_action();
      data_chunk = get_data_chunk(io_type);
      addr = next_addr(io_type);

      io_size = io_type == IOType::kRead ? io_size_read : io_size_write;

      start_tp = GetCurrentTimeMicro();
      if (io_type == IOType::kRead) {
        ret = io_exec->Read(data_chunk, io_size_read, addr);
        io_size = io_size_read;
      } else {
        ret = io_exec->Write(data_chunk, io_size_write, addr);
        io_size = io_size_write;
      }
      rt_assert(static_cast<size_t>(ret) == io_size);
      end_tp = GetCurrentTimeMicro();

      if (io_type == IOType::kRead) {
        result.total_xfer_len_read += io_size;
        result.total_io_cnt_read++;
        result.lats_read.push_back(end_tp - start_tp);
      } else {
        result.total_xfer_len_write += io_size;
        result.total_io_cnt_write++;
        result.lats_write.push_back(end_tp - start_tp);
      }
    }
    io_exec->Close();
  });
}

void BenchThread::BenchEvol() {
  thread_handle_ = std::thread([this] {
    auto qd = thread_options.queue_depth;
    auto vol = device_.vol;
    std::shared_ptr<AsyncIOExecutor> io_exec;

    int flags = O_RDWR;
    if (g_bench_options.direct_io)
      flags |= O_DIRECT;
    io_exec = vol->CreateAsyncIOExecutor(qd, flags);
    if (io_exec == nullptr) {
      LOG_ERROR("Failed to create async io executor\n");
      bench_erroed = true;
      return;
    }
    alive_bench_threads++;
    /* busy waiting until all threads are ready */
    while (!bench_keep_running && !bench_erroed) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    char* data_chunk;
    uint64_t addr;
    AsyncIOCb* sqe;
    AsyncIOCb **cqes = new AsyncIOCb *[qd], *cqe;
    size_t io_size_read = thread_options.io_size_read;
    size_t io_size_write = thread_options.io_size_write;
    size_t io_size;
    IOType io_type;
    std::unordered_set<uint32_t> written_addrs;
    int ret = 0;
    int batch = 0;

    while (bench_keep_running && !bench_erroed) {
      io_type = next_action();
      data_chunk = get_data_chunk(io_type);
      addr = next_addr(io_type);

      // TODO: currently we assume the read size is lower than
      // write size with scoped read, and the read/write are random
      if (g_bench_options.scoped_read && io_type == IOType::kRead) {
        if (written_addrs.find(addr) != written_addrs.end()) {
          io_type = IOType::kWrite;
          data_chunk = get_data_chunk(io_type);
          addr = next_addr(io_type);
        } else {
          if (!written_addrs.empty()) {
            int rand_idx = rand() % written_addrs.size();
            auto it = written_addrs.begin();
            std::advance(it, rand_idx);
            addr = *it;
          }
        }
      }

      io_size = io_type == IOType::kRead ? io_size_read : io_size_write;

      sqe = new AsyncIOCb(data_chunk, io_size, addr);
      assert(sqe != nullptr);

      if (io_type == IOType::kRead)
        ret = io_exec->Read(sqe);
      else
        ret = io_exec->Write(sqe);
      if (ret != 0) {
        LOG_ERROR("read/write error\n");
        bench_erroed = true;
        break;
      }

      batch++;
      if (batch == g_bench_options.io_batch_submit) {
        ret = io_exec->Submit();
        assert(ret == g_bench_options.io_batch_submit);
        batch = 0;
      }

      do {
        int nr_cqe = io_exec->PeekCompletions(qd, cqes);
        if (nr_cqe < 0) {
          bench_erroed = true;
          break;
        }
        if (nr_cqe == 0) {
          continue;
        }
        for (int i = 0; i < nr_cqe; i++) {
          cqe = cqes[i];
          written_addrs.insert(cqe->addr);
          update_evol_result(cqe);
          delete cqe;
        }
      } while (bench_keep_running && io_exec->CurrentDepth() >= qd);
    }
    io_exec->Close();
    delete[] cqes;
  });
}

void BenchThread::BenchRawBdev() {
  thread_handle_ = std::thread([this] {
    alive_bench_threads++;
    while (!bench_keep_running) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    struct io_uring io_ring;
    char* data_chunk;
    uint64_t addr;
    auto qd = thread_options.queue_depth;
    size_t io_size_read = thread_options.io_size_read;
    size_t io_size_write = thread_options.io_size_write;
    size_t io_size;
    IOType io_type;
    struct io_uring_sqe* sqe;
    struct io_uring_cqe* cqes[qd];
    struct io_uring_cqe* cqe;
    int cur_qd = 0;
    int bdev_idx = 0;
    auto& raw_bdevs = device_.raw_bdevs;
    unsigned bdev_nr = raw_bdevs.size();
    int ret;
    int fd;
    int flags;
    std::unordered_set<uint32_t> written_addrs;
    int repeat_dev_times = 0;
    int batch = 0;

    srand(time(nullptr));

    rt_assert(io_uring_queue_init(qd, &io_ring, 0) == 0);
    int fds[raw_bdevs.size()];
    for (unsigned i = 0; i < raw_bdevs.size(); i++) {
      flags = O_RDWR;
      if (g_bench_options.direct_io)
        flags |= O_DIRECT;
      fd = open(raw_bdevs[i].sys_path.c_str(), flags);
      rt_assert(fd >= 0, "Error opening bdev %s",
                raw_bdevs[i].sys_path.c_str());
      fds[i] = fd;
    }
    rt_assert(io_uring_register_files(&io_ring, fds, raw_bdevs.size()) == 0,
              "Error registering bdev fd");

    while (bench_keep_running) {
      io_type = next_action();
      data_chunk = get_data_chunk(io_type);
      addr = next_addr(io_type);

      // TODO: currently we assume the read size is lower than
      // write size with scoped read, and the read/write are random
      if (g_bench_options.scoped_read && io_type == IOType::kRead) {
        if (written_addrs.find(addr) != written_addrs.end()) {
          io_type = IOType::kWrite;
          data_chunk = get_data_chunk(io_type);
          addr = next_addr(io_type);
        } else {
          if (!written_addrs.empty()) {
            int rand_idx = rand() % written_addrs.size();
            auto it = written_addrs.begin();
            std::advance(it, rand_idx);
            addr = *it;
          }
        }
      }
      io_size = io_type == IOType::kRead ? io_size_read : io_size_write;

      sqe = io_uring_get_sqe(&io_ring);
      assert(sqe != nullptr);
      if (io_type == IOType::kWrite)
        io_uring_prep_write(sqe, bdev_idx, data_chunk, io_size, addr);
      else
        io_uring_prep_read(sqe, bdev_idx, data_chunk, io_size, addr);

      repeat_dev_times++;
      if (repeat_dev_times == g_bench_options.dev_roundrobin_interval) {
        bdev_idx = (bdev_idx + 1) % bdev_nr;
        repeat_dev_times = 0;
      }
      // LOG_DEBUG("bench thread %d, bdev idx %d, paddr %lu\n", id_, bdev_idx, paddr);

      IOStats* stats = new IOStats;
      stats->io_type = io_type;
      stats->mark_queued();

      io_uring_sqe_set_data(sqe, static_cast<void*>(stats));
      sqe->flags |= IOSQE_FIXED_FILE;
      batch++;

      if (batch == g_bench_options.io_batch_submit) {
        ret = io_uring_submit(&io_ring);
        assert(ret == g_bench_options.io_batch_submit);
        stats->mark_submitted();
        cur_qd += ret;
        batch = 0;
      }

      if (g_bench_options.poll_mode) {
        do {
          ret = io_uring_peek_batch_cqe(&io_ring, cqes, cur_qd);
          assert(ret >= 0);
          if (ret == 0)
            continue;
          rt_assert(ret <= cur_qd);

          for (int i = 0; i < ret; i++) {
            cqe = cqes[i];
            IOStats* cqe_stats =
                static_cast<IOStats*>(io_uring_cqe_get_data(cqe));
            cqe_stats->len = cqe->res;
            cqe_stats->mark_completed();
            update_raw_bdev_result(cqe_stats);
            io_uring_cqe_seen(&io_ring, cqe);
            delete cqe_stats;
          }
          cur_qd -= ret;
        } while (bench_keep_running && cur_qd >= qd);
      } else {
        if (cur_qd >= qd) {
          ret = io_uring_wait_cqe(&io_ring, &cqe);
          rt_assert(ret == 0);
          IOStats* cqe_stats =
              static_cast<IOStats*>(io_uring_cqe_get_data(cqe));
          cqe_stats->len = cqe->res;
          cqe_stats->mark_completed();
          io_uring_cqe_seen(&io_ring, cqe);
          update_raw_bdev_result(cqe_stats);
          delete cqe_stats;
          cur_qd--;

          ret = io_uring_peek_batch_cqe(&io_ring, cqes, cur_qd);
          for (int i = 0; i < ret; i++) {
            cqe = cqes[i];
            cqe_stats = static_cast<IOStats*>(io_uring_cqe_get_data(cqe));
            cqe_stats->len = cqe->res;
            cqe_stats->mark_completed();
            update_raw_bdev_result(cqe_stats);
            io_uring_cqe_seen(&io_ring, cqe);
            delete cqe_stats;
          }
          cur_qd -= ret;
        }
      }
    }

    io_uring_queue_exit(&io_ring);
    io_uring_unregister_files(&io_ring);
    for (unsigned i = 0; i < raw_bdevs.size(); i++)
      close(fds[i]);
  });
}

void BenchThread::Run() {
  switch (g_bench_options.dev_type) {
    case DeviceType::kRawBdev:
      BenchRawBdev();
      break;
    case DeviceType::kEVOL:
      BenchEvol();
      break;
  }
}

void BenchThread::Run(int core) {
  this->core_ = core;
  Run();
}

void BenchThread::alloc_data_chunks() {
  /**
   * we don't want the cache performance to affect benchmark results,
   * so make the data chunks number a bit large.
  */
  static char random_chars[64];
  static std::string letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  static std::random_device rd;
  static std::mt19937 mt(rd());
  static std::uniform_int_distribution<> dist(0, letters.size() - 1);
  auto io_size_read = thread_options.io_size_read;
  auto io_size_write = thread_options.io_size_write;
  for (int i = 0; i < 64; i++)
    random_chars[i] = letters[dist(mt)];

  data_chunks_for_read_.resize(kDataChunksNum);
  for (int i = 0; i < kDataChunksNum; i++) {
    if (g_bench_options.direct_io)
      data_chunks_for_read_[i] = (char*)aligned_alloc(PAGE_SIZE, io_size_read);
    else
      data_chunks_for_read_[i] = (char*)calloc(1, io_size_read);
    rt_assert(data_chunks_for_read_[i], "error allocating memory");
  }
  for (int i = 0; i < kDataChunksNum; i++) {
    char* chunk = data_chunks_for_read_[i];
    uint32_t j;
    for (j = 0; j < io_size_read; j += 64)
      memcpy(chunk + j, random_chars, 64);
    j -= 64;
    if (io_size_read > j)
      memcpy(chunk + j, random_chars, io_size_read - j);
  }

  data_chunks_for_write_.resize(kDataChunksNum);
  for (int i = 0; i < kDataChunksNum; i++) {
    if (g_bench_options.direct_io)
      data_chunks_for_write_[i] =
          (char*)aligned_alloc(PAGE_SIZE, io_size_write);
    else
      data_chunks_for_write_[i] = (char*)calloc(1, io_size_write);
    rt_assert(data_chunks_for_write_[i], "error allocating memory");
  }
  for (int i = 0; i < kDataChunksNum; i++) {
    char* chunk = data_chunks_for_write_[i];
    uint32_t j;
    for (j = 0; j < io_size_write; j += 64)
      memcpy(chunk + j, random_chars, 64);
    j -= 64;
    if (io_size_write > j)
      memcpy(chunk + j, random_chars, io_size_write - j);
  }
}

IOType BenchThread::next_action() {
  if (thread_options.workload == Workload::kRead)
    return IOType::kRead;
  if (thread_options.workload == Workload::kWrite)
    return IOType::kWrite;
  return (*br_)() ? IOType::kRead : IOType::kWrite;
}

char* BenchThread::get_data_chunk(IOType io_type) {
  static int ir = 0, iw = 0;
  char* ret;
  if (io_type == IOType::kRead) {
    ret = data_chunks_for_read_[ir];
    ir = (ir + 1) % kDataChunksNum;
  } else {
    ret = data_chunks_for_write_[iw];
    iw = (iw + 1) % kDataChunksNum;
  }
  return ret;
}

size_t BenchThread::next_addr(IOType io_type) {
  uint64_t cur_addr = cur_addr_;
  auto io_size = io_type == IOType::kRead ? thread_options.io_size_read
                                          : thread_options.io_size_write;

  cur_addr = (cur_addr + io_size) > g_bench_options.size ? 0 : cur_addr;
  if (thread_options.access_pattern == AccessPattern::kSequential)
    cur_addr_ = cur_addr + io_size;
  else {
    auto t = fast_rand_() % g_bench_options.size;
    cur_addr_ = ROUND_UP(t, kBlockSize);
  }
  return cur_addr;
}

void BenchThread::update_evol_result(const AsyncIOCb* iocb) {
  auto slat_micros = iocb->queueing_lat;
  auto clat_micros = iocb->exec_lat;
  auto lat_micros = iocb->completed_tp - iocb->issued_tp;

  if (iocb->io_type == IOType::kRead) {
    result.total_xfer_len_read += iocb->res;
    result.total_io_cnt_read++;
    result.lats_read.push_back(lat_micros);
    result.slats_read.push_back(slat_micros);
    result.clats_read.push_back(clat_micros);
  } else {
    result.total_xfer_len_write += iocb->res;
    result.total_io_cnt_write++;
    result.lats_write.push_back(lat_micros);
    result.slats_write.push_back(slat_micros);
    result.clats_write.push_back(clat_micros);
  }
}

void BenchThread::update_raw_bdev_result(const IOStats* stats) {
  auto slat_micros = stats->submitted_time - stats->queued_time;
  auto clat_micros = stats->completed_time - stats->submitted_time;
  auto lat_micros = stats->completed_time - stats->queued_time;

  if (stats->io_type == IOType::kRead) {
    result.total_xfer_len_read += stats->len;
    result.total_io_cnt_read++;
    result.lats_read.push_back(lat_micros);
    result.slats_read.push_back(slat_micros);
    result.clats_read.push_back(clat_micros);
  } else {
    result.total_xfer_len_write += stats->len;
    result.total_io_cnt_write++;
    result.lats_write.push_back(lat_micros);
    result.slats_write.push_back(slat_micros);
    result.clats_write.push_back(clat_micros);
  }
}

class Bench {
 public:
  std::vector<BenchThreadOptions> bench_thread_options;

  Bench(const std::vector<BenchThreadOptions>& bo) : bench_thread_options(bo) {}
  void Run();

 private:
  std::vector<std::shared_ptr<BenchThread>> bench_threads_;
  BenchDevice device_;

  bool open_device();

  void close_device();
};

bool Bench::open_device() {
  int fd;
  switch (g_bench_options.dev_type) {
    case DeviceType::kRawBdev:
      for (const std::string& bdev_path : g_bench_options.bdev_paths) {
        auto& raw_bdev = device_.raw_bdevs.emplace_back();
        raw_bdev.sys_path = bdev_path;
        fd = open(bdev_path.c_str(), O_RDWR);
        rt_assert(fd >= 0, "Error opening bdev %s", bdev_path.c_str());
        if (ioctl(fd, BLKGETSIZE64, &raw_bdev.size) ||
            ioctl(fd, BLKSSZGET, &raw_bdev.block_size)) {
          perror("ioctl");
          exit(1);
        }
        close(fd);
        rt_assert(raw_bdev.block_size == kBlockSize, "Block size must be %d",
                  kBlockSize);
        if (raw_bdev.size < g_bench_options.size) {
          printf(
              "Device size %lu can't be greater than the benchmark size %lu\n",
              raw_bdev.size, g_bench_options.size);
          return false;
        }
      }
      break;
    case DeviceType::kEVOL:
      auto vol = Volume::Open(g_bench_options.vol_options);
      if (vol == nullptr)
        return false;
      if (vol->GetSize() < g_bench_options.size) {
        printf("Volume size %lu can't be greater than the benchmark size %lu\n",
               vol->GetSize(), g_bench_options.size);
        return false;
      }
      device_.vol = vol;
      break;
  }
  return true;
}

void Bench::close_device() {
  switch (g_bench_options.dev_type) {
    case DeviceType::kRawBdev:
      break;
    case DeviceType::kEVOL:
      break;
  }
}

void Bench::Run() {
  int thread_num = bench_thread_options.size();
  uint64_t cur_io_cnt_read = 0, last_io_cnt_read = 0;
  uint64_t cur_io_cnt_write = 0, last_io_cnt_write = 0;
  uint64_t cur_xfer_len_read = 0, last_xfer_len_read = 0;
  uint64_t cur_xfer_len_write = 0, last_xfer_len_write = 0;
  double iops_read, bw_read;
  double iops_write, bw_write;
  std::map<std::string, std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>>
      grouped_last_results;
  std::map<std::string, std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>>
      grouped_cur_results;
  time_point start_time, end_time;
  double past;
  double avg_lat_read, p50_lat_read, p99_lat_read, p999_lat_read, avg_slat_read,
      p50_slat_read, p99_slat_read, p999_slat_read, avg_clat_read,
      p50_clat_read, p99_clat_read, p999_clat_read, avg_lat_write,
      p50_lat_write, p99_lat_write, p999_lat_write, avg_slat_write,
      p50_slat_write, p99_slat_write, p999_slat_write, avg_clat_write,
      p50_clat_write, p99_clat_write, p999_clat_write;

  if (g_bench_options.cores.size() > 0 &&
      g_bench_options.cores.size() < (unsigned)thread_num) {
    printf("Number of specified cores can't be less than the number of jobs\n");
    return;
  }

  if (g_bench_options.dev_type == DeviceType::kEVOL) {
    env::FlintConfig flint_options;

    int ret = env::Init(flint_options);
    if (ret != 0) {
      LOG_ERROR("Failed to initialize flint!\n");
      return;
    }
  }

  if (!open_device()) {
    LOG_ERROR("Error opening device\n");
    goto out;
  }

  for (int i = 0; i < thread_num; i++) {
    auto& th = bench_threads_.emplace_back(
        std::make_shared<BenchThread>(bench_thread_options[i], device_));
    if (!th->Init()) {
      goto out;
    }
    th->Run(bench_thread_options[i].core);
    th->BindCore(bench_thread_options[i].core);
  }

  while (alive_bench_threads < thread_num && !bench_erroed) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  bench_keep_running = true;

  start_time = std::chrono::steady_clock::now();
  while (bench_keep_running && !bench_erroed) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (bench_erroed)
      break;

    if (!g_bench_options.group_based) {
      cur_io_cnt_read = cur_io_cnt_write = 0;
      cur_xfer_len_read = cur_xfer_len_write = 0;

      for (auto bench_thread : bench_threads_) {
        cur_io_cnt_read += bench_thread->result.total_io_cnt_read;
        cur_xfer_len_read += bench_thread->result.total_xfer_len_read;

        cur_io_cnt_write += bench_thread->result.total_io_cnt_write;
        cur_xfer_len_write += bench_thread->result.total_xfer_len_write;
      }

      iops_read = cur_io_cnt_read - last_io_cnt_read;
      bw_read = (cur_xfer_len_read - last_xfer_len_read) / 1000000.0;
      last_io_cnt_read = cur_io_cnt_read;
      last_xfer_len_read = cur_xfer_len_read;

      iops_write = cur_io_cnt_write - last_io_cnt_write;
      bw_write = (cur_xfer_len_write - last_xfer_len_write) / 1000000.0;
      last_io_cnt_write = cur_io_cnt_write;
      last_xfer_len_write = cur_xfer_len_write;
      printf("[READ] %.1f MB/s, %.1f IOPS, [WRITE] %.1f MB/s, %.1f IOPS\n",
             bw_read, iops_read, bw_write, iops_write);
    } else {
      grouped_cur_results.clear();
      for (auto bench_thread : bench_threads_) {
        const std::string& group = bench_thread->thread_options.group;
        auto& result_tuple = grouped_cur_results[group];
        std::get<0>(result_tuple) += bench_thread->result.total_io_cnt_read;
        std::get<1>(result_tuple) += bench_thread->result.total_xfer_len_read;
        std::get<2>(result_tuple) += bench_thread->result.total_io_cnt_write;
        std::get<3>(result_tuple) += bench_thread->result.total_xfer_len_write;
      }

      std::stringstream perf_string_realtime;
      for (const auto& r : grouped_cur_results) {
        const auto& group = r.first;
        const auto& cur_results = r.second;
        const auto& last_results = grouped_last_results[group];
        iops_read = std::get<0>(cur_results) - std::get<0>(last_results);
        bw_read =
            (std::get<1>(cur_results) - std::get<1>(last_results)) / 1000000.0;
        iops_write = std::get<2>(cur_results) - std::get<2>(last_results);
        bw_write =
            (std::get<3>(cur_results) - std::get<3>(last_results)) / 1000000.0;
        perf_string_realtime
            << "group:(" << group << ") " << std::fixed << std::showpoint
            << std::setprecision(1) << "[READ] " << bw_read << " MB/s, "
            << iops_read << " IOPS, "
            << "[WRITE] " << bw_write << " MB/s, " << iops_write << " IOPS | ";
      }
      grouped_last_results = grouped_cur_results;

      std::cout << perf_string_realtime.str() << std::endl;
    }

    end_time = std::chrono::steady_clock::now();
    if (std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time)
            .count() > g_bench_options.runtime)
      break;
  }
  bench_keep_running = false;

  for (int i = 0; i < thread_num; i++) {
    bench_threads_[i]->Finish();
  }

  if (bench_erroed) {
    LOG_ERROR("Benchmark terminated due to errors.\n");
    goto out;
  }

  past = std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now() - start_time)
             .count();
  cur_io_cnt_read = cur_io_cnt_write = 0;
  cur_xfer_len_read = cur_xfer_len_write = 0;
  grouped_cur_results.clear();
  for (auto bench_thread : bench_threads_) {
    if (!g_bench_options.group_based) {
      cur_io_cnt_read += bench_thread->result.total_io_cnt_read;
      cur_xfer_len_read += bench_thread->result.total_xfer_len_read;

      cur_io_cnt_write += bench_thread->result.total_io_cnt_write;
      cur_xfer_len_write += bench_thread->result.total_xfer_len_write;
    } else {
      const std::string& group = bench_thread->thread_options.group;
      auto& result_tuple = grouped_cur_results[group];
      std::get<0>(result_tuple) += bench_thread->result.total_io_cnt_read;
      std::get<1>(result_tuple) += bench_thread->result.total_xfer_len_read;
      std::get<2>(result_tuple) += bench_thread->result.total_io_cnt_write;
      std::get<3>(result_tuple) += bench_thread->result.total_xfer_len_write;
    }
  }

  close_device();

  if (!g_bench_options.group_based) {
    iops_read = cur_io_cnt_read / past * 1000;
    bw_read = cur_xfer_len_read / past * 1000 / 1000000;

    iops_write = cur_io_cnt_write / past * 1000;
    bw_write = cur_xfer_len_write / past * 1000 / 1000000;

    std::vector<uint32_t> read_lats, write_lats;
    std::vector<uint32_t> read_slats, write_slats;
    std::vector<uint32_t> read_clats, write_clats;
    for (auto bench_thread : bench_threads_) {
      read_lats.insert(read_lats.end(), bench_thread->result.lats_read.begin(),
                       bench_thread->result.lats_read.end());
      read_slats.insert(read_slats.end(),
                        bench_thread->result.slats_read.begin(),
                        bench_thread->result.slats_read.end());
      read_clats.insert(read_clats.end(),
                        bench_thread->result.clats_read.begin(),
                        bench_thread->result.clats_read.end());

      write_lats.insert(write_lats.end(),
                        bench_thread->result.lats_write.begin(),
                        bench_thread->result.lats_write.end());
      write_slats.insert(write_slats.end(),
                         bench_thread->result.slats_write.begin(),
                         bench_thread->result.slats_write.end());
      write_clats.insert(write_clats.end(),
                         bench_thread->result.clats_write.begin(),
                         bench_thread->result.clats_write.end());
    }
    std::sort(read_lats.begin(), read_lats.end());
    std::sort(read_slats.begin(), read_slats.end());
    std::sort(read_clats.begin(), read_clats.end());
    std::sort(write_lats.begin(), write_lats.end());
    std::sort(write_slats.begin(), write_slats.end());
    std::sort(write_clats.begin(), write_clats.end());
    avg_lat_read =
        read_lats.size() == 0
            ? 0.0
            : std::accumulate(read_lats.begin(), read_lats.end(), 0.0) /
                  read_lats.size();
    p50_lat_read =
        read_lats.size() == 0 ? 0.0 : read_lats[(int)(read_lats.size() * 0.5)];
    p99_lat_read =
        read_lats.size() == 0 ? 0.0 : read_lats[(int)(read_lats.size() * 0.99)];
    p999_lat_read = read_lats.size() == 0
                        ? 0.0
                        : read_lats[(int)(read_lats.size() * 0.999)];
    avg_slat_read =
        read_slats.size() == 0
            ? 0.0
            : std::accumulate(read_slats.begin(), read_slats.end(), 0.0) /
                  read_slats.size();
    p50_slat_read = read_slats.size() == 0
                        ? 0.0
                        : read_slats[(int)(read_slats.size() * 0.5)];
    p99_slat_read = read_slats.size() == 0
                        ? 0.0
                        : read_slats[(int)(read_slats.size() * 0.99)];
    p999_slat_read = read_slats.size() == 0
                         ? 0.0
                         : read_slats[(int)(read_slats.size() * 0.999)];
    avg_clat_read =
        read_clats.size() == 0
            ? 0.0
            : std::accumulate(read_clats.begin(), read_clats.end(), 0.0) /
                  read_clats.size();
    p50_clat_read = read_clats.size() == 0
                        ? 0.0
                        : read_clats[(int)(read_clats.size() * 0.5)];
    p99_clat_read = read_clats.size() == 0
                        ? 0.0
                        : read_clats[(int)(read_clats.size() * 0.99)];
    p999_clat_read = read_clats.size() == 0
                         ? 0.0
                         : read_clats[(int)(read_clats.size() * 0.999)];

    avg_lat_write =
        write_lats.size() == 0
            ? 0.0
            : std::accumulate(write_lats.begin(), write_lats.end(), 0.0) /
                  write_lats.size();
    p50_lat_write = write_lats.size() == 0
                        ? 0.0
                        : write_lats[(int)(write_lats.size() * 0.5)];
    p99_lat_write = write_lats.size() == 0
                        ? 0.0
                        : write_lats[(int)(write_lats.size() * 0.99)];
    p999_lat_write = write_lats.size() == 0
                         ? 0.0
                         : write_lats[(int)(write_lats.size() * 0.999)];
    avg_slat_write =
        write_slats.size() == 0
            ? 0.0
            : std::accumulate(write_slats.begin(), write_slats.end(), 0.0) /
                  write_slats.size();
    p50_slat_write = write_slats.size() == 0
                         ? 0.0
                         : write_slats[(int)(write_slats.size() * 0.5)];
    p99_slat_write = write_slats.size() == 0
                         ? 0.0
                         : write_slats[(int)(write_slats.size() * 0.99)];
    p999_slat_write = write_slats.size() == 0
                          ? 0.0
                          : write_slats[(int)(write_slats.size() * 0.999)];
    avg_clat_write =
        write_clats.size() == 0
            ? 0.0
            : std::accumulate(write_clats.begin(), write_clats.end(), 0.0) /
                  write_clats.size();
    p50_clat_write = write_clats.size() == 0
                         ? 0.0
                         : write_clats[(int)(write_clats.size() * 0.5)];
    p99_clat_write = write_clats.size() == 0
                         ? 0.0
                         : write_clats[(int)(write_clats.size() * 0.99)];
    p999_clat_write = write_clats.size() == 0
                          ? 0.0
                          : write_clats[(int)(write_clats.size() * 0.999)];
    printf("\n-- Summary --\n");
    printf(
        "[READ] average: %.1f MB/s, %.1f IOPS, [WRITE] average: %.1f MB/s, "
        "%.1f IOPS\n",
        bw_read, iops_read, bw_write, iops_write);
    printf(
        "[READ] average latency: %.1fus, "
        "50%% latency: %.1fus, "
        "99%% latency: %.1fus, "
        "99.9%% latency: %.1fus\n"
        "[READ] average submission latency: %.1fus, "
        "50%% submission latency: %.1fus, "
        "99%% submission latency: %.1fus, "
        "99.9%% submission latency: %.1fus\n"
        "[READ] average completion latency: %.1fus, "
        "50%% completion latency: %.1fus, "
        "99%% completion latency: %.1fus, "
        "99.9%% completion latency: %.1fus\n"
        "[WRITE] average latency: %.1fus, "
        "50%% latency: %.1fus, "
        "99%% latency: %.1fus, "
        "99.9%% latency: %.1fus\n"
        "[WRITE] average submission latency: %.1fus, "
        "50%% submission latency: %.1fus, "
        "99%% submission latency: %.1fus, "
        "99.9%% submission latency: %.1fus\n"
        "[WRITE] average completion latency: %.1fus, "
        "50%% completion latency: %.1fus, "
        "99%% completion latency: %.1fus, "
        "99.9%% completion latency: %.1fus\n",
        avg_lat_read, p50_lat_read, p99_lat_read, p999_lat_read, avg_slat_read,
        p50_slat_read, p99_slat_read, p999_slat_read, avg_clat_read,
        p50_clat_read, p99_clat_read, p999_clat_read, avg_lat_write,
        p50_lat_write, p99_lat_write, p999_lat_write, avg_slat_write,
        p50_slat_write, p99_slat_write, p999_slat_write, avg_clat_write,
        p50_clat_write, p99_clat_write, p999_clat_write);
  } else {
    std::map<std::string,
             std::tuple<std::vector<uint32_t>, std::vector<uint32_t>>>
        lat_results;
    for (auto bench_thread : bench_threads_) {
      const std::string& group = bench_thread->thread_options.group;
      auto& res = lat_results[group];
      auto& read_lats = std::get<0>(res);
      auto& write_lats = std::get<1>(res);

      read_lats.insert(read_lats.end(), bench_thread->result.lats_read.begin(),
                       bench_thread->result.lats_read.end());
      write_lats.insert(write_lats.end(),
                        bench_thread->result.lats_write.begin(),
                        bench_thread->result.lats_write.end());
    }

    std::stringstream summary_string;
    summary_string << std::endl
                   << "-- Summary --" << std::endl
                   << std::fixed << std::showpoint << std::setprecision(1);
    for (const auto& r : grouped_cur_results) {
      const auto& group = r.first;
      const auto& cur_results = r.second;
      iops_read = std::get<0>(cur_results) / past * 1000;
      bw_read = std::get<1>(cur_results) / past * 1000 / 1000000;
      iops_write = std::get<2>(cur_results) / past * 1000;
      bw_write = std::get<3>(cur_results) / past * 1000 / 1000000;

      auto& res = lat_results[group];
      auto& read_lats = std::get<0>(res);
      auto& write_lats = std::get<1>(res);
      std::sort(read_lats.begin(), read_lats.end());
      std::sort(write_lats.begin(), write_lats.end());

      double avg_lat_read =
          read_lats.size() == 0
              ? 0.0
              : std::accumulate(read_lats.begin(), read_lats.end(), 0.0) /
                    read_lats.size();
      double p50_lat_read = read_lats.size() == 0
                                ? 0.0
                                : read_lats[(int)(read_lats.size() * 0.5)];
      double p99_lat_read = read_lats.size() == 0
                                ? 0.0
                                : read_lats[(int)(read_lats.size() * 0.99)];
      double p999_lat_read = read_lats.size() == 0
                                 ? 0.0
                                 : read_lats[(int)(read_lats.size() * 0.999)];

      double avg_lat_write =
          write_lats.size() == 0
              ? 0.0
              : std::accumulate(write_lats.begin(), write_lats.end(), 0.0) /
                    write_lats.size();
      double p50_lat_write = write_lats.size() == 0
                                 ? 0.0
                                 : write_lats[(int)(write_lats.size() * 0.5)];
      double p99_lat_write = write_lats.size() == 0
                                 ? 0.0
                                 : write_lats[(int)(write_lats.size() * 0.99)];
      double p999_lat_write =
          write_lats.size() == 0 ? 0.0
                                 : write_lats[(int)(write_lats.size() * 0.999)];

      summary_string << "group:(" << group << "):" << std::endl
                     << "[READ] average: " << bw_read << " MB/s, " << iops_read
                     << " IOPS, "
                     << "[WRITE] average: " << bw_write << " MB/s, "
                     << iops_write << " IOPS, " << std::endl
                     << "[READ] average latency: " << avg_lat_read
                     << "us, 50% latency: " << p50_lat_read
                     << "us, 99% latency: " << p99_lat_read
                     << "us, 99.9% latency: " << p999_lat_read << "us"
                     << std::endl
                     << "[WRITE] average latency: " << avg_lat_write
                     << "us, 50% latency: " << p50_lat_write
                     << "us, 99% latency: " << p99_lat_write
                     << "us, 99.9% latency: " << p999_lat_write << "us"
                     << std::endl;
    }

    std::cout << summary_string.str();
  }

out:
  if (g_bench_options.dev_type == DeviceType::kEVOL) {
    env::Close();
#ifdef CONFIG_STATS
    std::cout << std::endl << client_stats.ToString();
#endif
  }
}

static void redirect_output(const std::string& output_fn, bool overwrite) {
  int flags = O_WRONLY | O_CREAT;
  if (!overwrite)
    flags |= O_APPEND;
  int fd = open(output_fn.c_str(), flags, 0644);
  if (fd < 0) {
    LOG_ERROR("error opening output file %s: %s\n", output_fn.c_str(),
              strerror(errno));
    exit(1);
  }
  if ((dup2(fd, STDOUT_FILENO) == -1) || (dup2(fd, STDERR_FILENO) == -1)) {
    LOG_ERROR("error redirecting output: %s\n", strerror(errno));
    exit(1);
  }
}

static size_t read_size(const std::string& s) {
  char last_char = s.at(s.size() - 1);
  size_t res;
  float v = std::stof(s.substr(0, s.size() - 1));
  switch (last_char) {
    case 't':
    case 'T':
      res = static_cast<size_t>(v * TiB(1));
      break;
    case 'g':
    case 'G':
      res = static_cast<size_t>(v * GiB(1));
      break;
    case 'm':
    case 'M':
      res = static_cast<size_t>(v * MiB(1));
      break;
    case 'k':
    case 'K':
      res = static_cast<size_t>(v * KiB(1));
      break;
    default:
      res = std::stoull(s);
      break;
  }
  return res;
}

static std::vector<int> read_cores(const std::string& s) {
  std::vector<int> result;
  std::stringstream ss(s);
  std::string part;

  while (std::getline(ss, part, ',')) {
    size_t dashPos = part.find('-');
    if (dashPos != std::string::npos) {
      int start, end;
      start = std::stoi(part.substr(0, dashPos));
      end = std::stoi(part.substr(dashPos + 1));

      for (int i = start; i <= end; ++i) {
        result.push_back(i);
      }
    } else {
      result.push_back(std::stoi(part));
    }
  }
  return result;
}

static std::vector<BenchThreadOptions> parse_job_yaml(
    const std::string yaml_file) {
  std::vector<BenchThreadOptions> res;
  YAML::Node config;

  try {
    config = YAML::LoadFile(yaml_file);
  } catch (const YAML::Exception& e) {
    LOG_ERROR("Failed to load config file: %s\n", e.what());
    exit(1);
  }

  ASSERT_OR_EXIT(static_cast<bool>(config["dev_type"]), "Missing dev_type");
  auto v_dev_type = config["dev_type"].as<std::string>();
  if (v_dev_type == "evol") {
    g_bench_options.dev_type = DeviceType::kEVOL;
    ASSERT_OR_EXIT(static_cast<bool>(config["vol_name"]), "Missing vol_name");
    auto v_vol_name = config["vol_name"].as<std::string>();
    g_bench_options.vol_options.name = v_vol_name;
  } else if (v_dev_type == "raw") {
    g_bench_options.dev_type = DeviceType::kRawBdev;
    ASSERT_OR_EXIT(static_cast<bool>(config["bdev_path"]), "Missing bdev_path");

    auto v_bdev_path = config["bdev_path"].as<std::string>();
    g_bench_options.bdev_paths = string_split(v_bdev_path, ":");
  } else {
    ASSERT_OR_EXIT(false, "Invalid dev_type: %s", v_dev_type.c_str());
  }
  g_bench_options.runtime = config["runtime"].as<int>();
  g_bench_options.direct_io = config["direct_io"].as<bool>();
  g_bench_options.erpc_udp_port = config["erpc_udp_port"].as<int>();
  g_bench_options.poll_mode = config["poll_mode"].as<bool>();
  g_bench_options.io_batch_submit = config["io_batch_submit"].as<int>();
  auto v_size = config["size"].as<std::string>();
  g_bench_options.size = read_size(v_size);
  if (static_cast<bool>(config["group_based"])) {
    g_bench_options.group_based = config["group_based"].as<bool>();
  }

  if (static_cast<bool>(config["cores"])) {
    g_bench_options.cores = read_cores(config["cores"].as<std::string>());
  }

  if (static_cast<bool>(config["dev_roundrobin_interval"])) {
    g_bench_options.dev_roundrobin_interval =
        config["dev_roundrobin_interval"].as<int>();
  }

  if (static_cast<bool>(config["scoped_read"])) {
    g_bench_options.scoped_read = config["scoped_read"].as<bool>();
  }

  for (auto job_node : config["jobs"]) {
    YAML::Node job = job_node["job"];
    int job_num = job["num"].as<int>();
    auto job_config = job["config"];
    std::vector<int> group_cores;
    if (static_cast<bool>(job_config["cores"])) {
      group_cores = read_cores(job_config["cores"].as<std::string>());
      ASSERT_OR_EXIT(group_cores.size() >= (size_t)job_num,
                     "Insufficient core number specified for benchmark group");
    }

    ASSERT_OR_EXIT(static_cast<bool>(job_config["workload"]),
                   "Missing workload");
    ASSERT_OR_EXIT(static_cast<bool>(job_config["access_pattern"]),
                   "Missing access_pattern");

    std::string v_workload = job_config["workload"].as<std::string>();
    std::string v_access_pattern =
        job_config["access_pattern"].as<std::string>();

    Workload workload = {};
    if (v_workload == "read")
      workload = Workload::kRead;
    else if (v_workload == "write")
      workload = Workload::kWrite;
    else if (v_workload == "mixrw")
      workload = Workload::kMixRW;
    else
      ASSERT_OR_EXIT(false, "Invalid workload: %s", v_workload.c_str());

    AccessPattern access_pattern = {};
    if (v_access_pattern == "seq")
      access_pattern = AccessPattern::kSequential;
    else if (v_access_pattern == "rand")
      access_pattern = AccessPattern::kRandom;
    else
      ASSERT_OR_EXIT(false, "Invalid access_pattern: %s",
                     v_access_pattern.c_str());

    ASSERT_OR_EXIT(static_cast<bool>(job_config["qd"]), "Missing qd");
    int qd = job_config["qd"].as<int>();

    if (static_cast<bool>(config["output_file"])) {
      std::string output_fn = config["output_file"].as<std::string>();
      auto overwrite = true;
      if (static_cast<bool>(config["append"])) {
        overwrite = config["append"].as<bool>();
      }
      redirect_output(output_fn, overwrite);
    }

    BenchThreadOptions bto = {
        .queue_depth = qd,
        .workload = workload,
        .access_pattern = access_pattern,
    };

    if (g_bench_options.group_based) {
      if (static_cast<bool>(job_config["group"]))
        bto.group = job_config["group"].as<std::string>();
    }

    if (workload == Workload::kMixRW) {
      ASSERT_OR_EXIT(static_cast<bool>(job_config["read_ratio"]),
                     "Missing read_ratio");
      ASSERT_OR_EXIT(static_cast<bool>(job_config["io_size_read"]),
                     "Missing io_size_read");
      ASSERT_OR_EXIT(static_cast<bool>(job_config["io_size_write"]),
                     "Missing io_size_write");

      double read_ratio = job_config["read_ratio"].as<double>();
      size_t io_size_read =
          read_size(job_config["io_size_read"].as<std::string>());
      size_t io_size_write =
          read_size(job_config["io_size_write"].as<std::string>());
      bto.read_ratio = read_ratio;
      bto.io_size_read = io_size_read;
      bto.io_size_write = io_size_write;
    } else {
      ASSERT_OR_EXIT(static_cast<bool>(job_config["io_size"]),
                     "Missing io_size");
      size_t io_size = read_size(job_config["io_size"].as<std::string>());
      bto.io_size_read = bto.io_size_write = io_size;
    }

    for (int i = 0; i < job_num; i++) {
      if (!group_cores.empty())
        bto.core = group_cores[i];
      res.push_back(bto);
      LOG_DEBUG("%s\n", bto.ToString().c_str());
    }
  }

  return res;
}

int main(int argc, char* argv[]) {
  std::vector<BenchThreadOptions> thread_options;
  std::string job_filename;

  if (argc == 1 || strcmp(argv[1], "-h") == 0 ||
      strcmp(argv[1], "--help") == 0) {
    printf("Usage: bench_tool <job_file>\n");
    exit(1);
  }

  job_filename.assign(argv[1]);

  std::signal(SIGINT, bench_alarm);
  // std::signal(SIGQUIT, bench_alarm);

#ifdef CONFIG_DEBUG
  struct rlimit new_lim;
  new_lim.rlim_max = RLIM_INFINITY;
  new_lim.rlim_cur = RLIM_INFINITY;
  if (setrlimit(RLIMIT_CORE, &new_lim) == -1) {
    LOG_FATAL("error setting rlimit for core dump: %s\n", strerror(errno));
    exit(1);
  }
#endif

  thread_options = parse_job_yaml(job_filename);

  srand(time(nullptr));

  Bench b(thread_options);
  b.Run();

  return 0;
}