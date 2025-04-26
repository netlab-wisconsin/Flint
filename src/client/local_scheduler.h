#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include "common/io_defs.h"
#include "common/rpc.h"
#include "common/shadow_view.h"
#include "io.h"
#include "io_executor.h"
#include "view_agent.h"

namespace flint {
class LocalScheduler;

class RpcLocalSchedulerServiceImpl final
    : public FLINT_RPC_SERVICE::LocalSchedulerService::Service {
 private:
  LocalScheduler* local_scheduler_;

 public:
  RpcLocalSchedulerServiceImpl(LocalScheduler* local_scheduler)
      : local_scheduler_(local_scheduler) {}

  Status PushView(ServerContext* context,
                  const FLINT_RPC_MESSAGE::PushViewRequest* request,
                  FLINT_RPC_MESSAGE::PushViewResponse* response) override;

  Status BottleneckReport(ServerContext* context,
                          const FLINT_RPC_MESSAGE::BottleneckReportRequest* request,
                          FLINT_RPC_MESSAGE::BottleneckReportResponse* response) override;
};

class LocalScheduler {
 private:
  std::shared_ptr<RpcClient<FLINT_RPC_SERVICE::SchedulerService>> rpc_client_;
  std::unique_ptr<RpcServer<RpcLocalSchedulerServiceImpl>> rpc_server_;

  std::thread rpc_server_thread_;

  bool has_shutdown_ = false;

  std::string server_ip_;
  int server_port_;
  std::atomic_bool in_schedule_;

  std::vector<int> ssd_ids_;
  std::set<int> congested_ssds_;
  volatile bool keep_running_ = true;

  std::thread sched_thread_;

  void sched_thread_do_work();

  std::unique_ptr<ViewAgent> view_agent_;
  std::unordered_map<int, std::deque<IOUnitPtr>> io_streams_;
  std::unordered_map<int, IOSlice> io_slices_;
  std::unordered_map<int, long> next_slice_epoch_times_;
  std::mutex mutex_;

  int register_tenant();

  int unregister_tenant();

  float compute_rank(const PartialShadowView& view, IOUnitPtr& io_u);

  IOUnitPtr pifo_head_io(const PartialShadowView& view);

  void pifo_dequeue_io(const IOUnitPtr& iou_to_dequeue);

  void try_request_io_slice(int ssd_id);

 public:
  LocalScheduler();

  int Init();

  void Shutdown();

  ~LocalScheduler();

  /// Report the statistics of the completed IO unit to the arbiter through RPC.
  void ReportCmpl(const std::vector<IOCompletion>& cmpls);

  int UpdateSlo(SloLevel slo_level);

  int AddIOExecutor(IOExecutor& io_exec) {
    int ret = 0;
    std::lock_guard<std::mutex> lock(mutex_);
    if (io_streams_.count(io_exec.Id()) != 0) {
      LOG_ERROR("IO executor %d already exists\n", io_exec.Id());
      ret = -1;
      return ret;
    }
    io_streams_.try_emplace(io_exec.Id());
    LOG_DEBUG("Added IO executor %d.\n", io_exec.Id());
    return ret;
  }

  int RemoveIOExecutor(IOExecutor& io_exec) {
    int ret = 0;
    std::lock_guard<std::mutex> lock(mutex_);
    if (io_streams_.count(io_exec.Id()) == 0) {
      LOG_ERROR("IO executor %d does not exist.\n", io_exec.Id());
      ret = -1;
      return ret;
    }
    io_streams_.erase(io_exec.Id());
    LOG_DEBUG("Removed IO executor %d.\n", io_exec.Id());
    return ret;
  }

  void ScheduleIO();

  void EnqueueIO(size_t executor_id, std::vector<IOUnitPtr>& io_us) {
    auto& io_stream = io_streams_.at(executor_id);
    for (auto& io_u : io_us) {
      io_stream.push_back(io_u);
    }
  }

  int PushView(const FLINT_RPC_MESSAGE::PartialView& partial_view);

  /// Calculate the weight of an IO unit based on its type and current
  /// write cost from the shadow view.
  float IOWeight(const IOUnit& io_u) {
    float weight = 0.0;
    if (io_u.io_type == IOType::kRead) {
      weight = io_u.len * 1.0;
    } else {
      float wc =
          view_agent_->PartialView().ssd_views.at(io_u.ssd->id).write_cost;
      weight = io_u.len * wc;
    }
    return weight;
  }

  void HandleBottleneck(const std::vector<int>& congested_ssds);
};

namespace internal {
inline std::unique_ptr<LocalScheduler> g_local_scheduler;
}

};  // namespace flint