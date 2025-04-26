#pragma once

#include <shared_mutex>
#include <thread>
#include "common/shadow_view.h"
#undef BLOCK_SIZE
#include "common/utils/mpmc_queue.h"
#include "common/utils/semaphore.h"
#include "common/grpc/service.grpc.pb.h"
#include "io.h"

namespace flint {
using moodycamel::ConcurrentQueue;

class ViewAgent {
 public:
  ViewAgent(std::shared_ptr<RpcClient<FLINT_RPC_SERVICE::SchedulerService>>
                rpc_client);

  int Init();

  void Shutdown();

  void ReportCmpl(const std::vector<IOCompletion>& cmpls);

  PartialShadowView PartialView() {
    std::shared_lock lk{view_mu_};
    return partial_view_;
  }

  int PushView(const FLINT_RPC_MESSAGE::PartialView& partial_view);

 private:
  std::shared_ptr<RpcClient<FLINT_RPC_SERVICE::SchedulerService>> rpc_client_;

  bool keep_running_ = true;
  Semaphore threads_launched_sem_;

  PartialShadowView partial_view_;
  std::shared_mutex view_mu_;
  long last_view_update_time_ = 0;

  std::thread reporter_thread_;
  ConcurrentQueue<IOCompletion> report_queue_;
  // std::mutex report_queue_mu_;
  // std::condition_variable report_queue_cv_;
  static constexpr int kReportBatchSize = 128;

  void reporter_thread_do_work();

  bool is_view_outdated(const FLINT_RPC_MESSAGE::ViewRecency& view_recency);

  void update_partial_view_by_cmpls(const std::vector<IOCompletion>& cmpls, int cnt);

  int pull_view();

  bool is_in_sync_window();
};

}  // namespace flint