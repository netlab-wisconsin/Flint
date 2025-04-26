#pragma once

#include "common/rpc.h"
#include "scheduler.h"
#include "ssd_manager.h"

#include <memory>
#include <thread>

namespace flint {
namespace arbiter {

class Scheduler;
class SchedulerService;

class RpcSchedulerServiceImpl final
    : public FLINT_RPC_SERVICE::SchedulerService::Service {
 private:
  SchedulerService *sched_service_;

 public:
  RpcSchedulerServiceImpl(SchedulerService *sched_service)
      : sched_service_(sched_service) {}

  Status PullView(ServerContext *context,
                  const FLINT_RPC_MESSAGE::PullViewRequest *request,
                  FLINT_RPC_MESSAGE::PullViewResponse *resp) override;

  Status ReportCmpl(ServerContext *context,
                    const FLINT_RPC_MESSAGE::ReportCmplRequest *request,
                    FLINT_RPC_MESSAGE::ReportCmplResponse *resp) override;

  Status RequestIOSlice(
      ServerContext *context,
      const FLINT_RPC_MESSAGE::RequestIOSliceRequest *request,
      FLINT_RPC_MESSAGE::RequestIOSliceResponse *resp) override;

  Status RegisterClient(ServerContext *context,
                        const FLINT_RPC_MESSAGE::RegisterRequest *request,
                        FLINT_RPC_MESSAGE::RegisterResponse *resp) override;

  Status UnregisterClient(ServerContext *context,
                          const FLINT_RPC_MESSAGE::RegisterRequest *request,
                          FLINT_RPC_MESSAGE::RegisterResponse *resp) override;

  Status UpdateClientSlo(ServerContext *context,
                         const FLINT_RPC_MESSAGE::RegisterRequest *request,
                         FLINT_RPC_MESSAGE::RegisterResponse *resp) override;

  Status RegisterEventCallbacks(
      ServerContext *context,
      const FLINT_RPC_MESSAGE::RegisterEventCallbackRequest *request,
      FLINT_RPC_MESSAGE::RegisterEventCallbackResponse *resp) override;
};

class SchedulerService {
 private:
  SsdPoolManager &ssd_pool_manager_;
  std::unique_ptr<Scheduler> scheduler_;
  std::mutex mu_;

  std::unique_ptr<RpcServer<RpcSchedulerServiceImpl>> rpc_server_;
  std::thread rpc_loop_thread_;

  std::atomic<int> client_id_ = 0;

 public:
  SchedulerService(const std::string &ip, int port,
                   SsdPoolManager &ssd_pool_manager);

  auto GetViewController() const { return scheduler_->GetViewController(); }

  int Init();

  void Run();

  void Shutdown();

  int PullView(const FLINT_RPC_MESSAGE::PullViewRequest *req,
               FLINT_RPC_MESSAGE::PullViewResponse *resp);

  int ReportCmpl(const FLINT_RPC_MESSAGE::ReportCmplRequest *req,
                 FLINT_RPC_MESSAGE::ReportCmplResponse *resp);

  int RegisterClient(const FLINT_RPC_MESSAGE::RegisterRequest *req,
                     FLINT_RPC_MESSAGE::RegisterResponse *resp);

  int UnregisterClient(const FLINT_RPC_MESSAGE::RegisterRequest *req,
                       FLINT_RPC_MESSAGE::RegisterResponse *resp);

  int UpdateClientSlo(const FLINT_RPC_MESSAGE::RegisterRequest *req,
                      FLINT_RPC_MESSAGE::RegisterResponse *resp);

  int RegisterEventCallbacks(
      const FLINT_RPC_MESSAGE::RegisterEventCallbackRequest *req,
      FLINT_RPC_MESSAGE::RegisterEventCallbackResponse *resp);

  int RequestIOSlice(const FLINT_RPC_MESSAGE::RequestIOSliceRequest *req,
                     FLINT_RPC_MESSAGE::RequestIOSliceResponse *resp);
};

}  // namespace arbiter
}  // namespace flint
