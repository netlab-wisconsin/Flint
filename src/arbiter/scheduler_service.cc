#include "scheduler_service.h"
#include <grpcpp/support/status_code_enum.h>
#include <cstddef>
#include "common/logging.h"
#include "common/rpc.h"
#include "internal.h"
#include "scheduler.h"
#include "ssd_manager.h"

namespace flint {
namespace arbiter {
int SchedulerService::RegisterClient(
    const FLINT_RPC_MESSAGE::RegisterRequest *request,
    FLINT_RPC_MESSAGE::RegisterResponse *reply) {
  int client_id = client_id_++;
  scheduler_->RegisterClient(
      client_id, static_cast<SloLevel>(request->slo_level()),
      request->local_scheduler_ip(), request->local_scheduler_port(),
      request->ebof_port());
  reply->set_client_id(client_id);
  reply->set_err_code(static_cast<int>(SchedServiceErrCode::kOK));
  LOG_DEBUG("Client %d registered.\n", client_id);
  return 0;
}

int SchedulerService::UnregisterClient(
    const FLINT_RPC_MESSAGE::RegisterRequest *request,
    FLINT_RPC_MESSAGE::RegisterResponse *reply) {
  scheduler_->UnregisterClient(request->client_id());
  LOG_DEBUG("Client %d unregistered.\n", request->client_id());
  reply->set_err_code(static_cast<int>(SchedServiceErrCode::kOK));
  return 0;
}

int SchedulerService::UpdateClientSlo(
    const FLINT_RPC_MESSAGE::RegisterRequest *request,
    FLINT_RPC_MESSAGE::RegisterResponse *reply) {
  scheduler_->UpdateClientSlo(request->client_id(),
                              static_cast<SloLevel>(request->slo_level()));
  reply->set_err_code(static_cast<int>(SchedServiceErrCode::kOK));
  return 0;
}

int SchedulerService::RegisterEventCallbacks(
    const FLINT_RPC_MESSAGE::RegisterEventCallbackRequest *request,
    FLINT_RPC_MESSAGE::RegisterEventCallbackResponse *reply) {
  std::vector<EventCallback> event_callbacks;
  for (const auto &callback : request->callbacks()) {
    event_callbacks.push_back(EventCallback{
        request->client_id(), static_cast<EventCallbackOp>(callback.op()),
        callback.threshold(), static_cast<IOType>(callback.io_type()),
        static_cast<CmpOp>(callback.cmp_op())});
  }
  scheduler_->RegisterEventCallbacks(event_callbacks);
  reply->set_err_code(static_cast<int>(SchedServiceErrCode::kOK));
  return 0;
}

int SchedulerService::PullView(const FLINT_RPC_MESSAGE::PullViewRequest *req,
                               FLINT_RPC_MESSAGE::PullViewResponse *resp) {
  int ret = scheduler_->GetPartialView(req->ebof_port(),
                                       resp->mutable_partial_view());
  if (ret != 0) {
    resp->set_err_code(static_cast<int>(SchedServiceErrCode::kInternal));
    return ret;
  }
  resp->set_err_code(static_cast<int>(SchedServiceErrCode::kOK));
  return 0;
}

int SchedulerService::ReportCmpl(
    const FLINT_RPC_MESSAGE::ReportCmplRequest *req,
    FLINT_RPC_MESSAGE::ReportCmplResponse *resp) {
  std::vector<IOCmplInfo> cmpls;
  for (const auto &cpl : req->completions()) {
    cmpls.emplace_back(cpl.client_id(), req->ebof_port(), cpl.ssd_id(),
                       static_cast<IOType>(cpl.io_type()), cpl.lat_microsec(),
                       cpl.size());
  }
  int ret = scheduler_->ReportCmpl(cmpls);
  if (ret != 0) {
    resp->set_err_code(static_cast<int>(SchedServiceErrCode::kInternal));
    return ret;
  }
  if (req->in_sync_window()) {
    ret = scheduler_->GetViewRecency(req->ebof_port(),
                                     resp->mutable_view_recency());
    if (ret != 0) {
      resp->set_err_code(static_cast<int>(SchedServiceErrCode::kInternal));
      return ret;
    }
  } else {
    resp->clear_view_recency();
  }
  resp->set_err_code(static_cast<int>(SchedServiceErrCode::kOK));
  return 0;
}

int SchedulerService::RequestIOSlice(
    const FLINT_RPC_MESSAGE::RequestIOSliceRequest *req,
    FLINT_RPC_MESSAGE::RequestIOSliceResponse *resp) {
  IOSlice io_slice;
  io_slice.ssd_id = req->ssd_id();
  io_slice.epoch = req->last_epoch();
  size_t epoch_waittime;
  int ret =
      scheduler_->RequestIOSlice(req->client_id(), io_slice, epoch_waittime);
  if (ret != 0) {
    resp->set_err_code(static_cast<int>(SchedServiceErrCode::kInternal));
    return ret;
  }
  if (io_slice.size_allocated == 0) {
    resp->set_wait_time(epoch_waittime);
    resp->set_err_code(static_cast<int>(SchedServiceErrCode::kWait));
  } else {
    resp->set_epoch(io_slice.epoch);
    resp->set_slice_size(io_slice.size_allocated);
    LOG_DEBUG("resp slice size: %lu.\n", io_slice.size_allocated);
    resp->set_err_code(static_cast<int>(SchedServiceErrCode::kOK));
  }
  return 0;
}

Status RpcSchedulerServiceImpl::PullView(
    ServerContext *context, const FLINT_RPC_MESSAGE::PullViewRequest *request,
    FLINT_RPC_MESSAGE::PullViewResponse *resp) {
  int ret = sched_service_->PullView(request, resp);
  if (ret != 0) {
    return Status(grpc::StatusCode::INTERNAL, "Failed to pull view.");
  }
  return Status::OK;
}

Status RpcSchedulerServiceImpl::ReportCmpl(
    ServerContext *context, const FLINT_RPC_MESSAGE::ReportCmplRequest *request,
    FLINT_RPC_MESSAGE::ReportCmplResponse *resp) {
  int ret = sched_service_->ReportCmpl(request, resp);
  if (ret != 0) {
    return Status(grpc::StatusCode::INTERNAL, "Failed to report completion.");
  }
  return Status::OK;
}

Status RpcSchedulerServiceImpl::RequestIOSlice(
    ServerContext *context,
    const FLINT_RPC_MESSAGE::RequestIOSliceRequest *request,
    FLINT_RPC_MESSAGE::RequestIOSliceResponse *resp) {
  int ret = sched_service_->RequestIOSlice(request, resp);
  if (ret != 0) {
    return Status(grpc::StatusCode::INTERNAL, "Failed to request IO slice.");
  }
  return Status::OK;
}

Status RpcSchedulerServiceImpl::RegisterClient(
    ServerContext *context, const FLINT_RPC_MESSAGE::RegisterRequest *request,
    FLINT_RPC_MESSAGE::RegisterResponse *resp) {
  int ret = sched_service_->RegisterClient(request, resp);
  if (ret != 0) {
    return Status(grpc::StatusCode::INTERNAL, "Failed to register client.");
  }
  return Status::OK;
}

Status RpcSchedulerServiceImpl::UnregisterClient(
    ServerContext *context, const FLINT_RPC_MESSAGE::RegisterRequest *request,
    FLINT_RPC_MESSAGE::RegisterResponse *resp) {
  int ret = sched_service_->UnregisterClient(request, resp);
  if (ret != 0) {
    return Status(grpc::StatusCode::INTERNAL, "Failed to unregister client.");
  }
  return Status::OK;
}

Status RpcSchedulerServiceImpl::RegisterEventCallbacks(
    ServerContext *context,
    const FLINT_RPC_MESSAGE::RegisterEventCallbackRequest *request,
    FLINT_RPC_MESSAGE::RegisterEventCallbackResponse *resp) {
  int ret = sched_service_->RegisterEventCallbacks(request, resp);
  if (ret != 0) {
    return Status(grpc::StatusCode::INTERNAL,
                  "Failed to register event callbacks.");
  }
  return Status::OK;
}

Status RpcSchedulerServiceImpl::UpdateClientSlo(
    ServerContext *context, const FLINT_RPC_MESSAGE::RegisterRequest *request,
    FLINT_RPC_MESSAGE::RegisterResponse *resp) {
  int ret = sched_service_->UpdateClientSlo(request, resp);
  if (ret != 0) {
    return Status(grpc::StatusCode::INTERNAL, "Failed to update client slo.");
  }
  return Status::OK;
}

SchedulerService::SchedulerService(const std::string &ip, int port,
                                   SsdPoolManager &ssd_pool_manager)
    : ssd_pool_manager_(ssd_pool_manager) {
  scheduler_ = std::make_unique<Scheduler>(ssd_pool_manager);
  rpc_server_ = std::make_unique<RpcServer<RpcSchedulerServiceImpl>>(
      ip, port, std::make_unique<RpcSchedulerServiceImpl>(this));
}

int SchedulerService::Init() {
  int ret = scheduler_->Init();
  return ret;
}

void SchedulerService::Run() {
  scheduler_->Run();
  rpc_loop_thread_ = std::thread([this] { rpc_server_->Start(); });
  pthread_setname_np(rpc_loop_thread_.native_handle(), "sched-service-rpc");
}

void SchedulerService::Shutdown() {
  scheduler_->Shutdown();
  if (rpc_server_)
    rpc_server_->Shutdown();
  if (rpc_loop_thread_.joinable())
    rpc_loop_thread_.join();
}

}  // namespace arbiter
}  // namespace flint