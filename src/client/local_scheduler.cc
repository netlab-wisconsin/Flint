#include "common/shadow_view.h"
#include "common/utils/misc.h"
#include "flint.h"
#include "internal.h"

#include <pthread.h>
#include <memory>
#include <mutex>
#include "common/grpc/message.pb.h"
#include "common/logging.h"
#include "io.h"
#include "local_scheduler.h"

namespace flint {
using time_point = std::chrono::system_clock::time_point;
using namespace std::chrono_literals;
using env::g_config;
using nlohmann::json;

static std::atomic_uint32_t g_iou_id = 0;

IOUnit::IOUnit(int executor_id, IOType io_type, ChainRepRole rep_role, Ssd* ssd,
               char* buf, uint32_t len, uint64_t paddr)
    : ssd(ssd),
      user_buf(buf),
      len(len),
      paddr(paddr),
      io_type(io_type),
      rep_role(rep_role),
      executor_id(executor_id),
      ready_(false) {
  this->id = g_iou_id.fetch_add(1, std::memory_order::memory_order_relaxed);
}

void IOUnit::Complete() {
  completed_tp = GetCurrentTimeMicro();
}

Status RpcLocalSchedulerServiceImpl::PushView(
    ServerContext* context, const FLINT_RPC_MESSAGE::PushViewRequest* request,
    FLINT_RPC_MESSAGE::PushViewResponse* response) {
  local_scheduler_->PushView(request->partial_view());
  return Status::OK;
}

Status RpcLocalSchedulerServiceImpl::BottleneckReport(
    ServerContext* context, const FLINT_RPC_MESSAGE::BottleneckReportRequest* request,
    FLINT_RPC_MESSAGE::BottleneckReportResponse* response) {
  std::vector<int> congested_ssds;
  for (const auto& ssd_id : request->congested_ssds()) {
    congested_ssds.push_back(ssd_id);
  }
  local_scheduler_->HandleBottleneck(congested_ssds);
  return Status::OK;
}

LocalScheduler::LocalScheduler() {}

int LocalScheduler::Init() {
  int ret = 0;

  rpc_server_ = std::make_unique<RpcServer<RpcLocalSchedulerServiceImpl>>(
      g_config.local_scheduler_ip, g_config.local_scheduler_port,
      std::make_unique<RpcLocalSchedulerServiceImpl>(this));

  rpc_server_thread_ = std::thread([this] { rpc_server_->Start(); });
  pthread_setname_np(rpc_server_thread_.native_handle(),
                     "local-scheduler-server");

  rpc_client_ =
      std::make_shared<RpcClient<FLINT_RPC_SERVICE::SchedulerService>>(
          g_config.arbiter_ip, g_config.scheduler_service_port);
  ret = rpc_client_->Connect();
  if (ret != 0) {
    LOG_ERROR("Failed to connect to scheduler service.");
    return ret;
  }
  view_agent_ = std::make_unique<ViewAgent>(rpc_client_);

  ret = register_tenant();
  if (ret != 0) {
    LOG_ERROR("Failed to register tenant.");
    return ret;
  }
  LOG_INFO("Flint client registered, id = %d.\n", internal::g_client_id);

  ret = view_agent_->Init();
  if (ret != 0) {
    LOG_ERROR("Failed to initialize view agent.");
    return ret;
  }

  for (const auto& p : internal::g_ssd_id_map) {
    io_slices_.emplace(p.second, IOSlice());
    next_slice_epoch_times_.emplace(p.second, 0);
    ssd_ids_.push_back(p.second);
  }

  return 0;
}

void LocalScheduler::Shutdown() {
  keep_running_ = false;
  unregister_tenant();
  view_agent_->Shutdown();

  if (sched_thread_.joinable())
    sched_thread_.join();

  rpc_server_->Shutdown();
  if (rpc_server_thread_.joinable())
    rpc_server_thread_.join();

  has_shutdown_ = true;
}

LocalScheduler::~LocalScheduler() {
  if (!has_shutdown_)
    Shutdown();
}

// TODO: should consider different ssds
float LocalScheduler::compute_rank(const PartialShadowView& view,
                                   IOUnitPtr& io_u) {
  if (io_u->rank > 0)
    return io_u->rank;
  float cost_factor = 1.0;
  if (io_u->io_type == IOType::kWrite)
    cost_factor = view.ssd_views.at(io_u->ssd->id).write_cost;
  io_u->rank = 1.0 * io_u->len / KiB(1) * cost_factor;
  return io_u->rank;
}

IOUnitPtr LocalScheduler::pifo_head_io(const PartialShadowView& view) {
  if (io_streams_.size() == 1) {
    auto& stream = io_streams_.begin()->second;
    if (stream.empty())
      return nullptr;
    auto head_io = stream.front();
    return head_io;
  }

  float min_rank = 1e8;
  IOUnitPtr head_io;
  for (auto& p : io_streams_) {
    auto& stream = p.second;
    if (stream.empty())
      continue;
    float rank = compute_rank(view, stream.front());
    if (rank < min_rank) {
      min_rank = rank;
      head_io = stream.front();
    }
  }
  return head_io;
}

void LocalScheduler::pifo_dequeue_io(const IOUnitPtr& iou_to_dequeue) {
  if (io_streams_.size() == 1) {
    auto& stream = io_streams_.begin()->second;
    stream.pop_front();
    return;
  }
  // aging
  for (auto& p : io_streams_) {
    const auto& stream = p.second;
    auto stream_id = p.first;
    if (stream_id == iou_to_dequeue->executor_id || stream.empty()) {
      continue;
    }
    // only consider the head io unit of the same ssd
    if (stream.front()->ssd->id == iou_to_dequeue->ssd->id) {
      auto io_u = stream.front();
      io_u->rank = std::min(0.0f, io_u->rank - iou_to_dequeue->rank);
    }
  }

  auto& head_io_stream = io_streams_.at(iou_to_dequeue->executor_id);
  head_io_stream.pop_front();
}

void LocalScheduler::ScheduleIO() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (io_streams_.empty())
    return;
  PartialShadowView view = view_agent_->PartialView();

  while (true) {
    auto head_io = pifo_head_io(view);
    if (head_io == nullptr)
      break;
    auto weighted_len = head_io->len;
    if (head_io->io_type == IOType::kWrite)
      weighted_len *= view.ssd_views.at(head_io->ssd->id).write_cost;
    auto& io_slice = io_slices_.at(head_io->ssd->id);
    if (io_slice.IsFull()) {
      LOG_DEBUG("io slice full, allocated: %lu, used: %lu.\n",
                io_slice.size_allocated, io_slice.size_used);
      try_request_io_slice(head_io->ssd->id);
      break;
    }
    io_slice.Consume(weighted_len);
    head_io->Admit();
    pifo_dequeue_io(head_io);
    break;
  }
}

void LocalScheduler::try_request_io_slice(int ssd_id) {
  auto now = GetCurrentTimeMicro();
  if (now < next_slice_epoch_times_.at(ssd_id))
    return;
  ClientContext ctx;
  FLINT_RPC_MESSAGE::RequestIOSliceRequest req;
  FLINT_RPC_MESSAGE::RequestIOSliceResponse resp;
  static auto stub = rpc_client_->GetStub();
  req.set_client_id(internal::g_client_id);
  req.set_ssd_id(ssd_id);
  req.set_last_epoch(io_slices_.at(ssd_id).epoch);
  Status status = stub->RequestIOSlice(&ctx, req, &resp);
  if (!status.ok()) {
    LOG_ERROR("Grpc error: %s.\n", status.error_message().c_str());
    FLINT_FATAL();
    return;
  }
  if (resp.err_code() == static_cast<int>(SchedServiceErrCode::kWait)) {
    next_slice_epoch_times_.at(ssd_id) = now + resp.wait_time();
    return;
  }
  if (resp.err_code() != static_cast<int>(SchedServiceErrCode::kOK)) {
    LOG_ERROR("Error requesting io slice.\n");
    return;
  }
  io_slices_.at(ssd_id).size_allocated = resp.slice_size();
  io_slices_.at(ssd_id).epoch = resp.epoch();
  io_slices_.at(ssd_id).size_used = 0;
  LOG_DEBUG("Requested io slice for ssd %d, size: %lu, epoch: %lu.\n", ssd_id,
           io_slices_.at(ssd_id).size_allocated, io_slices_.at(ssd_id).epoch);
}

int LocalScheduler::register_tenant() {
  FLINT_RPC_MESSAGE::RegisterRequest req;
  FLINT_RPC_MESSAGE::RegisterResponse resp;

  req.set_local_scheduler_ip(g_config.local_scheduler_ip);
  req.set_local_scheduler_port(g_config.local_scheduler_port);
  req.set_ebof_port(internal::g_ebof_port);
  req.set_slo_level(static_cast<int>(g_config.slo_level));
  auto stub = rpc_client_->GetStub();
  ClientContext ctx;
  Status status = stub->RegisterClient(&ctx, req, &resp);
  if (!status.ok()) {
    LOG_ERROR("Grpc error: %s.", status.error_message().c_str());
    return -1;
  }
  if (resp.err_code() != static_cast<int>(SchedServiceErrCode::kOK)) {
    LOG_ERROR("Error registering client: %s.\n", resp.err_msg().c_str());
    return -1;
  }
  internal::g_client_id = resp.client_id();
  return 0;
}

int LocalScheduler::unregister_tenant() {
  FLINT_RPC_MESSAGE::RegisterRequest req;
  FLINT_RPC_MESSAGE::RegisterResponse resp;
  req.set_client_id(internal::g_client_id);
  auto stub = rpc_client_->GetStub();
  ClientContext ctx;
  Status status = stub->UnregisterClient(&ctx, req, &resp);
  if (!status.ok()) {
    LOG_ERROR("Grpc error: %s.\n", status.error_message().c_str());
    return -1;
  }
  if (resp.err_code() != static_cast<int>(SchedServiceErrCode::kOK)) {
    LOG_ERROR("Error unregistering client: %s.\n", resp.err_msg().c_str());
    return -1;
  }
  return 0;
}

int LocalScheduler::UpdateSlo(SloLevel slo_level) {
  FLINT_RPC_MESSAGE::RegisterRequest req;
  FLINT_RPC_MESSAGE::RegisterResponse resp;
  req.set_client_id(internal::g_client_id);
  req.set_slo_level(static_cast<int>(slo_level));

  auto stub = rpc_client_->GetStub();
  ClientContext ctx;
  Status status = stub->UpdateClientSlo(&ctx, req, &resp);
  if (!status.ok()) {
    LOG_ERROR("Grpc error: %s.\n", status.error_message().c_str());
    return -1;
  }
  if (resp.err_code() != static_cast<int>(SchedServiceErrCode::kOK)) {
    LOG_ERROR("Error updating slo level: %s.\n", resp.err_msg().c_str());
    return -1;
  }
  return 0;
}

void LocalScheduler::HandleBottleneck(const std::vector<int>& congested_ssds) {
  for (const auto& ssd_id : congested_ssds) {
    if (congested_ssds_.count(ssd_id) == 1) continue;
    congested_ssds_.insert(ssd_id);
  }
}

void LocalScheduler::ReportCmpl(const std::vector<IOCompletion>& cmpls) {
  view_agent_->ReportCmpl(cmpls);
}

int LocalScheduler::PushView(
    const FLINT_RPC_MESSAGE::PartialView& partial_view) {
  return view_agent_->PushView(partial_view);
}

}  // namespace flint
