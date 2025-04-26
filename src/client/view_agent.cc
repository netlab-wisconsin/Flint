#include "view_agent.h"
#include "client/io.h"
#include "common/logging.h"
#include "common/utils/misc.h"
#include "flint.h"
#include "internal.h"

namespace flint {
ViewAgent::ViewAgent(
    std::shared_ptr<RpcClient<FLINT_RPC_SERVICE::SchedulerService>> rpc_client)
    : rpc_client_(rpc_client) {}

int ViewAgent::Init() {
  if (!rpc_client_->Connected()) {
    LOG_ERROR("ViewAgent: rpc client not connected.");
    return -1;
  }
  // Init the partial shadow view
  partial_view_.net_port_view.port_id = internal::g_ebof_port;
  partial_view_.net_pipe_view.pipe_id = internal::g_ebof_port;
  for (int i = 0; i < internal::g_nic_ports_num; i++) {
    partial_view_.io_port_views.emplace(i, i);
  }

  for (const auto& p : internal::g_ssd_id_map) {
    partial_view_.io_pipe_views.emplace(p.second, p.second);
    partial_view_.ssd_views.emplace(p.second, p.second);
  }

  threads_launched_sem_.Reset(1);

  reporter_thread_ = std::thread([this] { reporter_thread_do_work(); });
  pthread_setname_np(reporter_thread_.native_handle(), "cmpl-reporter");

  threads_launched_sem_.Wait();

  // Register event callback
  // Note that we provide the event callback (i.e. view push mode) here more
  // as a placeholder for future use. The Flint user can always define their
  // own event callbacks when they see certains events are needed to make
  // their customized scheduling decisions.
  ClientContext ctx;
  FLINT_RPC_MESSAGE::RegisterEventCallbackRequest request;
  FLINT_RPC_MESSAGE::RegisterEventCallbackResponse response;

  request.set_client_id(internal::g_client_id);
  auto callback = request.add_callbacks();
  callback->set_op(static_cast<int32_t>(EventCallbackOp::kSsdAvailBwThreshold));
  callback->set_threshold(800.0);
  callback->set_port_or_ssd(0);
  callback->set_io_type(static_cast<int32_t>(IOType::kRead));
  callback->set_cmp_op(static_cast<int32_t>(CmpOp::kLessThan));

  auto stub = rpc_client_->GetStub();
  stub->RegisterEventCallbacks(&ctx, request, &response);
  if (response.err_code() != static_cast<int>(SchedServiceErrCode::kOK)) {
    LOG_ERROR("Error registering event callback.\n");
    return -1;
  }
  return 0;
}

void ViewAgent::Shutdown() {
  keep_running_ = false;
  // report_queue_cv_.notify_one();
  if (reporter_thread_.joinable())
    reporter_thread_.join();
}

void ViewAgent::ReportCmpl(const std::vector<IOCompletion>& cmpls) {
  report_queue_.enqueue_bulk(cmpls.begin(), cmpls.size());
}

static void CopyProtoToPartialView(
    const FLINT_RPC_MESSAGE::PartialView& proto_view,
    PartialShadowView& partial_view) {
  partial_view.net_port_view.port_id = proto_view.net_port_view().port_id();
  partial_view.net_port_view.read_bw = proto_view.net_port_view().read_bw();
  partial_view.net_port_view.write_bw = proto_view.net_port_view().write_bw();
  partial_view.net_port_view.read_iops = proto_view.net_port_view().read_iops();
  partial_view.net_port_view.write_iops =
      proto_view.net_port_view().write_iops();
  for (int i = 0; i < proto_view.net_port_view().read_size_dist_size(); i++) {
    partial_view.net_port_view.read_size_dist[i] =
        proto_view.net_port_view().read_size_dist(i);
    partial_view.net_port_view.write_size_dist[i] =
        proto_view.net_port_view().write_size_dist(i);
  }

  partial_view.net_pipe_view.pipe_id = proto_view.net_pipe_view().pipe_id();
  partial_view.net_pipe_view.read_bw = proto_view.net_pipe_view().read_bw();
  partial_view.net_pipe_view.write_bw = proto_view.net_pipe_view().write_bw();
  partial_view.net_pipe_view.read_iops = proto_view.net_pipe_view().read_iops();
  partial_view.net_pipe_view.write_iops =
      proto_view.net_pipe_view().write_iops();

  for (const auto& [ssd_id, io_port_view] : proto_view.io_port_views()) {
    auto& my_io_port_view = partial_view.io_port_views.at(ssd_id);
    my_io_port_view.port_id = io_port_view.port_id();
    my_io_port_view.read_bw = io_port_view.read_bw();
    my_io_port_view.write_bw = io_port_view.write_bw();
    my_io_port_view.read_iops = io_port_view.read_iops();
    my_io_port_view.write_iops = io_port_view.write_iops();
    for (int i = 0; i < io_port_view.read_size_dist_size(); i++) {
      my_io_port_view.read_size_dist[i] = io_port_view.read_size_dist(i);
      my_io_port_view.write_size_dist[i] = io_port_view.write_size_dist(i);
    }
  }

  for (const auto& [ssd_id, io_pipe_view] : proto_view.io_pipe_views()) {
    auto& my_io_pipe_view = partial_view.io_pipe_views.at(ssd_id);
    my_io_pipe_view.pipe_id = io_pipe_view.pipe_id();
    my_io_pipe_view.read_bw = io_pipe_view.read_bw();
    my_io_pipe_view.write_bw = io_pipe_view.write_bw();
    my_io_pipe_view.read_iops = io_pipe_view.read_iops();
    my_io_pipe_view.write_iops = io_pipe_view.write_iops();
  }

  for (const auto& [ssd_id, ssd_view] : proto_view.ssd_views()) {
    auto& my_ssd_view = partial_view.ssd_views.at(ssd_id);
    my_ssd_view.ssd_id = ssd_view.ssd_id();
    my_ssd_view.read_bw_used = ssd_view.read_bw_used();
    my_ssd_view.write_bw_used = ssd_view.write_bw_used();
    my_ssd_view.read_bw_free = ssd_view.read_bw_free();
    my_ssd_view.write_bw_free = ssd_view.write_bw_free();
    my_ssd_view.frag_degree = ssd_view.frag_degree();
    my_ssd_view.write_cost = ssd_view.write_cost();
  }
}

int ViewAgent::PushView(const FLINT_RPC_MESSAGE::PartialView& partial_view) {
  std::lock_guard lk{view_mu_};
  CopyProtoToPartialView(partial_view, partial_view_);
  return 0;
}

void ViewAgent::update_partial_view_by_cmpls(
    const std::vector<IOCompletion>& cmpls, int cnt) {
  std::lock_guard lk{view_mu_};
  for (int i = 0; i < cnt; i++) {
    const auto& cpl = cmpls[i];
    auto& netport_view = partial_view_.net_port_view;
    netport_view.RecordIO(cpl.len, cpl.io_type);
    auto& netpipe_view = partial_view_.net_pipe_view;
    netpipe_view.RecordIO(cpl.len, cpl.io_type);
    auto& ioport_view =
        partial_view_.io_port_views.at(cpl.ssd_id % internal::g_nic_ports_num);
    ioport_view.RecordIO(cpl.len, cpl.io_type);
    auto& iopipe_view = partial_view_.io_pipe_views.at(cpl.ssd_id);
    iopipe_view.RecordIO(cpl.len, cpl.io_type);
    auto& ssd_view = partial_view_.ssd_views.at(cpl.ssd_id);
    ssd_view.RecordIO(cpl.len, cpl.io_type, cpl.lat);
  }
}

int ViewAgent::pull_view() {
  ClientContext ctx;
  FLINT_RPC_MESSAGE::PullViewRequest request;
  FLINT_RPC_MESSAGE::PullViewResponse response;
  static auto stub = rpc_client_->GetStub();
  stub->PullView(&ctx, request, &response);
  if (response.err_code() != static_cast<int>(SchedServiceErrCode::kOK)) {
    LOG_ERROR("Error pulling view from scheduler service.\n");
    return -1;
  }
  const auto& view = response.partial_view();
  std::lock_guard lk{view_mu_};

  const auto& netport_view = view.net_port_view();
  const auto& netpipe_view = view.net_pipe_view();
  const auto& io_port_views = view.io_port_views();
  const auto& io_pipe_views = view.io_pipe_views();
  const auto& ssd_views = view.ssd_views();

  // net port
  partial_view_.net_port_view.port_id = netport_view.port_id();
  partial_view_.net_port_view.read_bw = netport_view.read_bw();
  partial_view_.net_port_view.write_bw = netport_view.write_bw();
  partial_view_.net_port_view.read_iops = netport_view.read_iops();
  partial_view_.net_port_view.write_iops = netport_view.write_iops();
  for (int i = 0; i < netport_view.read_size_dist_size(); i++) {
    partial_view_.net_port_view.read_size_dist[i] =
        netport_view.read_size_dist(i);
    partial_view_.net_port_view.write_size_dist[i] =
        netport_view.write_size_dist(i);
  }
  partial_view_.net_port_view.recency_counter = netport_view.recency_counter();

  // net pipe
  partial_view_.net_pipe_view.pipe_id = netpipe_view.pipe_id();
  partial_view_.net_pipe_view.read_bw = netpipe_view.read_bw();
  partial_view_.net_pipe_view.write_bw = netpipe_view.write_bw();
  partial_view_.net_pipe_view.read_iops = netpipe_view.read_iops();
  partial_view_.net_pipe_view.write_iops = netpipe_view.write_iops();
  partial_view_.net_pipe_view.recency_counter = netpipe_view.recency_counter();

  // io port
  for (const auto& [ssd_id, io_port_view] : io_port_views) {
    auto& my_io_port_view = partial_view_.io_port_views.at(ssd_id);
    my_io_port_view.port_id = io_port_view.port_id();
    my_io_port_view.read_bw = io_port_view.read_bw();
    my_io_port_view.write_bw = io_port_view.write_bw();
    my_io_port_view.read_iops = io_port_view.read_iops();
    my_io_port_view.write_iops = io_port_view.write_iops();
    for (int i = 0; i < io_port_view.read_size_dist_size(); i++) {
      my_io_port_view.read_size_dist[i] = io_port_view.read_size_dist(i);
      my_io_port_view.write_size_dist[i] = io_port_view.write_size_dist(i);
    }
    my_io_port_view.recency_counter = io_port_view.recency_counter();
  }

  // io pipe
  for (const auto& [ssd_id, io_pipe_view] : io_pipe_views) {
    auto& my_io_pipe_view = partial_view_.io_pipe_views.at(ssd_id);
    my_io_pipe_view.pipe_id = io_pipe_view.pipe_id();
    my_io_pipe_view.read_bw = io_pipe_view.read_bw();
    my_io_pipe_view.write_bw = io_pipe_view.write_bw();
    my_io_pipe_view.read_iops = io_pipe_view.read_iops();
    my_io_pipe_view.write_iops = io_pipe_view.write_iops();
    my_io_pipe_view.recency_counter = io_pipe_view.recency_counter();
  }

  // ssd
  for (const auto& [ssd_id, ssd_view] : ssd_views) {
    auto& my_ssd_view = partial_view_.ssd_views.at(ssd_id);
    my_ssd_view.ssd_id = ssd_view.ssd_id();
    my_ssd_view.read_bw_used = ssd_view.read_bw_used();
    my_ssd_view.write_bw_used = ssd_view.write_bw_used();
    my_ssd_view.read_bw_free = ssd_view.read_bw_free();
    my_ssd_view.write_bw_free = ssd_view.write_bw_free();
    my_ssd_view.frag_degree = ssd_view.frag_degree();
    my_ssd_view.write_cost = ssd_view.write_cost();
    my_ssd_view.recency_counter = ssd_view.recency_counter();
  }
  return 0;
}

bool ViewAgent::is_in_sync_window() {
  return (GetCurrentTimeMillis() - last_view_update_time_) >
         env::g_config.sync_view_interval_ms;
}

bool ViewAgent::is_view_outdated(
    const FLINT_RPC_MESSAGE::ViewRecency& view_recency) {
  for (const auto& [ssd_id, recency] : view_recency.ssd_view_recency()) {
    if (recency > partial_view_.ssd_views.at(ssd_id).recency_counter) {
      return true;
    }
  }
  for (const auto& [port_id, recency] : view_recency.io_port_view_recency()) {
    if (recency > partial_view_.io_port_views.at(port_id).recency_counter) {
      return true;
    }
  }
  for (const auto& [pipe_id, recency] : view_recency.io_pipe_view_recency()) {
    if (recency > partial_view_.io_pipe_views.at(pipe_id).recency_counter) {
      return true;
    }
  }
  if (view_recency.net_port_view_recency() >
      partial_view_.net_port_view.recency_counter) {
    return true;
  }
  if (view_recency.net_pipe_view_recency() >
      partial_view_.net_pipe_view.recency_counter) {
    return true;
  }
  return false;
}

void ViewAgent::reporter_thread_do_work() {
  auto stub = rpc_client_->GetStub();
  if (!stub) {
    LOG_ERROR("Failed to get grpc stub\n");
    FLINT_FATAL();
    threads_launched_sem_.Signal();
    return;
  }

  threads_launched_sem_.Signal();

  while (keep_running_ && FLINT_ISALIVE) {
    FLINT_RPC_MESSAGE::ReportCmplRequest request;
    FLINT_RPC_MESSAGE::ReportCmplResponse resp;
    ClientContext ctx;

    request.set_client_id(internal::g_client_id);
    request.set_ebof_port(internal::g_ebof_port);
    request.set_in_sync_window(is_in_sync_window());

    std::vector<IOCompletion> io_cmpls(kReportBatchSize);
    int nr_cmpls =
        report_queue_.try_dequeue_bulk(io_cmpls.data(), kReportBatchSize);

    if (nr_cmpls == 0) {
      std::this_thread::yield();
      continue;
    }

    for (int i = 0; i < nr_cmpls; i++) {
      FLINT_RPC_MESSAGE::IOCompletion* cpl = request.add_completions();
      const auto& cmpl = io_cmpls[i];
      cpl->set_ssd_id(cmpl.ssd_id);
      cpl->set_io_type(static_cast<uint32_t>(cmpl.io_type));
      cpl->set_size(cmpl.len);
      auto lat = cmpl.lat;
      cpl->set_lat_microsec(lat);
    }

    update_partial_view_by_cmpls(io_cmpls, nr_cmpls);

    auto status = stub->ReportCmpl(&ctx, request, &resp);
    if (!status.ok()) {
      LOG_ERROR("Error reporting completion to scheduler service: %s.\n",
                status.error_details().c_str());
      FLINT_FATAL();
    }

    if (resp.has_view_recency()) {
      if (is_view_outdated(resp.view_recency())) {
        LOG_INFO("Partial view is outdated. Pulling latest view.\n");
        pull_view();
      }
      last_view_update_time_ = GetCurrentTimeMillis();
    }
  }
}

}  // namespace flint