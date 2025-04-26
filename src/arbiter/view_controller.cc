#include "view_controller.h"
#include <mutex>
#include <shared_mutex>
#include "common/grpc/message.pb.h"
#include "common/interface.h"
#include "common/logging.h"
#include "common/shadow_view.h"
#include "internal.h"

namespace flint {
namespace arbiter {
static void CopyViewToProto(int ebof_port, const ShadowView& shadow_view,
                            FLINT_RPC_MESSAGE::PartialView* partial_view) {
  const auto& net_port_view = shadow_view.net_port_views.at(ebof_port);
  const auto& net_pipe_view = shadow_view.net_pipe_views.at(ebof_port);

  // net port
  auto net_port_view_proto = partial_view->mutable_net_port_view();
  net_port_view_proto->set_port_id(net_port_view.port_id);
  net_port_view_proto->set_read_bw(net_port_view.read_bw);
  net_port_view_proto->set_write_bw(net_port_view.write_bw);
  net_port_view_proto->set_read_iops(net_port_view.read_iops);
  net_port_view_proto->set_write_iops(net_port_view.write_iops);
  for (const auto& p : net_port_view.read_size_dist) {
    net_port_view_proto->add_read_size_dist(p);
  }
  for (const auto& p : net_port_view.write_size_dist) {
    net_port_view_proto->add_write_size_dist(p);
  }
  net_port_view_proto->set_recency_counter(net_port_view.recency_counter);

  // net pipe
  auto net_pipe_view_proto = partial_view->mutable_net_pipe_view();
  net_pipe_view_proto->set_pipe_id(net_pipe_view.pipe_id);
  net_pipe_view_proto->set_read_bw(net_pipe_view.read_bw);
  net_pipe_view_proto->set_write_bw(net_pipe_view.write_bw);
  net_pipe_view_proto->set_read_iops(net_pipe_view.read_iops);
  net_pipe_view_proto->set_write_iops(net_pipe_view.write_iops);
  net_pipe_view_proto->set_recency_counter(net_pipe_view.recency_counter);

  for (const auto& p : internal::g_ssd_id_map) {
    int ssd_id = p.second;
    const auto& io_port_view = shadow_view.io_port_views.at(ssd_id);
    const auto& io_pipe_view = shadow_view.io_pipe_views.at(ssd_id);
    const auto& ssd_view = shadow_view.ssd_views.at(ssd_id);

    // io port
    auto io_port_view_proto = partial_view->mutable_io_port_views();
    auto& io_port_view_proto_entry = (*io_port_view_proto)[ssd_id];
    io_port_view_proto_entry.set_port_id(io_port_view.port_id);
    io_port_view_proto_entry.set_read_bw(io_port_view.read_bw);
    io_port_view_proto_entry.set_write_bw(io_port_view.write_bw);
    io_port_view_proto_entry.set_read_iops(io_port_view.read_iops);
    io_port_view_proto_entry.set_write_iops(io_port_view.write_iops);
    for (const auto& p : io_port_view.read_size_dist) {
      io_port_view_proto_entry.add_read_size_dist(p);
    }
    for (const auto& p : io_port_view.write_size_dist) {
      io_port_view_proto_entry.add_write_size_dist(p);
    }
    io_port_view_proto_entry.set_recency_counter(io_port_view.recency_counter);

    // io pipe
    auto io_pipe_view_proto = partial_view->mutable_io_pipe_views();
    auto& io_pipe_view_proto_entry = (*io_pipe_view_proto)[ssd_id];
    io_pipe_view_proto_entry.set_pipe_id(io_pipe_view.pipe_id);
    io_pipe_view_proto_entry.set_read_bw(io_pipe_view.read_bw);
    io_pipe_view_proto_entry.set_write_bw(io_pipe_view.write_bw);
    io_pipe_view_proto_entry.set_read_iops(io_pipe_view.read_iops);
    io_pipe_view_proto_entry.set_write_iops(io_pipe_view.write_iops);
    io_pipe_view_proto_entry.set_recency_counter(io_pipe_view.recency_counter);

    // ssd
    auto ssd_view_proto = partial_view->mutable_ssd_views();
    auto& ssd_view_proto_entry = (*ssd_view_proto)[ssd_id];
    ssd_view_proto_entry.set_ssd_id(ssd_view.ssd_id);
    ssd_view_proto_entry.set_read_bw_used(ssd_view.read_bw_used);
    ssd_view_proto_entry.set_write_bw_used(ssd_view.write_bw_used);
    ssd_view_proto_entry.set_read_bw_free(ssd_view.read_bw_free);
    ssd_view_proto_entry.set_write_bw_free(ssd_view.write_bw_free);
    ssd_view_proto_entry.set_frag_degree(ssd_view.frag_degree);
    ssd_view_proto_entry.set_write_cost(ssd_view.write_cost);
    ssd_view_proto_entry.set_recency_counter(ssd_view.recency_counter);
  }
}

static void CopyViewRecencyToProto(
    int ebof_port, const ShadowView& shadow_view,
    FLINT_RPC_MESSAGE::ViewRecency* view_recency) {
  auto& net_port_view = shadow_view.net_port_views.at(ebof_port);
  auto& net_pipe_view = shadow_view.net_pipe_views.at(ebof_port);
  view_recency->set_net_port_view_recency(net_port_view.recency_counter);
  view_recency->set_net_pipe_view_recency(net_pipe_view.recency_counter);
  auto io_port_view_recency = view_recency->mutable_io_port_view_recency();
  auto io_pipe_view_recency = view_recency->mutable_io_pipe_view_recency();
  auto ssd_view_recency = view_recency->mutable_ssd_view_recency();
  for (const auto& p : internal::g_ssd_id_map) {
    int ssd_id = p.second;
    auto& io_port_view = shadow_view.io_port_views.at(ssd_id);
    io_port_view_recency->insert({ssd_id, io_port_view.recency_counter});
    auto& io_pipe_view = shadow_view.io_pipe_views.at(ssd_id);
    io_pipe_view_recency->insert({ssd_id, io_pipe_view.recency_counter});
    auto& ssd_view = shadow_view.ssd_views.at(ssd_id);
    ssd_view_recency->insert({ssd_id, ssd_view.recency_counter});
  }
}

int ViewController::Init() {
  for (const auto& p : internal::g_ebof_port_map) {
    int port = p.second;
    shadow_view_.net_port_views.emplace(port, port);
    shadow_view_.net_pipe_views.emplace(port, port);
  }
  for (const auto& p : internal::g_ssd_id_map) {
    int ssd_id = p.second;
    shadow_view_.io_port_views.emplace(ssd_id, ssd_id);
    shadow_view_.io_pipe_views.emplace(ssd_id, ssd_id);
    shadow_view_.ssd_views.emplace(ssd_id, ssd_id);
  }

  event_callback_checker_thread_ =
      std::thread([this] { event_callback_checker_do_work(); });
  pthread_setname_np(event_callback_checker_thread_.native_handle(),
                     "event-callback-checker");
  return 0;
}

void ViewController::Shutdown() {
  keep_running_ = false;
  if (event_callback_checker_thread_.joinable())
    event_callback_checker_thread_.join();
}

int ViewController::RegisterClient(int client_id, const std::string& client_ip,
                                   int client_port, int ebof_port) {
  if (callback_rpcs_.count(client_id) == 1) {
    LOG_WARN("Client %d already registered.\n", client_id);
    return 0;
  }
  auto rpc_client =
      std::make_unique<RpcClient<FLINT_RPC_SERVICE::LocalSchedulerService>>(
          client_ip, client_port);
  if (rpc_client->Connect() != 0) {
    LOG_ERROR("Failed to connect to the local scheduler of client %d: %s:%d.\n",
              client_id, client_ip.c_str(), client_port);
    return -1;
  }
  callback_rpcs_[client_id] = std::move(rpc_client);
  client_ebof_ports_[client_id] = ebof_port;
  NvmeofSession session{
      .hostname = client_ip, .ebof_port = ebof_port, .client_id = client_id};
  for (const auto& p : internal::g_ssd_id_map) {
    session.target_ssd_id = p.second;
    nvmeof_sessions_.push_back(session);
  }
  LOG_INFO("Connected to the local scheduler of client %d: %s:%d.\n", client_id,
           client_ip.c_str(), client_port);
  return 0;
}

int ViewController::UnregisterClient(int client_id) {
  if (callback_rpcs_.count(client_id) == 0) {
    LOG_WARN("Client %d not registered.\n", client_id);
    return 0;
  }
  callback_rpcs_.erase(client_id);
  client_ebof_ports_.erase(client_id);
  for (auto it = nvmeof_sessions_.begin(); it != nvmeof_sessions_.end();) {
    if (it->client_id == client_id) {
      it = nvmeof_sessions_.erase(it);
      continue;
    } else {
      ++it;
    }
  }
  UnregisterEventCallbacks(client_id);
  return 0;
}

int ViewController::RegisterEventCallbacks(
    const std::vector<EventCallback>& event_callbacks) {
  std::unique_lock lock(event_callbacks_mutex_);
  for (const auto& event_callback : event_callbacks) {
    if (callback_rpcs_.count(event_callback.client_id) == 0) {
      LOG_WARN(
          "Client %d not registered, event callback registration failed.\n",
          event_callback.client_id);
      return -1;
    }
    switch (event_callback.op) {
      case EventCallbackOp::kSsdAvailBwThreshold:
      case EventCallbackOp::kSsdFragDegreeThreshold:
      case EventCallbackOp::kSsdWriteCostThreshold:
      case EventCallbackOp::kIOPipeBwThreshold:
        if (shadow_view_.ssd_views.count(event_callback.ssd_id) == 0) {
          LOG_WARN(
              "Ssd %d not found, event callback registration failed. "
              "Skipped.\n",
              event_callback.ssd_id);
          continue;
        }
        break;
      case EventCallbackOp::kNetPortBwThreshold:
        if (shadow_view_.net_port_views.count(event_callback.ebof_port) == 0) {
          LOG_WARN(
              "Ebof port %d not found, event callback registration failed. "
              "Skipped.\n",
              event_callback.ebof_port);
          continue;
        }
        break;
      default:
        LOG_ERROR("Unimplemented event callback opcode: %d.\n",
                  event_callback.op);
        return -1;
    }
    event_callbacks_.push_back(event_callback);
  }
  return 0;
}

void ViewController::UnregisterEventCallbacks(int client_id) {
  std::unique_lock lock(event_callbacks_mutex_);
  for (auto it = event_callbacks_.begin(); it != event_callbacks_.end();) {
    if (it->client_id == client_id) {
      it = event_callbacks_.erase(it);
    } else {
      ++it;
    }
  }
  LOG_INFO("Unregistered event callbacks for client %d.\n", client_id);
}

int ViewController::push_view(const EventCallback& event_callback) {
  if (callback_rpcs_.count(event_callback.client_id) == 0)
    return -1;
  auto& rpc = callback_rpcs_.at(event_callback.client_id);
  auto stub = rpc->GetStub();
  if (stub == nullptr) {
    LOG_ERROR("Failed to get stub for client %d.\n", event_callback.client_id);
    return -1;
  }

  FLINT_RPC_MESSAGE::PushViewRequest request;
  FLINT_RPC_MESSAGE::PushViewResponse response;
  request.set_event_op(static_cast<int>(event_callback.op));
  switch (event_callback.op) {
    case EventCallbackOp::kSsdAvailBwThreshold:
    case EventCallbackOp::kSsdFragDegreeThreshold:
    case EventCallbackOp::kSsdWriteCostThreshold:
    case EventCallbackOp::kIOPipeBwThreshold:
      request.set_port_or_ssd(event_callback.ssd_id);
      break;
    case EventCallbackOp::kNetPortBwThreshold:
      request.set_port_or_ssd(event_callback.ebof_port);
      break;
    default:
      LOG_ERROR("Unimplemented event callback opcode: %d.\n",
                event_callback.op);
      return -1;
  }
  CopyViewToProto(client_ebof_ports_.at(event_callback.client_id), shadow_view_,
                  request.mutable_partial_view());
  ClientContext ctx;
  auto status = stub->PushView(&ctx, request, &response);
  if (!status.ok()) {
    LOG_ERROR("Failed to push view to client %d: %s.\n",
              event_callback.client_id, status.error_message().c_str());
    return -1;
  }
  return 0;
}

static bool CompareMetric(float metric, float threshold, CmpOp cmp_op) {
  if (cmp_op == CmpOp::kLessThan) {
    return metric < threshold;
  } else {
    return metric > threshold;
  }
}

void ViewController::event_callback_checker_do_work() {
  while (ARBITER_ISALIVE && keep_running_) {
    std::shared_lock lk(event_callbacks_mutex_);

    for (const auto& event_callback : event_callbacks_) {
      bool trigger_push = false;

      switch (event_callback.op) {
        case EventCallbackOp::kSsdAvailBwThreshold: {
          const auto& ssd_view =
              shadow_view_.ssd_views.at(event_callback.ssd_id);
          float avail_bw;
          if (event_callback.io_type == IOType::kRead) {
            avail_bw = ssd_view.read_bw_free;
          } else {
            avail_bw = ssd_view.write_bw_free;
          }
          trigger_push = CompareMetric(avail_bw, event_callback.threshold,
                                       event_callback.cmp_op);
          break;
        }
        case EventCallbackOp::kSsdFragDegreeThreshold: {
          const auto& ssd_view =
              shadow_view_.ssd_views.at(event_callback.ssd_id);
          trigger_push =
              CompareMetric(ssd_view.frag_degree, event_callback.threshold,
                            event_callback.cmp_op);
          break;
        }
        case EventCallbackOp::kSsdWriteCostThreshold: {
          const auto& ssd_view =
              shadow_view_.ssd_views.at(event_callback.ssd_id);
          trigger_push =
              CompareMetric(ssd_view.write_cost, event_callback.threshold,
                            event_callback.cmp_op);
          break;
        }
        case EventCallbackOp::kIOPipeBwThreshold: {
          const auto& io_pipe_view =
              shadow_view_.io_pipe_views.at(event_callback.ssd_id);
          float bw;
          if (event_callback.io_type == IOType::kRead) {
            bw = io_pipe_view.read_bw;
          } else {
            bw = io_pipe_view.write_bw;
          }
          trigger_push = CompareMetric(bw, event_callback.threshold,
                                       event_callback.cmp_op);
          break;
        }
        case EventCallbackOp::kNetPortBwThreshold: {
          const auto& net_port_view =
              shadow_view_.net_port_views.at(event_callback.ebof_port);
          float bw;
          if (event_callback.io_type == IOType::kRead) {
            bw = net_port_view.read_bw;
          } else {
            bw = net_port_view.write_bw;
          }
          trigger_push = CompareMetric(bw, event_callback.threshold,
                                       event_callback.cmp_op);
          break;
        }
        default:
          // unimplemented event callback. skip.
          break;
      }

      if (trigger_push) {
        push_view(event_callback);
      }
    }

    DetectBottleneck();
    lk.unlock();

    std::this_thread::sleep_for(
        std::chrono::milliseconds(kEventCallbackCheckerInterval));
  }
}

void ViewController::DetectBottleneck() {
  std::shared_lock lk(view_mutex_);
  std::set<int> congested_ssds;
  std::set<int> congested_iopipes;
  std::set<int> congested_netpipes;
  std::vector<NvmeofSession> victim_sessions;
  for (const auto& p : shadow_view_.ssd_views) {
    const auto& ssd_view = p.second;
    if (ssd_view.AvailBw() < kBottleneckThreshold) {
      congested_ssds.insert(ssd_view.ssd_id);
      congested_iopipes.insert(ssd_view.ssd_id % internal::g_nic_ports_num);
    }
  }
  for (const auto& session : nvmeof_sessions_) {
    if (congested_ssds.count(session.target_ssd_id) == 1) {
      congested_netpipes.insert(session.ebof_port);
      victim_sessions.push_back(session);
    }
  }
  for (const auto& session : nvmeof_sessions_) {
    if (congested_netpipes.count(session.ebof_port) == 1) {
      bool dup = false;
      for (const auto& v : victim_sessions) {
        if (v.client_id == session.client_id && v.target_ssd_id == session.target_ssd_id)
          dup = true;
      }
      if (!dup) {
        victim_sessions.push_back(session);
      }
    }
  }

  std::set<int> victim_clients;
  for (const auto& session : victim_sessions) {
    if (victim_clients.count(session.client_id) == 1) continue;
    victim_clients.insert(session.client_id);
  
    auto& rpc_client = callback_rpcs_.at(session.client_id);
    auto stub = rpc_client->GetStub();
    if (stub == nullptr) {
      LOG_ERROR("Failed to get stub for client %d.\n", session.client_id);
      continue;
    }
    FLINT_RPC_MESSAGE::BottleneckReportRequest request;
    FLINT_RPC_MESSAGE::BottleneckReportResponse response;
    for (const auto& ssd_id : congested_ssds) {
      request.add_congested_ssds(ssd_id);
    }
    ClientContext ctx;
    auto status = stub->BottleneckReport(&ctx, request, &response);
    if (!status.ok()) {
      LOG_ERROR("Failed to report bottleneck to client %d: %s.\n",
                session.client_id, status.error_message().c_str());
      continue;
    }
  }
}

int ViewController::ReportCmpl(const std::vector<IOCmplInfo>& cmpls) {
  std::unique_lock lock(view_mutex_);

  for (const auto& cmpl : cmpls) {
    if (shadow_view_.net_port_views.count(cmpl.ebof_port) == 0) {
      LOG_ERROR("Unknown ebof port: %d.\n", cmpl.ebof_port);
      return -1;
    }
    auto& net_port_view = shadow_view_.net_port_views.at(cmpl.ebof_port);
    auto& net_pipe_view = shadow_view_.net_pipe_views.at(cmpl.ebof_port);
    net_port_view.RecordIO(cmpl.iolen, cmpl.io_type);
    net_pipe_view.RecordIO(cmpl.iolen, cmpl.io_type);

    if (shadow_view_.ssd_views.count(cmpl.ssd_id) == 0) {
      LOG_ERROR("Unknown ssd id: %d.\n", cmpl.ssd_id);
      return -1;
    }
    auto& io_port_view =
        shadow_view_.io_port_views.at(cmpl.ssd_id % internal::g_nic_ports_num);
    io_port_view.RecordIO(cmpl.iolen, cmpl.io_type);
    auto& io_pipe_view = shadow_view_.io_pipe_views.at(cmpl.ssd_id);
    io_pipe_view.RecordIO(cmpl.iolen, cmpl.io_type);
    auto& ssd_view = shadow_view_.ssd_views.at(cmpl.ssd_id);
    ssd_view.RecordIO(cmpl.iolen, cmpl.io_type, cmpl.lat_us);
  }

  return 0;
}

int ViewController::GetViewRecency(
    int ebof_port, FLINT_RPC_MESSAGE::ViewRecency* view_recency) {
  std::unique_lock lock(view_mutex_);
  CopyViewRecencyToProto(ebof_port, shadow_view_, view_recency);
  return 0;
}

int ViewController::GetPartialView(
    int ebof_port, FLINT_RPC_MESSAGE::PartialView* partial_view) {
  std::unique_lock lock(view_mutex_);
  CopyViewToProto(ebof_port, shadow_view_, partial_view);
  return 0;
}

}  // namespace arbiter
}  // namespace flint