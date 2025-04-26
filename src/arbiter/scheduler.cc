#include "scheduler.h"
#include <chrono>
#include <cstddef>
#include <memory>
#include "common/io_defs.h"
#include "common/utils/misc.h"
#include "ssd_manager.h"

namespace flint {
namespace arbiter {

inline constexpr size_t kEpochInterval = 50;  // millisecond
inline constexpr int kMaxIdleEpochs = 3;

void DRR::refresh_drr_lists() {
  for (auto& p : clients_) {
    auto& client_id = p.first;
    auto& client = p.second;
    if (client->state == ClientState::kActive &&
        client->last_active_epoch > 0) {
      assert(active_clients_.count(client_id) == 1);
      if (epoch_ - client->last_active_epoch > kMaxIdleEpochs) {
        client->state = ClientState::kIdle;
        idle_clients_.insert(std::make_pair(client_id, client));
        active_clients_.erase(client_id);
      }
    } else if (client->state == ClientState::kIdle) {
      assert(idle_clients_.count(client_id) == 1);
      if (client->last_active_epoch == epoch_) {
        client->state = ClientState::kActive;
        active_clients_.insert(std::make_pair(client_id, client));
        idle_clients_.erase(client_id);
      }
    }
  }
}

void DRR::estimate_avail_bw(const ShadowView& shadow_view) {
  avail_bw_ = shadow_view.ssd_views.at(ssd_id_).AvailBw();
}

void DRR::reassign_quota() {
  size_t bdp = avail_bw_ * kEpochInterval / 1000;
  int total_weights = 0;
  for (const auto& p : active_clients_) {
    auto& client = p.second;
    total_weights += static_cast<int>(client->slo_level);
  }
  for (const auto& p : active_clients_) {
    auto& client = p.second;
    client->current_quota =
        bdp * static_cast<int>(client->slo_level) / total_weights;
    client->current_epoch = epoch_;
    LOG_DEBUG("SSD %d: Reassigned quota for client %d, quota: %lu.\n", ssd_id_,
              client->client_id, client->current_quota);
  }
}

void DRR::RegisterClient(int client_id, SloLevel slo_level) {
  std::lock_guard lk{mu_};
  auto client = std::make_shared<Client>();
  client->client_id = client_id;
  client->current_epoch = epoch_;
  client->current_quota = 0;
  client->slo_level = slo_level;
  client->state = ClientState::kActive;
  clients_.insert(std::make_pair(client_id, client));
  active_clients_.insert(std::make_pair(client_id, client));
}

void DRR::UnregisterClient(int client_id) {
  std::lock_guard lk{mu_};
  auto it = clients_.find(client_id);
  rt_assert(it != clients_.end());
  auto& client = it->second;
  clients_.erase(it);
  if (client->state == ClientState::kActive) {
    active_clients_.erase(client_id);
  } else {
    idle_clients_.erase(client_id);
  }
}

void DRR::UpdateClientSlo(int client_id, SloLevel slo_level) {
  std::lock_guard lk{mu_};
  auto& client = clients_.at(client_id);
  client->slo_level = slo_level;
}

void DRR::epoch_advance() {
  epoch_++;
  auto now = GetCurrentTimeMicro();
  next_epoch_start_time_ = now + kEpochInterval * 1000;
}

void DRR::RunEpoch(const ShadowView& shadow_view) {
  std::lock_guard lk{mu_};
  epoch_advance();
  estimate_avail_bw(shadow_view);
  refresh_drr_lists();
  reassign_quota();
}

int DRR::RequestIOSlice(int client_id, IOSlice& io_slice, size_t& waittime) {
  std::lock_guard lk{mu_};
  auto& client = clients_.at(client_id);
  auto last_epoch = io_slice.epoch;
  assert(last_epoch <= epoch_);
  if (last_epoch == epoch_) {
    auto now = GetCurrentTimeMicro();
    if ((size_t)now < next_epoch_start_time_) {
      waittime = next_epoch_start_time_ - now;
    } else {
      waittime = 0;
    }
  } else {
    waittime = 0;
    io_slice.epoch = epoch_;
    io_slice.size_allocated = client->current_quota;
  }
  return 0;
}

void Scheduler::drr_routine() {
  while (ARBITER_ISALIVE) {
    auto shadow_view = view_controller_->GetShadowView();
    for (auto& p : drrs_) {
      auto& drr = p.second;
      drr.RunEpoch(shadow_view);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(kEpochInterval));
  }
}

bool Scheduler::RegisterClient(int client_id, SloLevel slo_level,
                               const std::string& client_ip, int client_port,
                               int ebof_port) {
  std::lock_guard lk{mu_};
  if (registered_clients_.count(client_id) == 1) {
    LOG_WARN("Client %d already registered.\n", client_id);
    return true;
  }
  for (auto& p : drrs_) {
    p.second.RegisterClient(client_id, slo_level);
  }
  registered_clients_.insert(client_id);
  view_controller_->RegisterClient(client_id, client_ip, client_port,
                                   ebof_port);
  return true;
}

bool Scheduler::UnregisterClient(int client_id) {
  std::lock_guard lk{mu_};
  if (registered_clients_.count(client_id) == 0) {
    LOG_WARN("Client %d not registered.\n", client_id);
    return true;
  }
  for (auto& p : drrs_) {
    p.second.UnregisterClient(client_id);
  }
  registered_clients_.erase(client_id);
  view_controller_->UnregisterClient(client_id);
  return true;
}

bool Scheduler::UpdateClientSlo(int client_id, SloLevel slo_level) {
  std::lock_guard lk{mu_};
  if (registered_clients_.count(client_id) == 0) {
    LOG_WARN("Client %d not registered.\n", client_id);
    return false;
  }
  for (auto& p : drrs_) {
    p.second.UpdateClientSlo(client_id, slo_level);
  }
  return true;
}

Scheduler::Scheduler(SsdPoolManager& ssd_pool_manager) {
  view_controller_ = std::make_shared<ViewController>();
  for (const auto& p : internal::g_ssd_id_map) {
    drrs_.emplace(p.second, p.second);
  }
}

int Scheduler::Init() {
  int ret = view_controller_->Init();
  return ret;
}

void Scheduler::Shutdown() {
  view_controller_->Shutdown();
  if (drr_routine_thread_.joinable())
    drr_routine_thread_.join();
}

void Scheduler::Run() {
  drr_routine_thread_ = std::thread([this] { drr_routine(); });
  pthread_setname_np(drr_routine_thread_.native_handle(), "drr");
}

int Scheduler::RequestIOSlice(int client_id, IOSlice& io_slice,
                              size_t& waittime) {
  auto ssd_id = io_slice.ssd_id;
  auto& drr = drrs_.at(ssd_id);
  int ret = drr.RequestIOSlice(client_id, io_slice, waittime);
  return ret;
}

int Scheduler::ReportCmpl(const std::vector<IOCmplInfo>& cmpls) {
  int ret = view_controller_->ReportCmpl(cmpls);
  if (ret != 0) {
    LOG_ERROR("Failed to report completions to view controller.\n");
    return ret;
  }
  std::unordered_set<int> ssd_ids;
  for (const auto& cpl : cmpls) {
    ssd_ids.insert(cpl.ssd_id);
  }
  int client_id = cmpls.front().client_id;
  for (const auto& ssd_id : ssd_ids) {
    drrs_.at(ssd_id).CompleteIO(client_id);
  }
  return 0;
}

int Scheduler::RegisterEventCallbacks(
    const std::vector<EventCallback>& event_callbacks) {
  return view_controller_->RegisterEventCallbacks(event_callbacks);
}

}  // namespace arbiter
}  // namespace flint
