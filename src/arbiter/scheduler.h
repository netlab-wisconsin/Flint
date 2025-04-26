#pragma once
#include "common/interface.h"
#include "common/io_defs.h"
#include "common/shadow_view.h"
#include "ssd_manager.h"
#include "view_controller.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace flint {
namespace arbiter {
enum class ClientState {
  kActive,
  kIdle,
};

struct Client {
  int client_id;
  uint32_t current_epoch;
  uint32_t last_active_epoch;
  size_t current_quota;
  SloLevel slo_level;
  ClientState state;
};

using ClientPtr = std::shared_ptr<Client>;

class DRR {
 private:
  int ssd_id_;
  size_t avail_bw_;

  // Run Deficit Round-Robin (DRR) on a per-epoch basis.
  std::atomic<uint32_t> epoch_ = 0;
  size_t next_epoch_start_time_ = 0;

  std::map<int, ClientPtr> clients_;
  std::map<int, ClientPtr> active_clients_;
  std::map<int, ClientPtr> idle_clients_;
  std::mutex mu_;

  void epoch_advance();

  void estimate_avail_bw(const ShadowView& shadow_view);

  void reassign_quota();

  void refresh_drr_lists();

 public:
  DRR(int ssd_id) : ssd_id_(ssd_id) {}

  void RegisterClient(int client_id, SloLevel slo_level);

  void UnregisterClient(int client_id);

  void UpdateClientSlo(int client_id, SloLevel slo_level);

  void RunEpoch(const ShadowView& shadow_view);

  int RequestIOSlice(int client_id, IOSlice& io_slice, size_t& waittime);

  void CompleteIO(int client_id) {
    if (clients_.count(client_id) == 0)
      return;
    clients_.at(client_id)->last_active_epoch = epoch_;
  }

  std::pair<uint64_t, size_t> NextEpoch() const;
};

class Scheduler {
 private:
  std::set<int> registered_clients_;
  std::mutex mu_;

  std::map<int, DRR> drrs_;
  std::thread drr_routine_thread_;

  std::shared_ptr<ViewController> view_controller_;

  void drr_routine();

 public:
  friend class SchedulerService;

  Scheduler(SsdPoolManager& ssd_pool_manager);

  int Init();

  void Run();

  void Shutdown();

  auto GetViewController() const { return view_controller_; }

  bool RegisterClient(int client_id, SloLevel slo_level,
                      const std::string& client_ip, int client_port,
                      int ebof_port);

  bool UnregisterClient(int client_id);

  bool UpdateClientSlo(int client_id, SloLevel slo_level);

  int RequestIOSlice(int client_id, IOSlice& io_slice, size_t& waittime);

  int ReportCmpl(const std::vector<IOCmplInfo>& cmpls);

  int GetPartialView(int ebof_port,
                     FLINT_RPC_MESSAGE::PartialView* partial_view) {
    return view_controller_->GetPartialView(ebof_port, partial_view);
  }

  int GetViewRecency(int ebof_port,
                     FLINT_RPC_MESSAGE::ViewRecency* view_recency) {
    return view_controller_->GetViewRecency(ebof_port, view_recency);
  }

  int RegisterEventCallbacks(const std::vector<EventCallback>& event_callbacks);
};

}  // namespace arbiter
}  // namespace flint