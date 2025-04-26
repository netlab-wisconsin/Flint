#pragma once

#include <cstddef>
#include <set>
#include <shared_mutex>
#include <thread>
#include <vector>
#include "common/grpc/message.pb.h"
#include "common/grpc/service.grpc.pb.h"
#include "common/interface.h"
#include "common/rpc.h"
#include "common/shadow_view.h"

namespace flint {
namespace arbiter {

struct IOCmplInfo {
  int client_id;
  int ebof_port;
  int ssd_id;
  IOType io_type;
  uint32_t lat_us;
  uint32_t iolen;

  IOCmplInfo(int client_id, int ebof_port, int ssd_id, IOType io_type,
             uint32_t lat_us, uint32_t iolen)
      : client_id(client_id),
        ebof_port(ebof_port),
        ssd_id(ssd_id),
        io_type(io_type),
        lat_us(lat_us),
        iolen(iolen) {}
};

class ViewController {
 private:
  ShadowView shadow_view_;
  std::shared_mutex view_mutex_;
  std::map<int,
           std::unique_ptr<RpcClient<FLINT_RPC_SERVICE::LocalSchedulerService>>>
      callback_rpcs_;
  std::map<int, int> client_ebof_ports_;
  std::vector<NvmeofSession> nvmeof_sessions_;

  std::vector<EventCallback> event_callbacks_;
  std::shared_mutex event_callbacks_mutex_;
  volatile bool keep_running_ = true;

  void check_callback_events();

  std::thread event_callback_checker_thread_;
  constexpr static int kEventCallbackCheckerInterval = 100;  // ms
  constexpr static size_t kBottleneckThreshold = 1500000000;

  void event_callback_checker_do_work();

  int push_view(const EventCallback& event_callback);

 public:
  ViewController() = default;

  ~ViewController() = default;

  int Init();

  void Shutdown();

  void DetectBottleneck();

  int RegisterClient(int client_id, const std::string& client_ip,
                     int client_port, int ebof_port);

  int UnregisterClient(int client_id);

  int RegisterEventCallbacks(const std::vector<EventCallback>& event_callbacks);

  void UnregisterEventCallbacks(int client_id);

  int ReportCmpl(const std::vector<IOCmplInfo>& cmpls);

  int GetPartialView(int client_id,
                     FLINT_RPC_MESSAGE::PartialView* partial_view);

  int GetViewRecency(int ebof_port,
                     FLINT_RPC_MESSAGE::ViewRecency* view_recency);

  auto GetShadowView() {
    std::shared_lock lock(view_mutex_);
    return shadow_view_;
  }
};

}  // namespace arbiter
}  // namespace flint
