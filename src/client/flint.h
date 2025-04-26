#pragma once

#include "common/interface.h"
#include "io_executor.h"
#include "volume.h"

namespace flint {
namespace env {
class FlintConfig {
 public:
  std::string arbiter_ip = "128.105.146.65";
  int volume_service_port = 31850;
  int scheduler_service_port = 31851;

  std::string local_scheduler_ip = "128.105.146.65";
  int local_scheduler_port = 31852;

  int sync_view_interval_ms = 100;
  SloLevel slo_level = SloLevel::MEDIUM;
};

inline FlintConfig g_config;

int Init(FlintConfig& config);

int Close();

int UpdateSlo(SloLevel slo_level);
}  // namespace env
};  // namespace flint