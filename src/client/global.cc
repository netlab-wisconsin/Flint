#include <cstddef>
#include "common/logging.h"
#include "common/utils/timing.h"
#include "flint.h"
#include "internal.h"
#include "local_scheduler.h"
#include "volume_agent.h"

namespace flint {
namespace env {
static bool g_flint_inited = false;

static void Reset() {
  g_flint_inited = false;
  internal::g_volume_agent = nullptr;
  internal::g_local_scheduler = nullptr;
  g_config = {};
}

int Init(FlintConfig& config) {
  int ret = 0;
  if (g_flint_inited)
    return 0;

  internal::g_freq_ghz = measure_rdtsc_freq();
  internal::g_volume_agent = std::make_unique<VolumeAgent>();
  ret = internal::g_volume_agent->Init();
  if (ret != 0) {
    LOG_ERROR("Failed to create volume agent.\n");
    Reset();
    return ret;
  }
  LOG_INFO("Volume agent created successfully.\n");

  internal::g_local_scheduler = std::make_unique<LocalScheduler>();
  ret = internal::g_local_scheduler->Init();
  if (ret != 0) {
    // TODO: We should be able to continue without local scheduler
    // in a degraded mode.
    LOG_ERROR("Failed to create local scheduler.\n");
    Reset();
    return ret;
  }
  LOG_INFO("Local scheduler created successfully.\n");

  g_config = config;
  g_flint_inited = true;

  return 0;
}

int Close() {
  if (!g_flint_inited)
    return 0;

  internal::g_local_scheduler->Shutdown();
  internal::g_volume_agent->Shutdown();

  internal::g_volume_agent.reset();
  internal::g_local_scheduler.reset();
  g_flint_inited = false;

  g_config = {};
  return 0;
}

int UpdateSlo(SloLevel slo_level) {
  if (!g_flint_inited)
    return -1;
  g_config.slo_level = slo_level;
  int ret = internal::g_local_scheduler->UpdateSlo(slo_level);
  if (ret != 0) {
    LOG_ERROR("Failed to update slo level.\n");
    return ret;
  }
  return 0;
}

}  // namespace env
}  // namespace flint