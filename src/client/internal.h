#pragma once

#include "common/rpc.h"
#include "common/ssd.h"

namespace flint {
namespace internal {

#define THREAD_HIGHESTPRIO -20

inline int g_client_id;

inline SsdIdMap g_ssd_id_map;
inline int g_ebof_port;
inline int g_nic_ports_num;

inline double g_freq_ghz = 0;

inline volatile bool g_shutdown_requested = false;

#define FLINT_ISALIVE (!flint::internal::g_shutdown_requested)

#define FLINT_FATAL()                             \
  do {                                            \
    flint::internal::g_shutdown_requested = true; \
  } while (0)

#define FLINT_ALIVE_OR_RET(ret) \
  do {                          \
    if (!FLINT_ISALIVE)         \
      return ret;               \
  } while (0)

}  // namespace internal
}  // namespace flint