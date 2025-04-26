#pragma once

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include "common/rpc.h"
#include "common/ssd.h"
#include "db.h"

namespace flint {
namespace arbiter {
namespace internal {

inline SsdIdMap g_ssd_id_map;

inline int g_volume_service_port = 31850;
inline int g_scheduler_service_port = 31851;

#define ROCKSDB_HOME "/tmp/flint_db"

inline std::shared_ptr<DBClient> g_db_client;


// hostname -> netport
inline std::map<std::string, uint32_t> g_ebof_port_map;
inline int g_nic_ports_num = 0;

inline volatile bool g_arbiter_shutdown_requested = false;

#define ARBITER_ISALIVE \
  (!flint::arbiter::internal::g_arbiter_shutdown_requested)

#define ARBITER_SHUTDOWN()                                         \
  do {                                                             \
    LOG_FATAL("Fatal error occurred. Exiting.\n");                 \
    flint::arbiter::internal::g_arbiter_shutdown_requested = true; \
  } while (0)

int LoadArbiterMetadata();

int PersistArbiterMetadata();

bool ArbiterInternalInited();

}  // namespace internal
}  // namespace arbiter
};  // namespace flint