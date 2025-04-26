#include "internal.h"

namespace flint {
namespace arbiter {
namespace internal {

#define ARBITER_METADATA_CF "flint_arbiter"
#define ARBITER_METADATA_KEY_SSD_ID_MAP "ssd_id_map"

int LoadArbiterMetadata() {
  std::string cfname = MakeKey(ARBITER_METADATA_CF);
  if (!g_db_client->CfExists(cfname)) {
    LOG_WARN("Arbiter metadata column family not found, a fresh run?\n");
    return -1;
  }
  std::string val_str;
  bool ok = g_db_client->Get(cfname, ARBITER_METADATA_KEY_SSD_ID_MAP, val_str);
  if (!ok) {
    LOG_WARN("Unable to find arbiter metadata, a fresh run?\n");
    return -1;
  }
  g_ssd_id_map = nlohmann::json::parse(val_str);
  return 0;
}

int PersistArbiterMetadata() {
  std::string cfname = MakeKey(ARBITER_METADATA_CF);
  if (!g_db_client->CfExists(cfname)) {
    if (!g_db_client->CreateCf(cfname)) {
      LOG_ERROR("Failed to create arbiter metadata column family");
      return -1;
    }
  }
  auto batch = g_db_client->BeginWriteBatch();
  batch.Put(cfname, ARBITER_METADATA_KEY_SSD_ID_MAP,
            nlohmann::json(g_ssd_id_map).dump());
  return batch.Commit() ? 0 : -1;
}

}  // namespace internal
}  // namespace arbiter
}  // namespace flint
