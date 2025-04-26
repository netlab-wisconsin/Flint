#include "volume_meta_manager.h"
#include <mutex>
#include "common/utils/json.hpp"
#include "common/utils/misc.h"
#include "db.h"
#include "internal.h"

namespace flint {
namespace arbiter {

using nlohmann::json;

#define KEY_VOLUME_META "volume_meta"
#define KEY_VOLUME_ATTR_EXT_LOCATORS "ext_locators"
#define KEY_VOLUME_ATTR_SIZE "size"
#define KEY_VOLUME_ATTR_REP_FACTOR "rep_factor"
#define KEY_VOLUME_ATTR_FLAGS "flags"

VolumeMetaManager::VolumeMetaManager() : db_client_(internal::g_db_client) {}

void VolumeMetaManager::CreateVolume(
    const VolumeOptions &options, const VolumeExtLocatorMap &ext_locator_map) {
  std::string cfname = MakeKey(KEY_VOLUME_META, options.name);
  db_client_->CreateCf(cfname);

  auto batch = db_client_->BeginWriteBatch();
  batch.Put(cfname, MakeKey(KEY_VOLUME_ATTR_SIZE),
            std::to_string(options.size));
  batch.Put(cfname, MakeKey(KEY_VOLUME_ATTR_REP_FACTOR),
            std::to_string(options.rep_factor));
  batch.Put(cfname, MakeKey(KEY_VOLUME_ATTR_FLAGS),
            std::to_string(options.flags));
  for (const auto &[ext_id, locators] : ext_locator_map) {
    json j = locators;
    batch.Put(cfname,
              MakeKey(KEY_VOLUME_ATTR_EXT_LOCATORS, std::to_string(ext_id)),
              j.dump());
  }
  rt_assert(batch.Commit(), "failed to create volume");
}

void VolumeMetaManager::UpdateVolumeExts(
    const std::string &vol_name, const VolumeExtLocatorMap &ext_locator_map) {
  std::string cfname = MakeKey(KEY_VOLUME_META, vol_name);
  auto batch = db_client_->BeginWriteBatch();
  for (const auto &[ext_id, locators] : ext_locator_map) {
    json j = locators;
    batch.Put(cfname,
              MakeKey(KEY_VOLUME_ATTR_EXT_LOCATORS, std::to_string(ext_id)),
              j.dump());
  }
  rt_assert(batch.Commit(), "failed to update volume");
}

void VolumeMetaManager::DeleteVolume(const std::string &vol_name) {
  rt_assert(db_client_->DropCf(MakeKey(KEY_VOLUME_META, vol_name)),
            "failed to delete volume");
}

void VolumeMetaManager::GetVolumeOptions(const std::string &vol_name,
                                         VolumeOptions &options) {
  std::string cfname = MakeKey(KEY_VOLUME_META, vol_name);
  std::string val;
  db_client_->Get(cfname, MakeKey(KEY_VOLUME_ATTR_SIZE), val);
  options.size = std::stoul(val);
  db_client_->Get(cfname, MakeKey(KEY_VOLUME_ATTR_REP_FACTOR), val);
  options.rep_factor = std::stoi(val);
  db_client_->Get(cfname, MakeKey(KEY_VOLUME_ATTR_FLAGS), val);
}

void VolumeMetaManager::GetExtLocatorMap(const std::string &vol_name,
                                         VolumeExtLocatorMap &ext_locator_map) {
  std::string cfname = MakeKey(KEY_VOLUME_META, vol_name);
  std::string ext_id_str;
  std::string value;
  std::string key_prefix = MakeKey(KEY_VOLUME_ATTR_EXT_LOCATORS);
  std::vector<std::string> keys;
  std::vector<std::string> values;
  db_client_->GetWithPrefix(cfname, key_prefix, keys, values);
  for (size_t i = 0; i < keys.size(); ++i) {
    ext_id_str = ExtractSingleKey(keys[i], key_prefix);
    json j = json::parse(values[i]);
    ext_locator_map[std::stoul(ext_id_str)] = j;
  }
}

bool VolumeMetaManager::VolumeExists(const std::string &vol_name) {
  return db_client_->CfExists(MakeKey(KEY_VOLUME_META, vol_name));
}

void VolumeMetaManager::GetVolumeNames(std::vector<std::string> &names) {
  auto cfs = db_client_->GetCfNames();
  for (const auto &cf : cfs) {
    auto name = ExtractSingleKey(cf, KEY_VOLUME_META);
    if (!name.empty())
      names.push_back(name);
  }
}
}  // namespace arbiter
}  // namespace flint