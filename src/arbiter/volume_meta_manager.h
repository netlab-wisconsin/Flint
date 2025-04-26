#pragma once

#include "db.h"
#include "common/io_defs.h"
#include "common/interface.h"

namespace flint {
namespace arbiter {

/**
 * @brief VolumeMetaManager is responsible for managing metadata of volume
 * control-path operations, e.g. create/open/close/delete. Volume data-path
 * operations are handled by IOExecutor in the client library.
*/
class VolumeMetaManager {
 private:
  std::shared_ptr<DBClient> db_client_;

 public:
  VolumeMetaManager();

  /// Update the volume's metadata into RocksDB
  void CreateVolume(const VolumeOptions &options,
                    const VolumeExtLocatorMap &ext_locator_map);
  
  void UpdateVolumeExts(const std::string& vol_name,
                        const VolumeExtLocatorMap& ext_locator_map);

  /// Delete the volume's metadata from RocksDB
  /// Needs uuid set
  void DeleteVolume(const std::string &vol_name);

  /// Check if the volume name exists in RocksDB
  bool VolumeExists(const std::string &vol_name);

  void GetVolumeOptions(const std::string &vol_name, VolumeOptions &options);

  void GetExtLocatorMap(const std::string& name, 
                       VolumeExtLocatorMap& ext_locator_map);

  void GetVolumeNames(std::vector<std::string> &names);
};

}  // namespace arbiter
}  // namespace flint