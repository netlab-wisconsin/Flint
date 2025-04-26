#pragma once
#include <cstddef>
#include <vector>
#include "common/rpc.h"
#include "common/ssd.h"
#include "volume.h"

namespace flint {

/**
 * @brief An agent that dispatches volume-related operations to the remote
 * volume service (arbiter).
*/
class VolumeAgent {
 private:
  std::shared_ptr<RpcClient<FLINT_RPC_SERVICE::VolumeService>> rpc_client_;

  std::mutex mu_;  ///< Mutex to ensure thread-safety of volume's operations
  SsdPool ssd_pool_;

  int get_flint_metadata();

 public:
  VolumeAgent();

  int Init();

  void Shutdown() {}

  int CreateVolume(VolumeOptions& options);

  std::shared_ptr<Volume> OpenVolume(VolumeOptions& options);

  /// @brief Handle volume extent fault.
  ///
  /// @param vol The volume to handle extent fault.
  /// @param lexts The missing logical extent numbers.
  /// @return The extent locators of the volume.
  int HandleVolumeExtentFault(Volume& vol, const std::vector<size_t>& lexts);

  int DeleteVolume(const std::string& vol_name);

  int ListVolume(const std::string& vol_name, VolumeOptions& options);

  int ListAllVolumes(std::vector<VolumeOptions>& vols);
};

namespace internal {
inline std::unique_ptr<VolumeAgent> g_volume_agent;
}  // namespace internal

};  // namespace flint