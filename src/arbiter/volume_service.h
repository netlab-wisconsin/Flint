#pragma once
#include <mutex>
#include <thread>
#include "common/rpc.h"
#include "internal.h"
#include "ssd_manager.h"
#include "volume_meta_manager.h"

namespace flint {
namespace arbiter {

// Forward declarations
class ExtAllocator;
class VolumeService;

class RpcVolumeServiceImpl final
    : public FLINT_RPC_SERVICE::VolumeService::Service {
 private:
  VolumeService *vol_service_;

 public:
  RpcVolumeServiceImpl(VolumeService *vol_service)
      : vol_service_(vol_service) {}

  Status GetFlintMetadata(
      ServerContext *context,
      const FLINT_RPC_MESSAGE::GetFlintMetadataRequest *request,
      FLINT_RPC_MESSAGE::GetFlintMetadataResponse *resp) override;

  Status CreateVolume(ServerContext *context,
                      const FLINT_RPC_MESSAGE::CreateVolumeRequest *request,
                      FLINT_RPC_MESSAGE::CreateVolumeResponse *resp) override;

  Status OpenVolume(ServerContext *context,
                    const FLINT_RPC_MESSAGE::OpenVolumeRequest *request,
                    FLINT_RPC_MESSAGE::OpenVolumeResponse *resp) override;

  Status DeleteVolume(ServerContext *context,
                      const FLINT_RPC_MESSAGE::DeleteVolumeRequest *request,
                      FLINT_RPC_MESSAGE::DeleteVolumeResponse *resp) override;

  Status ListVolumes(ServerContext *context,
                     const FLINT_RPC_MESSAGE::ListVolumeRequest *request,
                     FLINT_RPC_MESSAGE::ListVolumeResponse *resp) override;

  Status VolumeExtentFault(
      ServerContext *context,
      const FLINT_RPC_MESSAGE::VolumeExtentFaultRequest *request,
      FLINT_RPC_MESSAGE::VolumeExtentFaultResponse *resp) override;
};

/**
 * @brief RPC service for volume operations
*/
class VolumeService {
 private:
  RpcServer<RpcVolumeServiceImpl> *rpc_server_;

  SsdPoolManager &ssd_pool_manager_;
  VolumeMetaManager *vol_meta_manager_;
  ExtAllocator *ext_allocator_;
  std::mutex vol_mu_;

  std::thread loop_thread_;

  template <class Resp>
  void rpc_error(Resp *resp, VolumeOpErrCode err_code);

  template <class Resp>
  void rpc_error(Resp *resp, VolumeOpErrCode err_code,
                 const std::string &err_message);

  void load_extent_usage_map();

 public:
  VolumeService(const std::string &ip, int port,
                SsdPoolManager &ssd_pool_manager);

  int Init() { return 0; }

  ~VolumeService();

  ExtAllocator *GetExtAllocator() const { return ext_allocator_; }

  void Run();

  void Shutdown();

  void GetFlintMetadata(const FLINT_RPC_MESSAGE::GetFlintMetadataRequest *req,
                        FLINT_RPC_MESSAGE::GetFlintMetadataResponse *resp);

  void CreateVolume(const FLINT_RPC_MESSAGE::CreateVolumeRequest *req,
                    FLINT_RPC_MESSAGE::CreateVolumeResponse *resp);

  void OpenVolume(const FLINT_RPC_MESSAGE::OpenVolumeRequest *req,
                  FLINT_RPC_MESSAGE::OpenVolumeResponse *resp);

  void DeleteVolume(const FLINT_RPC_MESSAGE::DeleteVolumeRequest *req,
                    FLINT_RPC_MESSAGE::DeleteVolumeResponse *resp);

  void ListVolumes(const FLINT_RPC_MESSAGE::ListVolumeRequest *req,
                   FLINT_RPC_MESSAGE::ListVolumeResponse *resp);

  void VolumeExtentFault(const FLINT_RPC_MESSAGE::VolumeExtentFaultRequest *req,
                         FLINT_RPC_MESSAGE::VolumeExtentFaultResponse *resp);
};

}  // namespace arbiter
}  // namespace flint