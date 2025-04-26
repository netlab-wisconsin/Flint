#include "volume.h"
#include "common/ssd.h"
#include "io_executor.h"
#include "volume_agent.h"

namespace flint {

using internal::g_volume_agent;

int Volume::Create(VolumeOptions& options) {
  FLINT_ALIVE_OR_RET(-1);
  return g_volume_agent->CreateVolume(options);
}

std::shared_ptr<Volume> Volume::Open(VolumeOptions& options) {
  FLINT_ALIVE_OR_RET(nullptr);
  auto vol = g_volume_agent->OpenVolume(options);
  if (vol == nullptr)
    return vol;
  return vol;
}

int Volume::Delete() {
  FLINT_ALIVE_OR_RET(-1);
  return Delete(name_);
}

int32_t Volume::ExtentCount() const {
  return ROUND_UP(size_, kExtentSize) / kExtentSize;
}

int Volume::Delete(std::string name) {
  FLINT_ALIVE_OR_RET(-1);
  return g_volume_agent->DeleteVolume(name);
}

int Volume::List(std::string name, VolumeOptions& options) {
  FLINT_ALIVE_OR_RET(-1);
  return g_volume_agent->ListVolume(name, options);
}

int Volume::ListAll(std::vector<VolumeOptions>& vols) {
  FLINT_ALIVE_OR_RET(-1);
  return g_volume_agent->ListAllVolumes(vols);
}

std::shared_ptr<SyncIOExecutor> Volume::CreateSyncIOExecutor(int flags) {
  FLINT_ALIVE_OR_RET(nullptr);
  std::shared_ptr<SyncIOExecutor> io_exec(new SyncIOExecutor(this, flags));
  int ret = io_exec->init();
  if (ret < 0) {
    LOG_ERROR("SyncIOExecutor init failed\n");
    return nullptr;
  }
  return io_exec;
}

std::shared_ptr<AsyncIOExecutor> Volume::CreateAsyncIOExecutor(int depth,
                                                               int flags) {
  FLINT_ALIVE_OR_RET(nullptr);
  std::shared_ptr<AsyncIOExecutor> async_io_exec(
      new AsyncIOExecutor(this, depth, flags));
  int ret = async_io_exec->init();
  if (ret < 0) {
    LOG_ERROR("AsyncIOExecutor init failed\n");
    return nullptr;
  }
  return async_io_exec;
}

}  // namespace flint
