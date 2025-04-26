#include "volume_agent.h"
#include <fcntl.h>
#include <mutex>
#include "common/logging.h"
#include "common/rpc.h"
#include "common/ssd.h"
#include "common/utils/misc.h"
#include "flint.h"
#include "internal.h"
#include "volume.h"

#include "common/utils/json.hpp"

namespace flint {

using env::g_config;
using nlohmann::json;

inline static std::string volume_service_error_message(int err_code) {
  switch (err_code) {
    case static_cast<int>(VolumeOpErrCode::kOK):
      return "ok";
    case static_cast<int>(VolumeOpErrCode::kAlreadyExists):
      return "volume already exists";
    case static_cast<int>(VolumeOpErrCode::kNotExists):
      return "volume does not exist";
    case static_cast<int>(VolumeOpErrCode::kInternal):
      return "internal error";
    case static_cast<int>(VolumeOpErrCode::kInvalidArg):
      return "invalid args";
    case static_cast<int>(VolumeOpErrCode::kUnknownHostname):
      return "unknown hostname";
    default:
      return "unknown rpc error";
  }
}

VolumeAgent::VolumeAgent() {}

int VolumeAgent::Init() {
  int ret = 0;
  rpc_client_ = std::make_shared<RpcClient<FLINT_RPC_SERVICE::VolumeService>>(
      g_config.arbiter_ip, g_config.volume_service_port);
  ret = rpc_client_->Connect();
  if (ret != 0) {
    LOG_ERROR("Failed to connect to volume service\n");
    return ret;
  }

  LOG_INFO("Retrieving flint metadata...\n");
  ret = get_flint_metadata();
  if (ret != 0) {
    LOG_ERROR("Failed to retrieve flint metadata\n");
    return ret;
  }
  LOG_INFO("Flint metadata retrieved successfully.\n");
  LOG_INFO("My hostname: %s, ebof port: %d\n", GetHostname().c_str(),
           internal::g_ebof_port);
  LOG_INFO("Ssd id map:\n");
  for (const auto& p : internal::g_ssd_id_map) {
    LOG_INFO("Ssd: %s, id: %d\n", p.first.c_str(), p.second);
  }
  return 0;
}

int VolumeAgent::get_flint_metadata() {
  FLINT_RPC_MESSAGE::GetFlintMetadataRequest request;
  FLINT_RPC_MESSAGE::GetFlintMetadataResponse resp;

  request.set_hostname(GetHostname());

  const auto& stub = rpc_client_->GetStub();
  ClientContext ctx;
  Status status = stub->GetFlintMetadata(&ctx, request, &resp);
  if (!status.ok()) {
    LOG_ERROR("GetFlintMetadata grpc server error: %s.\n",
              status.error_message().c_str());
    return -1;
  }
  if (resp.err_code() != static_cast<int>(VolumeOpErrCode::kOK)) {
    LOG_ERROR("GetFlintMetadata rpc error: %s.\n",
              volume_service_error_message(resp.err_code()).c_str());
    return -1;
  }

  internal::g_ebof_port = resp.metadata().ebof_port();
  internal::g_nic_ports_num = resp.metadata().nic_ports_num();
  for (const auto& [nqn, id] : resp.metadata().ssd_id_map()) {
    internal::g_ssd_id_map[nqn] = id;
  }
  int res = SsdPool::Create(ssd_pool_, internal::g_ssd_id_map, std::nullopt);
  if (res != 0) {
    LOG_ERROR("Failed to create ssd pool");
    return -1;
  }
  return 0;
}

int VolumeAgent::CreateVolume(VolumeOptions& options) {
  if (options.name.empty()) {
    LOG_ERROR("Volume name must be specified\n");
    return -1;
  }

  FLINT_RPC_MESSAGE::CreateVolumeRequest request;
  FLINT_RPC_MESSAGE::CreateVolumeResponse resp;

  request.set_name(options.name);
  request.set_size(options.size);
  request.set_flags(options.flags);
  request.set_rep_factor(options.rep_factor);
  request.set_policy(static_cast<uint32_t>(options.policy));

  auto req_ssd_pref = request.mutable_ssd_preference();
  for (const auto& p : options.ssd_preference) {
    (*req_ssd_pref)[p.first] = p.second;
  }

  std::lock_guard<std::mutex> lk(mu_);
  const auto& stub = rpc_client_->GetStub();
  ClientContext ctx;
  Status status = stub->CreateVolume(&ctx, request, &resp);
  rt_assert(status.ok(), "grpc error: %s", status.error_message().c_str());

  if (resp.err_code() != static_cast<int>(VolumeOpErrCode::kOK)) {
    LOG_ERROR("CreateVolume server error: %s\n",
              volume_service_error_message(resp.err_code()).c_str());
    return -1;
  }
  return 0;
}

std::shared_ptr<Volume> VolumeAgent::OpenVolume(VolumeOptions& options) {
  if (options.name.empty()) {
    LOG_ERROR("Volume name is not specified\n");
    return nullptr;
  }

  FLINT_RPC_MESSAGE::OpenVolumeRequest request;
  FLINT_RPC_MESSAGE::OpenVolumeResponse resp;

  request.set_name(options.name);

  std::lock_guard<std::mutex> lk(mu_);
  const auto& stub = rpc_client_->GetStub();
  ClientContext ctx;
  Status status = stub->OpenVolume(&ctx, request, &resp);
  if (!status.ok()) {
    LOG_ERROR("OpenVolume grpc server error: %s\n",
              status.error_message().c_str());
    return nullptr;
  }

  if (resp.err_code() != static_cast<int>(VolumeOpErrCode::kOK)) {
    LOG_ERROR("OpenVolume server error: %s\n",
              volume_service_error_message(resp.err_code()).c_str());
    return nullptr;
  }

  options.size = resp.size();
  options.rep_factor = resp.rep_factor();
  options.flags = resp.flags();

  std::shared_ptr<Volume> vol(new Volume(options));
  for (const auto& [ext_id, locators] : resp.extent_locator_map()) {
    for (const auto& locator : locators.locators()) {
      vol->ext_locator_map_[ext_id].emplace_back(
          static_cast<ChainRepRole>(locator.rep_role()), locator.ssd_id(),
          locator.pext_num());
    }
  }
  return vol;
}

int VolumeAgent::HandleVolumeExtentFault(Volume& vol,
                                         const std::vector<size_t>& lexts) {
  FLINT_RPC_MESSAGE::VolumeExtentFaultRequest request;
  FLINT_RPC_MESSAGE::VolumeExtentFaultResponse resp;
  request.set_name(vol.GetName());
  for (const auto& lext : lexts) {
    request.add_lexts(lext);
  }
  const auto& stub = rpc_client_->GetStub();
  ClientContext ctx;
  Status status = stub->VolumeExtentFault(&ctx, request, &resp);
  rt_assert(status.ok(), "grpc error: %s", status.error_message().c_str());

  if (resp.err_code() != static_cast<int>(VolumeOpErrCode::kOK)) {
    LOG_ERROR("VolumeExtentFault server error: %s\n",
              volume_service_error_message(resp.err_code()).c_str());
    return -1;
  }

  // merge the new extent locators with the old ones
  std::unique_lock<std::shared_mutex> lk(vol.ext_mu_);
  for (const auto& [ext_id, locators] : resp.extent_locator_map()) {
    for (const auto& locator : locators.locators()) {
      vol.ext_locator_map_[ext_id].emplace_back(
          static_cast<ChainRepRole>(locator.rep_role()), locator.ssd_id(),
          locator.pext_num());
    }
  }
  return 0;
}

int VolumeAgent::DeleteVolume(const std::string& vol_name) {
  FLINT_RPC_MESSAGE::DeleteVolumeRequest request;
  FLINT_RPC_MESSAGE::DeleteVolumeResponse resp;
  request.set_name(vol_name);

  std::lock_guard<std::mutex> lk(mu_);

  const auto& stub = rpc_client_->GetStub();
  ClientContext ctx;
  Status status = stub->DeleteVolume(&ctx, request, &resp);
  rt_assert(status.ok(), "grpc error: %s", status.error_message().c_str());

  if (resp.err_code() != static_cast<int>(VolumeOpErrCode::kOK)) {
    LOG_ERROR("DeleteVolume server error: %s\n",
              volume_service_error_message(resp.err_code()).c_str());
    return -1;
  }
  return 0;
}

int VolumeAgent::ListVolume(const std::string& vol_name,
                            VolumeOptions& options) {
  FLINT_RPC_MESSAGE::ListVolumeRequest request;
  FLINT_RPC_MESSAGE::ListVolumeResponse resp;
  request.set_name(vol_name);
  request.set_list_all(false);

  const auto& stub = rpc_client_->GetStub();
  ClientContext ctx;
  Status status = stub->ListVolumes(&ctx, request, &resp);
  rt_assert(status.ok(), "grpc error: %s", status.error_message().c_str());

  if (resp.err_code() != static_cast<int>(VolumeOpErrCode::kOK)) {
    LOG_ERROR("ListVolume server error: %s\n",
              volume_service_error_message(resp.err_code()).c_str());
    return -1;
  }
  rt_assert(resp.vol_attrs_size() == 1);

  const auto& vol_attr = resp.vol_attrs(0);
  options.name = vol_attr.name();
  options.size = vol_attr.size();
  options.flags = vol_attr.flags();
  options.rep_factor = vol_attr.rep_factor();

  return 0;
}

int VolumeAgent::ListAllVolumes(std::vector<VolumeOptions>& vols) {
  FLINT_RPC_MESSAGE::ListVolumeRequest request;
  FLINT_RPC_MESSAGE::ListVolumeResponse resp;
  request.set_list_all(true);

  const auto& stub = rpc_client_->GetStub();
  ClientContext ctx;
  Status status = stub->ListVolumes(&ctx, request, &resp);
  rt_assert(status.ok(), "grpc error: %s", status.error_message().c_str());

  if (resp.err_code() != static_cast<int>(VolumeOpErrCode::kOK)) {
    LOG_ERROR("ListAllVolumes server error: %s\n",
              volume_service_error_message(resp.err_code()).c_str());
    return -1;
  }

  for (const auto& vol_attr : resp.vol_attrs()) {
    VolumeOptions options;
    options.name = vol_attr.name();
    options.size = vol_attr.size();
    options.flags = vol_attr.flags();
    options.rep_factor = vol_attr.rep_factor();

    vols.push_back(options);
  }
  return 0;
}

};  // namespace flint