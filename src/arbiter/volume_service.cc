#include "volume_service.h"
#include <cstddef>
#include "common/grpc/message.pb.h"
#include "common/interface.h"
#include "common/io_defs.h"
#include "common/logging.h"
#include "common/utils/json.hpp"
#include "common/utils/misc.h"
#include "ext_allocator.h"
#include "ssd_manager.h"
#include "volume_meta_manager.h"

namespace flint {
namespace arbiter {
Status RpcVolumeServiceImpl::GetFlintMetadata(
    ServerContext *context,
    const FLINT_RPC_MESSAGE::GetFlintMetadataRequest *request,
    FLINT_RPC_MESSAGE::GetFlintMetadataResponse *resp) {
  vol_service_->GetFlintMetadata(request, resp);
  return Status::OK;
}

Status RpcVolumeServiceImpl::CreateVolume(
    ServerContext *context,
    const FLINT_RPC_MESSAGE::CreateVolumeRequest *request,
    FLINT_RPC_MESSAGE::CreateVolumeResponse *resp) {
  vol_service_->CreateVolume(request, resp);
  return Status::OK;
}

Status RpcVolumeServiceImpl::OpenVolume(
    ServerContext *context, const FLINT_RPC_MESSAGE::OpenVolumeRequest *request,
    FLINT_RPC_MESSAGE::OpenVolumeResponse *resp) {
  vol_service_->OpenVolume(request, resp);
  return Status::OK;
}

Status RpcVolumeServiceImpl::DeleteVolume(
    ServerContext *context,
    const FLINT_RPC_MESSAGE::DeleteVolumeRequest *request,
    FLINT_RPC_MESSAGE::DeleteVolumeResponse *resp) {
  vol_service_->DeleteVolume(request, resp);
  return Status::OK;
}

Status RpcVolumeServiceImpl::ListVolumes(
    ServerContext *context, const FLINT_RPC_MESSAGE::ListVolumeRequest *request,
    FLINT_RPC_MESSAGE::ListVolumeResponse *resp) {
  vol_service_->ListVolumes(request, resp);
  return Status::OK;
}

Status RpcVolumeServiceImpl::VolumeExtentFault(
    ServerContext *context,
    const FLINT_RPC_MESSAGE::VolumeExtentFaultRequest *request,
    FLINT_RPC_MESSAGE::VolumeExtentFaultResponse *resp) {
  vol_service_->VolumeExtentFault(request, resp);
  return Status::OK;
}

VolumeService::VolumeService(const std::string &ip, int port,
                             SsdPoolManager &ssd_pool_manager)
    : ssd_pool_manager_(ssd_pool_manager) {
  vol_meta_manager_ = new VolumeMetaManager();

  ext_allocator_ = new ExtAllocator(ssd_pool_manager_);
  rpc_server_ = new RpcServer<RpcVolumeServiceImpl>(
      ip, port, std::make_unique<RpcVolumeServiceImpl>(this));
}

VolumeService::~VolumeService() {
  delete vol_meta_manager_;
  delete ext_allocator_;
  delete rpc_server_;
}

void VolumeService::Run() {
  loop_thread_ = std::thread([this] { rpc_server_->Start(); });
  pthread_setname_np(loop_thread_.native_handle(), "vol-service-rpc");
}

void VolumeService::Shutdown() {
  rpc_server_->Shutdown();
  loop_thread_.join();
}

template <class Resp>
void VolumeService::rpc_error(Resp *reply, VolumeOpErrCode err_code) {
  switch (err_code) {
    case VolumeOpErrCode::kOK:
      reply->set_err_message("ok");
      break;
    case VolumeOpErrCode::kAlreadyExists:
      reply->set_err_message("volume already exists");
      break;
    case VolumeOpErrCode::kNotExists:
      reply->set_err_message("volume does not exist");
      break;
    case VolumeOpErrCode::kInternal:
      reply->set_err_message("internal error");
      break;
    case VolumeOpErrCode::kInvalidArg:
      reply->set_err_message("invalid args");
      break;
    default:
      LOG_ERROR("unknown rpc error code: %d\n", err_code);
  }
  reply->set_err_code(static_cast<int>(err_code));
}

template <class Resp>
void VolumeService::rpc_error(Resp *resp, VolumeOpErrCode err_code,
                              const std::string &err_message) {
  resp->set_err_code(static_cast<int>(err_code));
  resp->set_err_message(err_message);
}

void VolumeService::GetFlintMetadata(
    const FLINT_RPC_MESSAGE::GetFlintMetadataRequest *req,
    FLINT_RPC_MESSAGE::GetFlintMetadataResponse *resp) {
  LOG_INFO("Received request to get flint metadata, hostname = %s.\n",
           req->hostname().c_str());
  auto metadata = resp->mutable_metadata();
  if (internal::g_ebof_port_map.find(req->hostname()) ==
      internal::g_ebof_port_map.end()) {
    LOG_WARN("Hostname %s not found in ebof port map.\n",
             req->hostname().c_str());
    rpc_error<FLINT_RPC_MESSAGE::GetFlintMetadataResponse>(
        resp, VolumeOpErrCode::kNotExists);
    return;
  }
  metadata->set_ebof_port(internal::g_ebof_port_map.at(req->hostname()));
  metadata->set_nic_ports_num(internal::g_nic_ports_num);
  for (const auto &p : internal::g_ssd_id_map) {
    (*metadata->mutable_ssd_id_map())[p.first] = p.second;
  }
  rpc_error<FLINT_RPC_MESSAGE::GetFlintMetadataResponse>(resp,
                                                         VolumeOpErrCode::kOK);
}

void VolumeService::CreateVolume(
    const FLINT_RPC_MESSAGE::CreateVolumeRequest *req,
    FLINT_RPC_MESSAGE::CreateVolumeResponse *reply) {
  VolumeOptions options;

  LOG_INFO(
      "Received request to create volume \"%s\", size = %.1f MiB, rep_factor = "
      "%d, flags = %d, policy = %s\n.",
      req->name().c_str(), req->size() * 1.0 / MiB(1), req->rep_factor(),
      req->flags(), ToString(static_cast<ExtAllocPolicy>(req->policy())));

  options.name = req->name();
  options.size = req->size();
  options.rep_factor = req->rep_factor();
  options.flags = req->flags();

  SsdPreferences ssd_preferences;
  for (const auto &p : req->ssd_preference()) {
    ssd_preferences[p.first] = p.second;
  }

  std::unique_lock<std::mutex> lk(vol_mu_);
  if (unlikely((size_t)options.rep_factor > ssd_pool_manager_.size())) {
    LOG_ERROR(
        "Volume %s specifies rep_factor = %d, larger than the number of "
        "available ssds: %d\n",
        options.name.c_str(), options.rep_factor, ssd_pool_manager_.size());
    rpc_error<FLINT_RPC_MESSAGE::CreateVolumeResponse>(
        reply, VolumeOpErrCode::kInvalidArg);
    return;
  }

  if (vol_meta_manager_->VolumeExists(options.name)) {
    LOG_WARN("Volume %s already exists.\n", options.name.c_str());
    rpc_error<FLINT_RPC_MESSAGE::CreateVolumeResponse>(
        reply, VolumeOpErrCode::kAlreadyExists);
    return;
  }

  auto ext_locator_map = ext_allocator_->PreAllocExt(
      options, ssd_preferences, static_cast<ExtAllocPolicy>(req->policy()));
  vol_meta_manager_->CreateVolume(options, ext_locator_map);
  LOG_INFO("Volume \"%s\" created successfully.\n", options.name.c_str());

  rpc_error<FLINT_RPC_MESSAGE::CreateVolumeResponse>(reply,
                                                     VolumeOpErrCode::kOK);
}

void VolumeService::OpenVolume(const FLINT_RPC_MESSAGE::OpenVolumeRequest *req,
                               FLINT_RPC_MESSAGE::OpenVolumeResponse *resp) {
  VolumeOptions options;
  LOG_INFO("Received request to open volume \"%s\".\n", req->name().c_str());

  std::unique_lock<std::mutex> lk(vol_mu_);
  if (!vol_meta_manager_->VolumeExists(req->name())) {
    LOG_WARN("volume %s does not exist.\n", req->name().c_str());
    rpc_error<FLINT_RPC_MESSAGE::OpenVolumeResponse>(
        resp, VolumeOpErrCode::kNotExists);
    return;
  }

  VolumeExtLocatorMap ext_locator_map;
  vol_meta_manager_->GetExtLocatorMap(req->name(), ext_locator_map);
  vol_meta_manager_->GetVolumeOptions(req->name(), options);
  lk.unlock();

  resp->set_size(options.size);
  resp->set_rep_factor(options.rep_factor);
  resp->set_flags(options.flags);
  auto resp_ext_locator_map = resp->mutable_extent_locator_map();
  for (const auto &[ext_id, locators] : ext_locator_map) {
    FLINT_RPC_MESSAGE::ExtentLocators ext_locators;
    for (const auto &locator : locators) {
      FLINT_RPC_MESSAGE::ExtentLocator l;
      l.set_rep_role(static_cast<int>(locator.rep_role));
      l.set_ssd_id(locator.ssd_id);
      l.set_pext_num(locator.pext_num);
      *ext_locators.add_locators() = l;
    }
    (*resp_ext_locator_map)[ext_id] = ext_locators;
  }
  LOG_INFO("Volume \"%s\" opened successfully.\n", req->name().c_str());
  rpc_error<FLINT_RPC_MESSAGE::OpenVolumeResponse>(resp, VolumeOpErrCode::kOK);
}

void VolumeService::DeleteVolume(
    const FLINT_RPC_MESSAGE::DeleteVolumeRequest *req,
    FLINT_RPC_MESSAGE::DeleteVolumeResponse *resp) {
  std::unique_lock<std::mutex> lk(vol_mu_);
  if (!vol_meta_manager_->VolumeExists(req->name())) {
    LOG_WARN("Volume \"%s\" does not exist.\n", req->name().c_str());
    rpc_error<FLINT_RPC_MESSAGE::DeleteVolumeResponse>(
        resp, VolumeOpErrCode::kNotExists);
    return;
  }

  if (!ext_allocator_->FreeExts(req->name())) {
    rpc_error<FLINT_RPC_MESSAGE::DeleteVolumeResponse>(
        resp, VolumeOpErrCode::kInternal);
    return;
  }
  vol_meta_manager_->DeleteVolume(req->name());

  lk.unlock();
  rpc_error<FLINT_RPC_MESSAGE::DeleteVolumeResponse>(resp,
                                                     VolumeOpErrCode::kOK);
}

void VolumeService::ListVolumes(const FLINT_RPC_MESSAGE::ListVolumeRequest *req,
                                FLINT_RPC_MESSAGE::ListVolumeResponse *resp) {
  std::vector<std::string> vol_names;
  bool list_all = req->list_all();

  std::unique_lock<std::mutex> lk(vol_mu_);
  if (list_all) {
    vol_meta_manager_->GetVolumeNames(vol_names);
  } else {
    vol_names.push_back(req->name());
  }
  for (auto name : vol_names) {
    VolumeOptions options;
    vol_meta_manager_->GetVolumeOptions(name, options);

    FLINT_RPC_MESSAGE::VolumeAttrs *vol_attr = resp->add_vol_attrs();
    vol_attr->set_name(options.name);
    vol_attr->set_size(options.size);
    vol_attr->set_rep_factor(options.rep_factor);
    vol_attr->set_flags(options.flags);
  }
  lk.unlock();

  rpc_error<FLINT_RPC_MESSAGE::ListVolumeResponse>(resp, VolumeOpErrCode::kOK);
}

void VolumeService::VolumeExtentFault(
    const FLINT_RPC_MESSAGE::VolumeExtentFaultRequest *req,
    FLINT_RPC_MESSAGE::VolumeExtentFaultResponse *resp) {
  std::vector<uint32_t> lexts;
  for (const auto &lext : req->lexts()) {
    lexts.push_back(lext);
  }

  SsdPreferences ssd_preferences;
  for (const auto &p : req->ssd_preference()) {
    ssd_preferences[p.first] = p.second;
  }

  std::unique_lock<std::mutex> lk(vol_mu_);
  if (!vol_meta_manager_->VolumeExists(req->name())) {
    LOG_WARN("Volume %s does not exist.\n", req->name().c_str());
    rpc_error<FLINT_RPC_MESSAGE::VolumeExtentFaultResponse>(
        resp, VolumeOpErrCode::kNotExists);
    return;
  }

  VolumeOptions options;
  vol_meta_manager_->GetVolumeOptions(req->name(), options);

  // usually takes 100+ us.
  auto ext_locator_map =
      ext_allocator_->AllocExt(options, ssd_preferences, lexts,
                               static_cast<ExtAllocPolicy>(req->policy()));
  auto resp_ext_locator_map = resp->mutable_extent_locator_map();
  for (const auto &[ext_id, locators] : ext_locator_map) {
    FLINT_RPC_MESSAGE::ExtentLocators ext_locators;
    for (const auto &locator : locators) {
      FLINT_RPC_MESSAGE::ExtentLocator l;
      l.set_rep_role(static_cast<int>(locator.rep_role));
      l.set_ssd_id(locator.ssd_id);
      l.set_pext_num(locator.pext_num);
      *ext_locators.add_locators() = l;
    }
    (*resp_ext_locator_map)[ext_id] = ext_locators;
  }
  vol_meta_manager_->UpdateVolumeExts(req->name(), ext_locator_map);
  rpc_error<FLINT_RPC_MESSAGE::VolumeExtentFaultResponse>(resp,
                                                          VolumeOpErrCode::kOK);
}

}  // namespace arbiter
}  // namespace flint
