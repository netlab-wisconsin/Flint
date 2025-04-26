#pragma once
#include "utils/misc.h"

#include <string>

#include "grpcpp/grpcpp.h"
#include "common/grpc/service.grpc.pb.h"
#include "common/grpc/message.pb.h"

namespace flint {
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::Status;
using grpc::StatusCode;

template <class ServiceImpl>
class RpcServer {
 private:
  std::string ip_;
  int port_;
  std::unique_ptr<grpc::Server> grpc_server_;
  std::unique_ptr<ServiceImpl> service_impl_;

  static_assert(std::is_base_of_v<grpc::Service, ServiceImpl>,
                "ServiceImpl must be derived from grpc::Service");

  void start_server() {
    std::string addr = ip_ + ":" + std::to_string(port_);
    grpc::EnableDefaultHealthCheckService(true);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(service_impl_.get());

    grpc_server_ = builder.BuildAndStart();
  }

 public:
  RpcServer(const std::string& ip, int port,
            std::unique_ptr<ServiceImpl> service_impl)
      : ip_(ip), port_(port), service_impl_(std::move(service_impl)) {}

  void Start() {
    start_server();
    grpc_server_->Wait();
  }

  void Shutdown() { grpc_server_->Shutdown(); }
};

template <class RpcService>
class RpcClient {
 private:
  std::string service_ip_;
  int service_port_;

  std::shared_ptr<grpc::Channel> grpc_channel_;

  using RpcStub = typename RpcService::Stub;
  std::mutex stub_mu_;

 public:
  RpcClient(const std::string& ip, int port)
      : service_ip_(ip), service_port_(port) {}

  int Connect() {
    std::string target_addr = service_ip_ + ":" + std::to_string(service_port_);
    grpc::ChannelArguments chan_args;
    chan_args.SetMaxReceiveMessageSize(MiB(32));

    grpc_channel_ = grpc::CreateCustomChannel(
        target_addr, grpc::InsecureChannelCredentials(), chan_args);
    if (grpc_channel_ == nullptr) {
      LOG_ERROR("Error creating grpc channel\n");
      return -1;
    }
    if (grpc_channel_->GetState(true) != GRPC_CHANNEL_READY) {
      if (!grpc_channel_->WaitForConnected(
              gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                           gpr_time_from_seconds(10, GPR_TIMESPAN)))) {
        LOG_ERROR(
            "grpc channel not ready after 10 seconds, current state: %d\n",
            grpc_channel_->GetState(true));
        return -1;
      }
    }
    return 0;
  }

  bool Connected() const {
    return grpc_channel_->GetState(true) == GRPC_CHANNEL_READY;
  }

  auto GetStub() {
    std::unique_lock lk(stub_mu_);
    return RpcService::NewStub(grpc_channel_);
  }
};

enum class VolumeOpErrCode {
  kOK = 0,
  kAlreadyExists,
  kNotExists,
  kInternal,
  kInvalidArg,
  kUnknownHostname,
};

enum class SchedServiceErrCode {
  kOK = 0,
  kInternal,
  kWait,
};

};  // namespace flint
