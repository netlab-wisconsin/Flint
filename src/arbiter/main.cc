#include <sys/resource.h>
#include <condition_variable>
#include <csignal>
#include "common/logging.h"
#include "common/rpc.h"
#include "common/ssd.h"
#include "ext_allocator.h"
#include "internal.h"
#include "scheduler_service.h"
#include "ssd_manager.h"
#include "volume_service.h"

#include <yaml-cpp/yaml.h>

#define ARBITER_PID_FILE "/var/run/flint/arbiter.pid"
#define ARBITER_SOCK_FILE "/var/run/flint/arbiter.sock"

using namespace flint;

static std::string arbiter_ip = "0.0.0.0";
static int volume_service_port = 31850;
static int scheduler_service_port = 31851;
static std::set<std::string> ssdlist;

static std::mutex mu;
static std::condition_variable shutdown_cv;

using namespace arbiter;
using namespace arbiter::internal;

void shutdown_sig_handler(int signal) {
  LOG_INFO("Shutting down arbiter...\n");
  g_arbiter_shutdown_requested = true;
  shutdown_cv.notify_one();
}

int enable_coredump() {
  struct rlimit new_lim;
  new_lim.rlim_max = RLIM_INFINITY;
  new_lim.rlim_cur = RLIM_INFINITY;
  if (setrlimit(RLIMIT_CORE, &new_lim) == -1) {
    LOG_FATAL("error setting rlimit for core dump: %s\n", strerror(errno));
    return -1;
  }
  return 0;
}

int parse_config_params(const std::string &conf_filename) {
  YAML::Node config;
  try {
    config = YAML::LoadFile(conf_filename);
  } catch (const YAML::Exception &e) {
    LOG_ERROR("Failed to load config file: %s\n", e.what());
    return -1;
  }

  if (static_cast<bool>(config["disks"])) {
    const auto &disks = config["disks"];
    if (!disks.IsSequence()) {
      LOG_ERROR("Invalid disks config: %s\n", disks.as<std::string>().c_str());
      return -1;
    }
    for (const auto &disk : disks)
      ssdlist.insert(disk.as<std::string>());
  }

  if (static_cast<bool>(config["arbiter_ip"]))
    arbiter_ip = config["arbiter_ip"].as<std::string>();

  if (static_cast<bool>(config["volume_service_port"])) {
    volume_service_port = config["volume_service_port"].as<int>();
  }

  if (static_cast<bool>(config["scheduler_service_port"])) {
    scheduler_service_port = config["scheduler_service_port"].as<int>();
  }

  if (static_cast<bool>(config["ebof_port"])) {
    const auto &ebof_port = config["ebof_port"];
    if (!ebof_port.IsSequence()) {
      LOG_ERROR("Invalid ebof port config: %s\n",
                ebof_port.as<std::string>().c_str());
      return -1;
    }
    for (const auto &port : ebof_port) {
      if (!port.IsMap()) {
        LOG_ERROR("Invalid ebof port config: %s\n",
                  port.as<std::string>().c_str());
        return -1;
      }
      for (const auto &kv : port) {
        g_ebof_port_map[kv.first.as<std::string>()] = kv.second.as<int>();
      }
    }
  } else {
    LOG_ERROR("No ebof port map specified.\n");
    return -1;
  }

  if (static_cast<bool>(config["nic_ports_num"])) {
    g_nic_ports_num = config["nic_ports_num"].as<int>();
  } else {
    LOG_ERROR("No nic ports num specified.\n");
    return -1;
  }
  return 0;
}

static int env_init() {
  g_db_client = std::make_shared<DBClient>();
  // Start RPC service
#if defined(CONFIG_RPC_ERPC)
  g_arbiter_config.nexus =
      CreateNexus(g_arbiter_config.rpc_ip, g_arbiter_config.rpc_port,
                  g_arbiter_config.erpc_numa, g_arbiter_config.erpc_bg_threads);
  register_rpc_req_handler(g_arbiter_config.nexus);
#endif

  SsdPool ssd_pool;
  int ret = LoadArbiterMetadata();
  if (ret != 0) {
    // fresh run, should assign ssd ids and persist them
    LOG_INFO("Generating initial ssd pool and id map...\n");
    if (!ssdlist.empty()) {
      ret = SsdPool::Create(ssd_pool, std::nullopt, ssdlist);
    } else {
      ret = SsdPool::Create(ssd_pool, std::nullopt, std::nullopt);
    }
    if (ret != 0) {
      LOG_FATAL("Failed to create ssd pool.\n");
      return ret;
    }
    for (auto &p : ssd_pool.ssds) {
      const auto &ssd = p.second;
      g_ssd_id_map[ssd->nqn] = ssd->id;
    }
    LOG_INFO("Ssd pool and id map generated, persisting...\n");
    PersistArbiterMetadata();

    // retry loading metadata
    ret = LoadArbiterMetadata();
    if (ret != 0) {
      LOG_FATAL("Failed to load arbiter metadata.\n");
      return ret;
    }
  }
  LOG_INFO("Arbiter metadata loaded. Ssd pool and id map:\n");
  for (auto &p : g_ssd_id_map) {
    LOG_INFO("Ssd: %s, id: %d\n", p.first.c_str(), p.second);
  }

  return 0;
}

static void env_destroy() {
  PersistArbiterMetadata();
  g_db_client->Close();
}

int main(int argc, char *argv[]) {
  int ret = 0;

#ifdef CONFIG_DEBUG
  ret = enable_coredump();
  if (ret != 0)
    return ret;
#endif

  if (argc == 1)
    LOG_INFO("No config file specified, using default arbiter config.");

  if (argc == 2) {
    ret = parse_config_params(std::string{argv[1]});
    if (ret != 0)
      return ret;
  }

  LOG_INFO("Flint arbiter initializing...\n");

  ret = env_init();
  if (ret != 0) {
    LOG_FATAL("Failed to initialize arbiter environment.\n");
    return ret;
  }

  SsdPoolManager ssd_pool_manager;
  if (ssdlist.empty())
    ret = SsdManager::Create(ssd_pool_manager, g_ssd_id_map, std::nullopt);
  else
    ret = SsdManager::Create(ssd_pool_manager, g_ssd_id_map, ssdlist);
  if (ssd_pool_manager.size() == 0) {
    LOG_ERROR("No available ssd device");
    env_destroy();
    return -1;
  }

  VolumeService volume_rpc_service(arbiter_ip, volume_service_port,
                                   ssd_pool_manager);
  ret = volume_rpc_service.Init();
  if (ret != 0) {
    LOG_FATAL("Failed to initialize volume service.\n");
    env_destroy();
    return ret;
  }
  volume_rpc_service.Run();
  LOG_INFO("Arbiter volume service started.\n");

  SchedulerService scheduler_rpc_service(arbiter_ip, scheduler_service_port,
                                         ssd_pool_manager);
  ret = scheduler_rpc_service.Init();
  if (ret != 0) {
    LOG_FATAL("Failed to initialize scheduler service.\n");
    env_destroy();
    return ret;
  }
  scheduler_rpc_service.Run();
  LOG_INFO("Arbiter scheduler service started.\n");

  auto view_controller = scheduler_rpc_service.GetViewController();
  auto ext_allocator = volume_rpc_service.GetExtAllocator();
  ext_allocator->SetViewController(view_controller);

  LOG_INFO("Arbiter initialized successfully!\n");

  std::signal(SIGINT, shutdown_sig_handler);
  std::signal(SIGTERM, shutdown_sig_handler);

  std::unique_lock lk{mu};
  shutdown_cv.wait(lk);

  volume_rpc_service.Shutdown();
  LOG_INFO("Volume service shutdown.\n");
  scheduler_rpc_service.Shutdown();
  LOG_INFO("Scheduler service shutdown.\n");

  env_destroy();
}