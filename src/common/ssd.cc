#include "ssd.h"
#include "logging.h"
#include "utils/misc.h"

#include <dirent.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <optional>

extern "C" {
#include <nvme/tree.h>
}

namespace flint {

static_assert(kExtentSize % kSSDBlockSize == 0,
              "Extent size must be a multiplier of physical block size");

SsdPtr SsdPool::GetSsd(const std::string& nqn) const {
  for (const auto& p : ssds) {
    if (p.second->nqn == nqn)
      return p.second;
  }
  return nullptr;
}

SsdPtr SsdPool::GetSsd(int id) const {
  if (ssds.count(id) == 1)
    return ssds.at(id);
  return nullptr;
}

SsdPtr SsdPool::GetSsdByPath(const std::string& sys_path) const {
  for (const auto& p : ssds) {
    if (p.second->sys_path == sys_path)
      return p.second;
  }
  return nullptr;
}

static const char* nvme_strerror(int errnum) {
  if (errnum >= ENVME_CONNECT_RESOLVE)
    return nvme_errno_to_string(errnum);
  return strerror(errnum);
}

// If ssd_id_map provided i.e. not empty, then ssds will be assigned with the id
// accordingly. Otherwise, we will assign id by ourselves.
int SsdPool::Create(SsdPool& ssd_pool, const std::optional<SsdIdMap>& ssd_id_map_opt,
                    const std::optional<std::set<std::string>>& devlist_opt) {
  nvme_host_t h;
  nvme_subsystem_t s;
  nvme_ctrl_t c;
  nvme_ns_t n;
  nvme_path_t p;
  nvme_root_t r;
  int err = 0;
  std::string subsysnqn;
  std::string devname;
  size_t lba;
  int ssd_id = 0;

  r = nvme_create_root(nullptr, 0);
  rt_assert(r, "Failed to create topology root: %s", nvme_strerror(errno));
  err = nvme_scan_topology(r, nullptr, nullptr);
  rt_assert(err >= 0, "Failed to scan topology: %s", nvme_strerror(errno));

  auto add_nvme_ns = [&](nvme_ns_t n, const std::string& nqn) {
    auto ssd = std::make_shared<Ssd>();
    devname.assign(nvme_ns_get_name(n));
    if (devlist_opt && devlist_opt->count(devname) == 0)
      return 0;
    lba = nvme_ns_get_lba_size(n);
    if (lba != kSSDBlockSize) {
      LOG_WARN("device %s has a block size %lu != %lu, skip..\n",
               devname.c_str(), lba, kSSDBlockSize);
      return 0;
    }
    ssd->sys_path = std::string(LINUX_DEVICE_PATH) + devname;
    ssd->nqn = nqn;
    if (!ssd_id_map_opt) {
      ssd_pool.ssds[ssd_id] = ssd;
      ssd->id = ssd_id;
      ssd_id++;
    } else {
      if (ssd_id_map_opt->count(nqn) == 1) {
        ssd_pool.ssds[ssd_id_map_opt->at(nqn)] = ssd;
        ssd->id = ssd_id_map_opt->at(nqn);
      }
    }
    return 0;
  };

  // The FS1600 node enforces each subsystem to have one unique namespace and a device,
  // so we can safely use the subsysnqn to uniquely identify an SSD device
  nvme_for_each_host(r, h) {
    nvme_for_each_subsystem(h, s) {
      subsysnqn.assign(nvme_subsystem_get_nqn(s));

      nvme_subsystem_for_each_ctrl(s, c) {
        if (strcmp(nvme_ctrl_get_transport(c), "tcp") != 0)
          continue;

        nvme_ctrl_for_each_ns(c, n) {
          if (add_nvme_ns(n, subsysnqn) != 0) {
            nvme_free_tree(r);
            return -1;
          }
        }

        nvme_ctrl_for_each_path(c, p) {
          n = nvme_path_get_ns(p);
          if (n) {
            if (add_nvme_ns(n, subsysnqn) != 0) {
              nvme_free_tree(r);
              return -1;
            }
          }
        }
      }
    }
  }

  nvme_free_tree(r);

  return 0;
}

int SsdPool::Open(int flags) {
  int res = 0;
  for (auto& p : ssds) {
    res = p.second->Open(flags);
    if (res < 0) {
      return res;
    }
  }
  return 0;
}

};  // namespace flint