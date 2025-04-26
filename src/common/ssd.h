#pragma once

#include <fcntl.h>
#include <unistd.h>  // for close()
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include "common/utils/json.hpp"
#include "logging.h"

namespace flint {

#define O_DIRECT __O_DIRECT

#define LINUX_DEVICE_PATH "/dev/"
#define LINUX_GROUP_DISK "disk"
#define LINUX_NVME_CLASS_PATH "/sys/class/nvme"

inline constexpr uint32_t kExtentSize = 1 << 21;  // 2MB
constexpr size_t kSSDBlockSize = 1 << 12;         // 4KB

inline uint64_t ExtentToAddr(uint64_t ext_num, uint64_t off) {
  return ext_num * kExtentSize + off;
}

inline uint32_t AddrToExtentNum(uint64_t addr) {
  return addr / kExtentSize;
}

inline uint32_t AddrToExtentOffset(uint64_t addr) {
  return addr % kExtentSize;
}

struct Ssd;
using SsdPtr = std::shared_ptr<Ssd>;

struct Ssd {
  int fd = -1;
  std::string nqn;
  std::string sys_path;
  int id = -1;

  int Open(int flags) {
    int res = 0;
    res = open(sys_path.c_str(), flags);
    if (res < 0) {
      LOG_ERROR("Failed to open SSD: %s", sys_path.c_str());
      return -1;
    }
    fd = res;
    return 0;
  }

  void Close() {
    close(fd);
    fd = -1;
  }

  ~Ssd() {
    if (fd != -1) {
      Close();
    }
  }
};

using SsdIdMap = std::map<std::string, int>;

inline void from_json(const nlohmann::json& j, SsdIdMap& map) {
  for (const auto& [key, value] : j.items()) {
    map[key] = value.get<int>();
  }
}

inline void to_json(nlohmann::json& j, const SsdIdMap& map) {
  for (const auto& [key, value] : map) {
    j[key] = value;
  }
}

struct SsdPool {
  std::map<int, SsdPtr> ssds;

 public:
  static int Create(SsdPool& ssd_pool,
                    const std::optional<SsdIdMap>& ssd_id_map,
                    const std::optional<std::set<std::string>>& devlist);

  // open all ssds in the pool with the given flags
  int Open(int flags);

  // better to get by id
  SsdPtr GetSsd(const std::string& nqn) const;

  SsdPtr GetSsd(int id) const;

  SsdPtr GetSsdByPath(const std::string& sys_path) const;

  int NumSsds() const { return ssds.size(); }
};

}  // namespace flint