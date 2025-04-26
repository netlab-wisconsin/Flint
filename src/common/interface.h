#pragma once

#include <map>
#include <sstream>
#include <string>
#include "common/io_defs.h"

namespace flint {
enum class ExtAllocPolicy {
  kLazy = 0,  // lazy allocation, allocate extents on demand
  kFull,      // allocate full extents of the volume
  kPerf,      // currently not used
};

inline constexpr const char* ToString(ExtAllocPolicy policy) {
  constexpr const char* kPolicyNames[] = {"Lazy", "Full", "Perf"};
  return kPolicyNames[static_cast<int>(policy)];
}

struct VolumeOptions {
  /* persisted */
  std::string name;
  uint64_t size = 0;
  int rep_factor = 0;
  uint32_t flags = 0;
  uint8_t policy = 0;

  /* user input */
  std::map<std::string, double> ssd_preference;  // <ssd_nqn, score>

  VolumeOptions() = default;

  VolumeOptions(const std::string& name, uint64_t size, int rep_factor,
                int flags)
      : name(name), size(size), rep_factor(rep_factor), flags(flags) {}

  VolumeOptions(const std::string& name, uint64_t size, int rep_factor,
                int flags, const std::map<std::string, double>& ssd_preference)
      : VolumeOptions(name, size, rep_factor, flags) {
    this->ssd_preference = ssd_preference;
  }

  std::string ToString() {
    std::stringstream ss;
    ss << "name: " << name << std::endl
       << "size: " << size << std::endl
       << "rep factor: " << rep_factor << std::endl
       << "flags: " << flags << std::endl;
    return ss.str();
  }
};

enum class EventCallbackOp {
  kSsdAvailBwThreshold = 0,
  kNetPortBwThreshold,
  kIOPipeBwThreshold,
  kSsdFragDegreeThreshold,
  kSsdWriteCostThreshold,
  kBottleneck,
};

enum class CmpOp {
  kLessThan = 0,
  kGreaterThan,
};

struct EventCallback {
  int client_id;
  EventCallbackOp op;
  float threshold;
  IOType io_type;
  CmpOp cmp_op;

  union {
    int ebof_port;
    int ssd_id;
  };
};

enum class SloLevel : uint8_t {
  LOW = 1,
  MEDIUM = 2,
  HIGH = 4,
};

}  // namespace flint