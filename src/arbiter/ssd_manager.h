#pragma once

#include <cstdint>
#include <map>
#include "common/utils/json.hpp"

#include <condition_variable>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include "common/logging.h"
#include "common/ssd.h"
#include "common/utils/misc.h"
#include "db.h"
#include "internal.h"

namespace flint {
namespace arbiter {

/// @brief ExtentTable maps a volume's name to its extent mapping, from logical
/// extent number to physical extent number.
using ExtentTable = std::map<std::string, std::map<uint32_t, uint32_t>>;

/// @brief Segments represents the physical extent ranges taken by any volume.
/// TODO: Use an interval tree to speed up insertion/lookup instead of a linear vector.
class Segments {
 public:
  using Interval = std::pair<size_t, size_t>;
  using Intervals = std::vector<Interval>;

  Intervals intervals;

  void Init(size_t lo, size_t hi) {
    intervals.clear();
    intervals.emplace_back(lo, hi);
  }

  void AddExtent(size_t ext_num) {
    if (unlikely(intervals.empty())) {
      intervals.emplace_back(ext_num, ext_num);
      return;
    }
    for (auto& interval : intervals) {
      if (likely(ext_num == interval.second + 1)) {
        interval.second += 1;
        return;
      } else if (interval.first <= ext_num && interval.second >= ext_num) {
        LOG_WARN("Extent %lu already allocated", ext_num);
        return;
      }
    }
    intervals.emplace_back(ext_num, ext_num);
  }

  void AddSegments(const Segments& segs) {
    intervals.insert(intervals.end(), segs.intervals.begin(),
                     segs.intervals.end());
  }

  size_t GetExtentCount() const {
    size_t cnt = 0;
    for (const auto& interval : intervals) {
      cnt += interval.second - interval.first + 1;
    }
    return cnt;
  }

  auto begin() const { return intervals.begin(); }

  auto end() const { return intervals.end(); }
};

void from_json(const nlohmann::json& j, Segments& segs);

void to_json(nlohmann::json& j, const Segments& segs);

class SsdManager;
using SsdPoolManager = std::map<std::string, std::shared_ptr<SsdManager>>;

class SsdManager {
 private:
  std::shared_ptr<Ssd> ssd_;
  Segments free_segments_;
  using VolumeSegmentsMap = std::unordered_map<std::string, Segments>;
  VolumeSegmentsMap volume_segments_;  // volume uuid -> segments
  uint64_t capacity_total_ = 0;
  uint64_t capacity_used_ = 0;
  std::map<std::string, size_t> extent_usage_map_;
  std::shared_ptr<DBClient> db_client_;

  void load_metadata();

 public:
  SsdManager(std::shared_ptr<Ssd> ssd, bool load = true);

  int32_t AllocLExtent(const std::string& volume_name, uint64_t extent_num);

  void FreeVolume(const std::string& volume_name);

  void ResetMetadata();

  void PersistMetadata();

  std::map<std::string, size_t> GetExtentUsageMap() const {
    return extent_usage_map_;
  }

  size_t GetExtentUsage(const std::string& volume_name) const {
    if (extent_usage_map_.count(volume_name) == 1)
      return extent_usage_map_.at(volume_name);
    return 0;
  }

  const std::shared_ptr<Ssd>& GetSsd() const { return ssd_; }

  std::string GetNqn() const { return ssd_->nqn; }

  uint64_t GetUsedCapacity() const { return capacity_used_; }

  uint64_t GetTotalCapacity() const { return capacity_total_; }

  /// If ssd_id_map is provided, it will be used to assign ssd ids.
  /// Otherwise no id will be assigned to the ssd.
  /// If devlist is provided, only the ssds in the list will be added to the pool.
  static int Create(SsdPoolManager& ssd_pool_manager,
                    const std::optional<SsdIdMap>& ssd_id_map_opt,
                    const std::optional<std::set<std::string>>& devlist_opt,
                    bool load = true);
};

}  // namespace arbiter
}  // namespace flint