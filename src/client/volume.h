#pragma once

#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <string>
#include "common/interface.h"
#include "common/io_defs.h"

namespace flint {

inline constexpr int kVolumeNameMaxLen = 31;
inline constexpr int kVolumeUUIDLen = 16;

/// Forward declarations
class SyncIOExecutor;
class AsyncIOExecutor;

class Volume {
 private:
  uint64_t size_;
  std::string name_;
  uint32_t flags_;  // POSIX file flags: O_RDWR, O_DIRECT...
  int rep_factor_;
  std::shared_mutex ext_mu_;
  bool opened_ = false;

  VolumeExtLocatorMap ext_locator_map_;

  Volume(const VolumeOptions& option)
      : size_(option.size),
        name_(option.name),
        flags_(option.flags),
        rep_factor_(option.rep_factor) {}

  /**
   * @brief Lookup the extent locators of a logical extent number.
   * @param lext_num the logical extent number.
   * @return the extent locators of the logical extent number, if it
   * valid and the physical extents have been allocated.
   */
  std::optional<ExtentLocators> lookup_logical_extent(size_t lext_num) {
    std::shared_lock<std::shared_mutex> lock(ext_mu_);
    if (ext_locator_map_.count(lext_num) == 0)
      return std::nullopt;
    return ext_locator_map_.at(lext_num);
  }

  const auto& get_extent_locator_map() const { return ext_locator_map_; }

 public:
  static int Create(VolumeOptions& options);

  static std::shared_ptr<Volume> Open(VolumeOptions& options);

  static int Delete(std::string name);

  static int List(std::string name, VolumeOptions& options);

  static int ListAll(std::vector<VolumeOptions>& vols);

  friend class IOExecutor;
  friend class SyncIOExecutor;
  friend class AsyncIOExecutor;
  friend class VolumeAgent;

  int Delete();

  std::string GetName() const { return name_; }

  uint32_t GetFlags() const { return flags_; }

  uint64_t GetSize() const { return size_; }

  int GetRepFactor() const { return rep_factor_; }

  int32_t ExtentCount() const;

  std::shared_ptr<SyncIOExecutor> CreateSyncIOExecutor(int flags);

  std::shared_ptr<AsyncIOExecutor> CreateAsyncIOExecutor(int depth, int flags);
};

};  // namespace flint