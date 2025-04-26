#pragma once
#include "common/interface.h"
#include "common/io_defs.h"
#include "common/utils/rand.h"
#include "ssd_manager.h"
#include "view_controller.h"

#include <cstddef>
#include <cstdint>

namespace flint {
namespace arbiter {

using SsdPreferences = std::map<std::string, float>;

class ExtAllocator {
 private:
  SsdPoolManager& ssd_pool_manager_;
  static constexpr double kAllocAlpha = 1.0;
  static constexpr double kAllocBeta = 1.0;
  static constexpr double kAllocGamma = 1.5;
  static constexpr double kAllocDelta = 1.5;
  static constexpr double kAllocEta = 0.5;

  std::mutex alloc_mu_;
  FastRand fast_rand_;
  std::shared_ptr<ViewController> view_controller_;
  float compute_score(const VolumeOptions& options,
                      const ShadowView& shadow_view,
                      const SsdManager& ssd_manager, float preference,
                      size_t extent_usage, ExtAllocPolicy policy);

 public:
  ExtAllocator(SsdPoolManager& ssd_pool_manager)
      : ssd_pool_manager_(ssd_pool_manager) {}

  void SetViewController(std::shared_ptr<ViewController> view_controller) {
    view_controller_ = view_controller;
  }

  VolumeExtLocatorMap PreAllocExt(
      const VolumeOptions& options, SsdPreferences& ssd_preferences,
      ExtAllocPolicy policy = ExtAllocPolicy::kLazy);

  VolumeExtLocatorMap AllocExt(const VolumeOptions& options,
                               SsdPreferences& ssd_preferences,
                               const std::vector<uint32_t>& lexts,
                               ExtAllocPolicy policy = ExtAllocPolicy::kLazy);

  /// Free the extents allocated for the volume.
  bool FreeExts(const std::string& vol_name);
};

}  // namespace arbiter
}  // namespace flint
