#include "ext_allocator.h"
#include "common/ssd.h"
#include "common/utils/misc.h"
#include "ssd_manager.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>

namespace flint {
namespace arbiter {
float ExtAllocator::compute_score(const VolumeOptions& options,
                                  const ShadowView& shadow_view,
                                  const SsdManager& ssd_manager,
                                  float preference, size_t extent_usage,
                                  ExtAllocPolicy policy) {
  float f_extent;
  float f_capacity;
  float f_busy_degree;
  float f_fragment_degree;
  float f_preference;

  auto vol_extent_count = ROUND_UP(options.size, kExtentSize) / kExtentSize;
  auto& ssd_view = shadow_view.ssd_views.at(ssd_manager.GetSsd()->id);

  f_extent = 1.0 * extent_usage / vol_extent_count;
  f_capacity =
      1.0 * ssd_manager.GetUsedCapacity() / ssd_manager.GetTotalCapacity();
  // TODO: should use a dedicated metric?
  f_busy_degree = ssd_view.write_cost;
  f_fragment_degree = ssd_view.frag_degree;
  f_preference = preference;

  float score = kAllocAlpha * f_extent + kAllocBeta * f_capacity +
                kAllocGamma * f_busy_degree + kAllocDelta * f_fragment_degree +
                kAllocEta * f_preference;
  score = 1.0 / score;
  return score;
}

using SsdScore = std::pair<std::shared_ptr<SsdManager>, float>;

static bool compare_ssd_scores(const SsdScore& lhs, const SsdScore& rhs) {
  return lhs.second > rhs.second;
}

static std::vector<uint32_t> StripeToExts(uint64_t vol_size) {
  std::vector<uint32_t> lexts;
  uint64_t s = 0;
  for (uint32_t i = 0; s < ROUND_UP(vol_size, kExtentSize);
       i++, s += kExtentSize) {
    lexts.push_back(i);
  }
  return lexts;
}

VolumeExtLocatorMap ExtAllocator::PreAllocExt(const VolumeOptions& options,
                                              SsdPreferences& ssd_preferences,
                                              ExtAllocPolicy policy) {
  auto lexts_striped = StripeToExts(options.size);
  std::vector<uint32_t> lexts_to_alloc;
  uint64_t size_to_provision = options.size;

  if (policy == ExtAllocPolicy::kLazy)
    size_to_provision = 0;

  size_t s = 0, i = 0;
  while (s < size_to_provision) {
    auto lext = lexts_striped[i++];
    lexts_to_alloc.push_back(lext);
    s += kExtentSize;
  }

  return AllocExt(options, ssd_preferences, lexts_to_alloc, policy);
}

VolumeExtLocatorMap ExtAllocator::AllocExt(const VolumeOptions& options,
                                           SsdPreferences& ssd_preferences,
                                           const std::vector<uint32_t>& lexts,
                                           ExtAllocPolicy policy) {

  std::vector<SsdScore> ssd_scores;
  VolumeExtLocatorMap ext_locators;
  std::set<SsdManager*> ssds_used;
  auto shadow_view = view_controller_->GetShadowView();
  auto resort_ssd_scores_quick = [&](const SsdManager& ssd_manager) {
    // pick the first ssd_score pair from the sorted vector as it has the
    // best score, kicks it out, do the resort, and insert the ssd back
    // with its new score using binary search
    auto prev = ssd_scores.front();
    ssd_scores.erase(ssd_scores.begin());
    float preference_score = 0.0;
    if (ssd_preferences.count(ssd_manager.GetNqn()))
      preference_score = ssd_preferences.at(ssd_manager.GetNqn());
    prev.second =
        compute_score(options, shadow_view, ssd_manager, preference_score,
                      ssd_manager.GetExtentUsage(options.name), policy);
    auto it = std::lower_bound(ssd_scores.begin(), ssd_scores.end(), prev,
                               compare_ssd_scores);
    ssd_scores.insert(it, prev);
  };

  // pre-sort
  for (auto& p : ssd_pool_manager_) {
    auto ssd_manager = p.second;
    float preference_score = 0.0;
    if (ssd_preferences.count(ssd_manager->GetNqn()))
      preference_score = ssd_preferences.at(ssd_manager->GetNqn());
    float score =
        compute_score(options, shadow_view, *ssd_manager, preference_score,
                      ssd_manager->GetExtentUsage(options.name), policy);
    ssd_scores.emplace_back(ssd_manager, score);
  }
  std::sort(ssd_scores.begin(), ssd_scores.end(), compare_ssd_scores);

  for (size_t i : lexts) {
    ExtentLocators locators;
    int32_t pext_num;
    std::shared_ptr<SsdManager> sm;
    float score;
    int rep_factor = options.rep_factor;

    if (rep_factor == 1) {
      std::tie(sm, score) = ssd_scores.front();

      pext_num = sm->AllocLExtent(options.name, i);
      rt_assert(pext_num >= 0);
      locators.emplace_back(ChainRepRole::kStandalone, sm->GetSsd()->id,
                            pext_num);
      ssds_used.insert(sm.get());
      resort_ssd_scores_quick(*sm);
    } else {
      rt_assert((size_t)rep_factor <= ssd_pool_manager_.size());

      std::tie(sm, score) = ssd_scores.front();
      pext_num = sm->AllocLExtent(options.name, i);
      rt_assert(pext_num >= 0);
      locators.emplace_back(ChainRepRole::kHead, sm->GetSsd()->id, pext_num);
      resort_ssd_scores_quick(*sm);
      ssds_used.insert(sm.get());

      for (int j = 0; j < rep_factor - 2; j++) {
        std::tie(sm, score) = ssd_scores.front();
        pext_num = sm->AllocLExtent(options.name, i);
        rt_assert(pext_num >= 0);
        locators.emplace_back(ChainRepRole::kMiddle, sm->GetSsd()->id,
                              pext_num);
        ssds_used.insert(sm.get());
        resort_ssd_scores_quick(*sm);
      }

      std::tie(sm, score) = ssd_scores.front();
      pext_num = sm->AllocLExtent(options.name, i);
      rt_assert(pext_num >= 0);
      locators.emplace_back(ChainRepRole::kTail, sm->GetSsd()->id, pext_num);
      ssds_used.insert(sm.get());
      resort_ssd_scores_quick(*sm);
    }
    ext_locators[i] = locators;
  }

  for (auto sm : ssds_used) {
    sm->PersistMetadata();
  }
  return ext_locators;
}

bool ExtAllocator::FreeExts(const std::string& vol_name) {
  for (auto& p : ssd_pool_manager_) {
    auto sm = p.second;
    sm->FreeVolume(vol_name);
    sm->PersistMetadata();
  }
  return true;
}

};  // namespace arbiter
};  // namespace flint