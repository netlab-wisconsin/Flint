#pragma once

#include <cstdint>
#include <memory>
#include "common/utils/json.hpp"
#include "ssd.h"

namespace flint {
enum class ChainRepRole : uint8_t {
  kHead,
  kMiddle,
  kTail,
  // no replication, i.e. rf=1
  kStandalone,
};

struct ExtentLocator {
  ChainRepRole rep_role;
  int ssd_id;
  uint32_t pext_num;

  ExtentLocator(ChainRepRole role, int ssd_id, uint32_t pext_num)
      : rep_role(role), ssd_id(ssd_id), pext_num(pext_num) {}

  ExtentLocator() {}
};

using ExtentLocators = std::vector<ExtentLocator>;

/// @brief A map from extent number to extent locators.
using VolumeExtLocatorMap = std::map<size_t, ExtentLocators>;

void from_json(const nlohmann::json& j, ExtentLocator& extent_locator);
void to_json(nlohmann::json& j, const ExtentLocator& extent_locator);

void from_json(const nlohmann::json& j, ExtentLocators& extent_locators);
void to_json(nlohmann::json& j, const ExtentLocators& extent_locators);

void from_json(const nlohmann::json& j,
               VolumeExtLocatorMap& extent_locators_map);
void to_json(nlohmann::json& j, const VolumeExtLocatorMap& extent_locators_map);

/**
 * @brief An enum class used by all eVol components
 * to identify an IO's direction
*/
enum class IOType : uint8_t {
  kRead = 0,
  kWrite = 1,
};

struct IOSlice {
  uint32_t size_allocated = 0;
  uint32_t size_used = 0;
  int ssd_id;
  uint64_t epoch;

  // This allows for oversized consumption
  void Consume(uint32_t len) { size_used += len; }

  bool IsFull() const { return size_used >= size_allocated; }
};

}  // namespace flint
