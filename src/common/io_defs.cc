#include "io_defs.h"

namespace flint {

using json = nlohmann::json;

void from_json(const json &j, ExtentLocator &extent_locator) {
  extent_locator.rep_role = static_cast<ChainRepRole>(j[0]);
  extent_locator.ssd_id = j[1];
  extent_locator.pext_num = j[2];
}

void to_json(json &j, const ExtentLocator &extent_locator) {
  j = json::array({static_cast<int>(extent_locator.rep_role),
                   extent_locator.ssd_id, extent_locator.pext_num});
}

void from_json(const json &j, ExtentLocators &extent_locators) {
  for (const auto &json_locator : j) {
    ExtentLocator extent_locator;
    from_json(json_locator, extent_locator);
    extent_locators.push_back(extent_locator);
  }
}

void to_json(json &j, const ExtentLocators &extent_locators) {
  j = json::array();
  for (const auto &locator : extent_locators) {
    json tmp_j;
    to_json(tmp_j, locator);
    j.push_back(tmp_j);
  }
}

void from_json(const json &j, VolumeExtLocatorMap &extent_locators_map) {
  for (auto it = j.begin(); it != j.end(); ++it) {
    size_t key = std::stoul(it.key());
    const json &value = it.value();

    ExtentLocators locators;
    from_json(value, locators);
    extent_locators_map[key] = locators;
  }
}

void to_json(json &j, const VolumeExtLocatorMap &extent_locators_map) {
  for (const auto &p : extent_locators_map) {
    json locator_j;
    to_json(locator_j, p.second);
    j[std::to_string(p.first)] = locator_j;
  }
}

}  // namespace flint
