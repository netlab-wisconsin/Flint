#include "ssd_manager.h"
#include <cstdint>
#include "common/logging.h"
#include "common/ssd.h"
#include "db.h"
#include "internal.h"

extern "C" {
#include <nvme/tree.h>
}

namespace flint {
namespace arbiter {

/// SSD metadata
/// structure:
/// ssd_meta:
///   nqn:
///     free_segments:
///       [start, end]
///     volume_segments:
///       volume_name:
///         [start, end]
///     extent_usage:
///       volume_name -> extent_num
#define KEY_SSD_META "ssd_meta"
#define KEY_SSD_NQN "nqn"
#define KEY_SSD_FREE_SEGMENTS "free_segments"
#define KEY_SSD_VOLUME_SEGMENTS "volume_segments"
#define KEY_SSD_EXTENT_USAGE "extent_usage"

using json = nlohmann::json;

void from_json(const json& j, Segments& segs) {
  segs.intervals = j.get<Segments::Intervals>();
}

void to_json(json& j, const Segments& segs) {
  j = segs.intervals;
}

void SsdManager::load_metadata() {
  std::string cfname = MakeKey(KEY_SSD_META, ssd_->nqn);
  if (!db_client_->CfExists(cfname)) {
    LOG_WARN("ssd %s not formatted, skipped\n", ssd_->sys_path.c_str());
    return;
  }
  std::string val_str;
  db_client_->Get(cfname, KEY_SSD_FREE_SEGMENTS, val_str);
  free_segments_ = json::parse(val_str);
  std::vector<std::string> keys;
  std::vector<std::string> values;
  db_client_->GetWithPrefix(cfname, KEY_SSD_VOLUME_SEGMENTS, keys, values);
  for (size_t i = 0; i < keys.size(); ++i) {
    volume_segments_[ExtractSingleKey(keys[i], KEY_SSD_VOLUME_SEGMENTS)] =
        json::parse(values[i]);
  }
  db_client_->GetWithPrefix(cfname, KEY_SSD_EXTENT_USAGE, keys, values);
  for (size_t i = 0; i < keys.size(); ++i) {
    extent_usage_map_[ExtractSingleKey(keys[i], KEY_SSD_EXTENT_USAGE)] =
        std::stoull(values[i]);
  }
  for (auto& [volume_name, segs] : volume_segments_) {
    auto ext_usage = extent_usage_map_.at(volume_name);
    rt_assert(segs.GetExtentCount() == ext_usage,
              "volume %s has %zu segments, but %zu extent usage",
              volume_name.c_str(), segs.intervals.size(), ext_usage);
  }
}

void SsdManager::PersistMetadata() {
  std::string cfname = MakeKey(KEY_SSD_META, ssd_->nqn);
  db_client_->CreateCf(cfname);
  auto batch = db_client_->BeginWriteBatch();
  json j = free_segments_;
  batch.Put(cfname, KEY_SSD_FREE_SEGMENTS, j.dump());
  for (auto& [volume_name, segs] : volume_segments_) {
    j = segs;
    batch.Put(cfname, MakeKey(KEY_SSD_VOLUME_SEGMENTS, volume_name), j.dump());
  }
  for (auto& [volume_name, extent_num] : extent_usage_map_) {
    j = extent_num;
    batch.Put(cfname, MakeKey(KEY_SSD_EXTENT_USAGE, volume_name), j.dump());
  }
  rt_assert(batch.Commit(), "failed to persist metadata");
}

SsdManager::SsdManager(std::shared_ptr<Ssd> ssd, bool load)
    : ssd_(ssd), db_client_(internal::g_db_client) {
  if (load)
    load_metadata();
}

int32_t SsdManager::AllocLExtent(const std::string& volume_name,
                                 uint64_t extent_num) {
  if (free_segments_.intervals.empty())
    return -1;
  auto& free_seg = free_segments_.intervals.front();
  auto pext_num = free_seg.first;
  free_seg.first += 1;
  if (unlikely(free_seg.first > free_seg.second))
    free_segments_.intervals.erase(free_segments_.begin());
  volume_segments_[volume_name].AddExtent(pext_num);
  extent_usage_map_[volume_name]++;
  return pext_num;
}

void SsdManager::FreeVolume(const std::string& volume_name) {
  free_segments_.AddSegments(volume_segments_[volume_name]);
  volume_segments_.erase(volume_name);
  extent_usage_map_.erase(volume_name);
}

void SsdManager::ResetMetadata() {
  free_segments_.intervals.clear();
  free_segments_.intervals.push_back({0, capacity_total_ / kExtentSize - 1});
  volume_segments_.clear();
  extent_usage_map_.clear();
  PersistMetadata();
}

static const char* nvme_strerror(int errnum) {
  if (errnum >= ENVME_CONNECT_RESOLVE)
    return nvme_errno_to_string(errnum);
  return strerror(errnum);
}

int SsdManager::Create(SsdPoolManager& ssd_pool_manager,
                       const std::optional<SsdIdMap>& ssd_id_map_opt,
                       const std::optional<std::set<std::string>>& devlist_opt,
                       bool load) {
  nvme_host_t h;
  nvme_subsystem_t s;
  nvme_ctrl_t c;
  nvme_ns_t n;
  nvme_root_t r;
  nvme_path_t p;
  int err = 0;
  std::string subsysnqn;
  std::string devname;
  size_t lba;

  r = nvme_create_root(nullptr, 0);
  if (!r) {
    LOG_FATAL("Failed to create topology root: %s", nvme_strerror(errno));
    return -1;
  }
  err = nvme_scan_topology(r, nullptr, nullptr);
  if (err < 0) {
    LOG_FATAL("Failed to scan topology: %s", nvme_strerror(errno));
    return -1;
  }

  auto add_nvme_ns = [&](nvme_ns_t n, const std::string& nqn) {
    auto ssd = std::make_shared<Ssd>();
    devname.assign(nvme_ns_get_name(n));
    if (devlist_opt && devlist_opt->count(devname) == 0)
      return;
    lba = nvme_ns_get_lba_size(n);
    if (lba != kSSDBlockSize) {
      LOG_WARN("device %s has a block size %lu != %lu, skip..\n",
               devname.c_str(), lba, kSSDBlockSize);
      return;
    }
    auto nsze = nvme_ns_get_lba_count(n) * lba;
    auto nuse = nvme_ns_get_lba_util(n) * lba;
    ssd->nqn = subsysnqn;
    ssd->sys_path = std::string(LINUX_DEVICE_PATH) + devname;
    if (ssd_id_map_opt && ssd_id_map_opt->count(subsysnqn) == 1)
      ssd->id = ssd_id_map_opt->at(subsysnqn);
    auto sm = std::make_shared<SsdManager>(ssd, load);
    sm->capacity_total_ = nsze;
    sm->capacity_used_ = nuse;
    ssd_pool_manager.emplace(subsysnqn, sm);
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
          add_nvme_ns(n, subsysnqn);
        }

        nvme_ctrl_for_each_path(c, p) {
          n = nvme_path_get_ns(p);
          if (n)
            add_nvme_ns(n, subsysnqn);
        }
      }
    }
  }

  nvme_free_tree(r);

  return 0;
}

}  // namespace arbiter
}  // namespace flint