#include "shadow_view.h"
#include "common/utils/misc.h"

// The time window for real-time measurement of bandwidth, iops, etc.
static constexpr int kRealTimeMeasureWindowMillis = 200;
static constexpr float kEwmaAlpha = 0.3f;

static void UpdateSizeDist(uint32_t dist[kSizeDistGranularity], size_t size) {
  if (size == 0)
    return;
  int bucket_idx = 0;
  double log_size = std::log2(size);
  bucket_idx = static_cast<int>(log_size);
  bucket_idx = std::min(bucket_idx, kSizeDistGranularity - 1);
  dist[bucket_idx]++;
}

namespace flint {
void PortView::RecordIO(size_t size, IOType type) {
  recency_counter++;
  if (type == IOType::kRead) {
    UpdateSizeDist(read_size_dist, size);
  } else {
    UpdateSizeDist(write_size_dist, size);
  }
  if (last_update_tp_ == 0) {
    last_update_tp_ = GetCurrentTimeMillis();
    return;
  }
  if (type == IOType::kRead) {
    windowed_read_bytes_ += size;
  } else {
    windowed_write_bytes_ += size;
  }
  auto now = GetCurrentTimeMillis();
  auto elapsed = now - last_update_tp_;
  if (elapsed < kRealTimeMeasureWindowMillis) {
    return;
  }
  float cur_bw;
  if (type == IOType::kRead) {
    cur_bw = 1.0f * windowed_read_bytes_ / (elapsed / 1000.0);
    read_bw = kEwmaAlpha * cur_bw + (1 - kEwmaAlpha) * read_bw;
    windowed_read_bytes_ = 0;
  } else {
    cur_bw = 1.0f * windowed_write_bytes_ / (elapsed / 1000.0);
    write_bw = kEwmaAlpha * cur_bw + (1 - kEwmaAlpha) * write_bw;
    windowed_write_bytes_ = 0;
  }
  last_update_tp_ = now;
}

void PipeView::RecordIO(size_t size, IOType type) {
  recency_counter++;
  if (last_update_tp_ == 0) {
    last_update_tp_ = GetCurrentTimeMillis();
    return;
  }
  if (type == IOType::kRead) {
    windowed_read_bytes_ += size;
  } else {
    windowed_write_bytes_ += size;
  }
  auto now = GetCurrentTimeMillis();
  auto elapsed = now - last_update_tp_;
  if (elapsed < kRealTimeMeasureWindowMillis) {
    return;
  }
  float cur_bw;
  if (type == IOType::kRead) {
    cur_bw = 1.0f * windowed_read_bytes_ / elapsed;
    read_bw = kEwmaAlpha * cur_bw + (1 - kEwmaAlpha) * read_bw;
    windowed_read_bytes_ = 0;
  } else {
    cur_bw = 1.0f * windowed_write_bytes_ / elapsed;
    write_bw = kEwmaAlpha * cur_bw + (1 - kEwmaAlpha) * write_bw;
    windowed_write_bytes_ = 0;
  }
  last_update_tp_ = now;
}

void SsdView::RecordIO(size_t size, IOType type, int latency) {
  recency_counter++;
  if (type == IOType::kRead) {
    windowed_read_bytes_ += size;
  } else {
    windowed_write_bytes_ += size;
  }
  auto now = GetCurrentTimeMillis();
  auto elapsed = now - last_update_tp_;
  if (elapsed > kRealTimeMeasureWindowMillis) {
    return;
  }
  float cur_bw;
  if (type == IOType::kRead) {
    cur_bw = 1.0f * windowed_read_bytes_ / elapsed;
    read_bw_used = kEwmaAlpha * cur_bw + (1 - kEwmaAlpha) * read_bw_used;
    windowed_read_bytes_ = 0;
  } else {
    cur_bw = 1.0f * windowed_write_bytes_ / elapsed;
    write_bw_used = kEwmaAlpha * cur_bw + (1 - kEwmaAlpha) * write_bw_used;
    windowed_write_bytes_ = 0;
    write_lat_ = kEwmaAlpha * latency + (1 - kEwmaAlpha) * write_lat_;
  }
  if (write_lat_ > kWriteLatMax) {
    write_cost = (write_cost + kWcBaseline) / 2;
  } else {
    write_cost -= kWcDescfactor;
  }
  write_cost = std::min(write_cost, kWcBaseline);
  write_cost = std::max(write_cost, 1.0f);
  last_update_tp_ = now;
}

}  // namespace flint
