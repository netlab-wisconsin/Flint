#pragma once

#include <chrono>
#include <cstdint>
#include <iomanip>
#include "misc.h"

namespace flint {

/// Return the TSC
static inline size_t rdtsc() {
  uint64_t rax;
  uint64_t rdx;
  asm volatile("rdtsc" : "=a"(rax), "=d"(rdx));
  return static_cast<size_t>((rdx << 32) | rax);
}

/// @note Borrowed from eRPC.
static inline double measure_rdtsc_freq() {
  auto start_time = std::chrono::system_clock::now();
  const uint64_t rdtsc_start = rdtsc();

  uint64_t sum = 5;
  for (uint64_t i = 0; i < 1000000; i++) {
    sum += i + (sum + i) * (i % sum);
  }
  rt_assert(sum == 13580802877818827968ull, "Error in RDTSC freq measurement");

  const uint64_t rdtsc_cycles = rdtsc() - rdtsc_start;
  auto end_time = std::chrono::system_clock::now();
  auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        end_time - start_time)
                        .count();
  const double freq_ghz = rdtsc_cycles * 1.0 / elapsed_ns;
  rt_assert(freq_ghz >= 0.5 && freq_ghz <= 5.0, "Invalid RDTSC frequency");

  return freq_ghz;
}

/// @note Sleep for a short period using rdtsc() for timing.
inline void busy_sleep(size_t sleep_us, double freq_ghz) {
  auto start_tsc = rdtsc();
  auto end_tsc = start_tsc;
  auto cycles = sleep_us * 1000 * freq_ghz;
  while (true) {
    if (unlikely(end_tsc - start_tsc >= cycles))
      break;
    end_tsc = rdtsc();
  }
}

inline size_t ms_to_cycles(double ms, double freq_ghz) {
  return static_cast<size_t>(ms * 1000 * 1000 * freq_ghz);
}

inline size_t us_to_cycles(double us, double freq_ghz) {
  return static_cast<size_t>(us * 1000 * freq_ghz);
}

inline size_t ns_to_cycles(double ns, double freq_ghz) {
  return static_cast<size_t>(ns * freq_ghz);
}

/// @note Sleep for a short period using std::chrono::system_clock for timing.
inline void busy_sleep(size_t sleep_us) {
  auto start_tp = std::chrono::system_clock::now();
  auto end_tp = start_tp;
  while (true) {
    size_t elapsed =
        std::chrono::duration_cast<std::chrono::microseconds>(end_tp - start_tp)
            .count();
    if (unlikely(elapsed >= sleep_us))
      break;
    end_tp = std::chrono::system_clock::now();
  }
}

inline std::string cur_time_str() {
  auto now = std::chrono::system_clock::now();
  std::time_t now_c = std::chrono::system_clock::to_time_t(now);
  std::tm tm_buf;
  localtime_r(&now_c, &tm_buf);
  auto duration = now.time_since_epoch();
  auto microseconds =
      std::chrono::duration_cast<std::chrono::microseconds>(duration) % 1000000;
  std::ostringstream oss;
  oss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S") << '.' << std::setfill('0')
      << std::setw(6) << microseconds.count();
  return oss.str();
}

};  // namespace flint