#pragma once
#include <unistd.h>
#include <chrono>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include "../logging.h"

#ifdef CONFIG_DEBUG
#undef NDEBUG
#endif
#include <cassert>

namespace flint {

#undef PAGE_SIZE
#define PAGE_SIZE 4096

#undef _unused
#define _unused __attribute__((unused))

#ifndef likely
#define likely(x) __builtin_expect(!!(x), 1)
#endif

#ifndef unlikely
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif

#ifndef container_of
#define container_of(ptr, type, member)                \
  ({                                                   \
    const typeof(((type *)0)->member) *__mptr = (ptr); \
    (type *)((char *)__mptr - offsetof(type, member)); \
  })
#endif

inline void WriteFixed8(uint8_t value, char *buffer) {
  buffer[0] = value & 0xff;
}

inline void WriteFixed16(uint16_t value, char *buffer) {
  buffer[0] = value & 0xff;
  buffer[1] = (value >> 8) & 0xff;
}

inline void WriteFixed32(uint32_t value, char *buffer) {
  buffer[0] = value & 0xff;
  buffer[1] = (value >> 8) & 0xff;
  buffer[2] = (value >> 16) & 0xff;
  buffer[3] = (value >> 24) & 0xff;
}

inline void WriteFixed64(uint64_t value, char *buffer) {
  buffer[0] = value & 0xff;
  buffer[1] = (value >> 8) & 0xff;
  buffer[2] = (value >> 16) & 0xff;
  buffer[3] = (value >> 24) & 0xff;
  buffer[4] = (value >> 32) & 0xff;
  buffer[5] = (value >> 40) & 0xff;
  buffer[6] = (value >> 48) & 0xff;
  buffer[7] = (value >> 56) & 0xff;
}

inline void ReadFixed8(uint8_t *value, const char *buffer) {
  *value = (static_cast<uint8_t>(static_cast<unsigned char>(buffer[0])));
}

inline void ReadFixed16(uint16_t *value, const char *buffer) {
  *value =
      ((static_cast<uint16_t>(static_cast<unsigned char>(buffer[0]))) |
       (static_cast<uint16_t>(static_cast<unsigned char>(buffer[1])) << 8));
}

inline void ReadFixed32(uint32_t *value, const char *buffer) {
  *value =
      ((static_cast<uint32_t>(static_cast<unsigned char>(buffer[0]))) |
       (static_cast<uint32_t>(static_cast<unsigned char>(buffer[1])) << 8) |
       (static_cast<uint32_t>(static_cast<unsigned char>(buffer[2])) << 16) |
       (static_cast<uint32_t>(static_cast<unsigned char>(buffer[3])) << 24));
}

inline void ReadFixed64(uint64_t *value, const char *buffer) {
  uint32_t lo;
  uint32_t hi;
  ReadFixed32(&lo, buffer);
  ReadFixed32(&hi, buffer + 4);
  *value = ((uint64_t)hi << 32) | lo;
}

/* maths */
#define ROUND_UP(number, base) (((number) + ((base) - 1)) / (base)) * (base)
#define ROUND_DOWN(number, base) ((number) / (base)) * (base)

/* misc */
#define KiB(i) (i * (static_cast<size_t>(1) << 10))
#define MiB(i) (i * (static_cast<size_t>(1) << 20))
#define GiB(i) (i * (static_cast<size_t>(1) << 30))
#define TiB(i) (i * (static_cast<size_t>(1) << 40))

#define enable_timer std::chrono::time_point<std::chrono::steady_clock> _st, _et
#define start_timing _st = std::chrono::steady_clock::now()
#define end_timing(msg, args...)                                            \
  do {                                                                      \
    _et = std::chrono::steady_clock::now();                                 \
    double _elapsed =                                                       \
        (double)std::chrono::duration_cast<std::chrono::microseconds>(_et - \
                                                                      _st)  \
            .count() /                                                      \
        1000;                                                               \
    LOG_DEBUG("[%.3fms] " msg "\n", _elapsed, ##args);                      \
  } while (0)

inline bool check_aligned(char *data) {
  return (reinterpret_cast<uintptr_t>(data) % PAGE_SIZE == 0);
}

/// Credit: https://stackoverflow.com/a/26221725
template <typename... Args>
std::string string_format(const std::string &format, Args... args) {
  int size_s = std::snprintf(nullptr, 0, format.c_str(), args...) + 1;
  if (size_s <= 0)
    throw std::runtime_error("Error during formatting string");
  auto size = static_cast<size_t>(size_s);
  std::unique_ptr<char[]> buf(new char[size]);
  std::snprintf(buf.get(), size, format.c_str(), args...);
  return std::string(buf.get(), buf.get() + size - 1);
}

inline std::vector<std::string> string_split(const std::string &s,
                                             const std::string &delim) {
  size_t pos_start = 0, pos_end, delim_len = delim.length();
  std::string token;
  std::vector<std::string> res;

  while ((pos_end = s.find(delim, pos_start)) != std::string::npos) {
    token = s.substr(pos_start, pos_end - pos_start);
    pos_start = pos_end + delim_len;
    res.push_back(token);
  }

  res.push_back(s.substr(pos_start));
  return res;
}

// Real-time assert, throw an std::runtime_error if the condition is false
template <typename... Args>
static inline void rt_assert(bool cond, const std::string &format,
                             Args... args) {
  if (unlikely(!cond)) {
    std::string error_str = string_format(format, args...);
    throw std::runtime_error(error_str);
  }
}

// Real-time assert, throw an std::runtime_error if the condition is false
// @param cond
inline void rt_assert(bool cond) {
  if (unlikely(!cond))
    throw std::runtime_error("Error");
}

inline std::string GenerateUUID(int uuid_len) {
  const static char *uuid_chars = "0123456789abcdefghijklmnopqrstuvwxyz";
  const static int len_chars = 36;
  static std::random_device r;
  static std::mt19937 rng(r());
  static std::uniform_int_distribution<> dist(0, len_chars - 1);
  std::stringstream ss;

  for (int i = 0; i < uuid_len; i++)
    ss << uuid_chars[dist(rng)];

  return ss.str();
}

/// Get current timestamp in micro second.
inline auto GetCurrentTimeMicro() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

/// Get current timestamp in millisecond.
inline auto GetCurrentTimeMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline std::string GetHostname() {
  char hostname[1024];
  gethostname(hostname, sizeof(hostname));
  return std::string(hostname);
}

};  // namespace flint
