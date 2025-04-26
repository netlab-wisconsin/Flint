#pragma once
#include <cerrno>
#include <cstring>

#define _log_cur_filename()                                                \
  (__builtin_strrchr(__FILE__, '/') ? __builtin_strrchr(__FILE__, '/') + 1 \
                                    : __FILE__)

namespace flint {
enum LogLevel {
  DEBUG_LEVEL = 0,
  INFO_LEVEL,
  WARN_LEVEL,
  ERROR_LEVEL,
  FATAL_LEVEL,
};

void Log(const enum LogLevel log_level, const char *format, ...);

#define LOG_STRINGIFY(x) #x
#define LOG_TOSTRING(x) LOG_STRINGIFY(x)
#define LOG_PREPEND_FILE_LINE(FMT) ("[%s:" LOG_TOSTRING(__LINE__) "] " FMT)

#ifdef CONFIG_DEBUG
#define LOG_DEBUG(FMT, ...)                                                   \
  Log(LogLevel::DEBUG_LEVEL, LOG_PREPEND_FILE_LINE(FMT), _log_cur_filename(), \
      ##__VA_ARGS__)
#else
#define LOG_DEBUG(FMT, ...) ((void)0)
#endif

#define LOG_INFO(FMT, ...)                                                   \
  Log(LogLevel::INFO_LEVEL, LOG_PREPEND_FILE_LINE(FMT), _log_cur_filename(), \
      ##__VA_ARGS__)

#define LOG_WARN(FMT, ...)                                                   \
  Log(LogLevel::WARN_LEVEL, LOG_PREPEND_FILE_LINE(FMT), _log_cur_filename(), \
      ##__VA_ARGS__)

#define LOG_ERROR(FMT, ...)                                                   \
  Log(LogLevel::ERROR_LEVEL, LOG_PREPEND_FILE_LINE(FMT), _log_cur_filename(), \
      ##__VA_ARGS__)

#define LOG_FATAL(FMT, ...)                                                   \
  Log(LogLevel::FATAL_LEVEL, LOG_PREPEND_FILE_LINE(FMT), _log_cur_filename(), \
      ##__VA_ARGS__)

};  // namespace flint
