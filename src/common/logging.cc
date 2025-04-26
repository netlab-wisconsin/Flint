#include "logging.h"
#include <cstdarg>
#include <cstdio>

namespace flint {
void Logv(const enum LogLevel log_level, const char* format, va_list ap) {
  static const char* kLogLevelNames[5] = { "DEBUG", "INFO", "WARN",
    "ERROR", "FATAL" };
  
  char new_format[500];
  snprintf(new_format, sizeof(new_format) - 1, "[%s] %s",
    kLogLevelNames[log_level], format);
  vprintf(new_format, ap);
}

void Log(const enum LogLevel log_level, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  Logv(log_level, format, ap);
  va_end(ap);
}
};