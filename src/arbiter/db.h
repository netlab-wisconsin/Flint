#pragma once

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <cstddef>

namespace flint {

#define SLASH '/'
#define SLASH_STR "/"

template <typename... Args>
inline std::string MakeKey(const std::string& first, const Args&... args) {
  if constexpr (sizeof...(args) == 0)
    return SLASH_STR + first;
  else
    return SLASH_STR + first + MakeKey(args...);
}

/// This util function is used to extract the single key item from a key
/// that is usually returned by prefix get operations, which will be of the
/// format /key...prefix/key0/key1/....
/// If pos is 0, key0 will be returned. If pos is 1, key1 will be returned, etc.
inline std::string ExtractSingleKey(const std::string& key,
                                    const std::string& prefix, int pos = 0) {
  auto start = prefix.length();
  int curr_pos = -1;

  if (key.find(prefix) != 0) {
    return "";
  }
  if (key[0] != SLASH || prefix[0] != SLASH) {
    return "";
  }

  size_t next_slash = start;

  while (curr_pos < pos && next_slash != std::string::npos) {
    next_slash = key.find(SLASH, next_slash + 1);
    curr_pos++;
  }

  if (next_slash == std::string::npos) {
    return key.substr(start + 1);
  }

  size_t component_start =
      (curr_pos == 0) ? start + 1 : key.rfind(SLASH, next_slash - 1) + 1;
  return key.substr(component_start, next_slash - component_start);
}

#define FLINT_DEFAULT_CF "flint_metadata"

class DBClient {
 private:
  ROCKSDB_NAMESPACE::Options options_;
  ROCKSDB_NAMESPACE::DB* db_;
  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*> cf_handles_;
  std::map<std::string, ROCKSDB_NAMESPACE::ColumnFamilyHandle*> cf_handles_map_;

 public:
  DBClient() {
    options_.create_if_missing = true;
    Open();
  }

  DBClient(const DBClient&) = delete;
  DBClient(DBClient&&) = delete;

  DBClient& operator=(const DBClient&) = delete;
  DBClient& operator=(DBClient&&) = delete;

  ~DBClient() {
    if (db_ != nullptr)
      Close();
  }

  class WriteBatch {
   private:
    ROCKSDB_NAMESPACE::WriteBatch batch_;
    ROCKSDB_NAMESPACE::DB* db_;
    const std::map<std::string, ROCKSDB_NAMESPACE::ColumnFamilyHandle*>&
        cf_handles_map_;

   public:
    WriteBatch(
        ROCKSDB_NAMESPACE::DB* db,
        const std::map<std::string, ROCKSDB_NAMESPACE::ColumnFamilyHandle*>&
            cf_handles_map)
        : db_(db), cf_handles_map_(cf_handles_map) {}

    void Put(const std::string& cf, const std::string& key,
             const std::string& value);

    void Delete(const std::string& cf, const std::string& key);

    bool Commit();
  };

  bool Open();
  void Close();

  WriteBatch BeginWriteBatch();

  bool CreateCf(const std::string& cf);

  bool DropCf(const std::string& cf);

  bool CfExists(const std::string& cf);

  std::vector<std::string> GetCfNames();

  bool Put(const std::string& cf, const std::string& key,
           const std::string& value);

  bool Get(const std::string& cf, const std::string& key, std::string& value);

  bool GetWithPrefix(const std::string& cf, const std::string& prefix,
                     std::vector<std::string>& keys,
                     std::vector<std::string>& values);

  bool Delete(const std::string& cf, const std::string& key);

  bool KeyExists(const std::string& cf, const std::string& key);
};
};  // namespace flint
