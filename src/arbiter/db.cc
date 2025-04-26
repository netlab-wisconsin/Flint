#include "db.h"
#include <filesystem>
#include "internal.h"

namespace flint {
using DBStatus = ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::ColumnFamilyOptions;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Iterator;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::WriteOptions;

bool DBClient::Open() {
  DB* db = nullptr;
  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyDescriptor> cf_descriptors;
  DBStatus s;

  /* in case the rocksdb directory does not contain valid files, create it */
  if (!std::filesystem::exists(ROCKSDB_HOME)) {
    s = DB::Open(options_, ROCKSDB_HOME, &db);
    if (!s.ok()) {
      LOG_ERROR("error opening rocksdb at %s, %s\n", ROCKSDB_HOME,
                s.ToString().c_str());
      return false;
    }
    s = db->Close();
    if (!s.ok()) {
      LOG_ERROR("error closing rocksdb, %s\n", s.ToString().c_str());
      return false;
    }
  }

  std::filesystem::path lockfile_path = ROCKSDB_HOME;
  lockfile_path /= "LOCK";
  if (std::filesystem::exists(lockfile_path)) {
    std::filesystem::remove(lockfile_path);
  }

  s = DB::ListColumnFamilies(options_, ROCKSDB_HOME, &cf_names);
  if (!s.ok()) {
    LOG_ERROR("error listing all rocksdb column families, %s\n",
              s.ToString().c_str());
    return false;
  }
  for (const std::string& cf : cf_names)
    cf_descriptors.emplace_back(cf, ColumnFamilyOptions());

  s = DB::Open(options_, ROCKSDB_HOME, cf_descriptors, &cf_handles_, &db);
  if (!s.ok()) {
    LOG_ERROR("error opening rocksdb, %s\n", s.ToString().c_str());
    return false;
  }
  for (auto handle : cf_handles_)
    cf_handles_map_.emplace(handle->GetName(), handle);
  db_ = db;
  return true;
}

void DBClient::Close() {
  DBStatus s;
  for (auto handle : cf_handles_) {
    s = db_->DestroyColumnFamilyHandle(handle);
    if (!s.ok()) {
      LOG_ERROR("error destroying column family handle, %s\n",
                s.ToString().c_str());
    }
  }
  s = db_->Close();
  if (!s.ok()) {
    LOG_ERROR("error closing rocksdb, %s\n", s.ToString().c_str());
  }
  cf_handles_.clear();
  cf_handles_map_.clear();
  delete db_;
  db_ = nullptr;
}

bool DBClient::CreateCf(const std::string& cf) {
  ColumnFamilyHandle* cf_handle;

  if (cf_handles_map_.count(cf) > 0)
    return true;
  DBStatus s = db_->CreateColumnFamily(ColumnFamilyOptions(), cf, &cf_handle);
  if (!s.ok()) {
    LOG_ERROR("error creating column family %s, %s\n", cf.c_str(),
              s.ToString().c_str());
    return false;
  }
  s = db_->DestroyColumnFamilyHandle(cf_handle);
  if (!s.ok()) {
    LOG_ERROR("error destroying column family handle, %s\n",
              s.ToString().c_str());
    return false;
  }
  Close();
  return Open();
}

bool DBClient::DropCf(const std::string& cf) {
  ColumnFamilyHandle* cf_handle;

  if (cf_handles_map_.count(cf) == 0)
    return true;
  cf_handle = cf_handles_map_[cf];
  DBStatus s = db_->DropColumnFamily(cf_handle);
  if (!s.ok()) {
    LOG_ERROR("error destroying column family handle, %s\n",
              s.ToString().c_str());
    return false;
  }
  s = db_->DestroyColumnFamilyHandle(cf_handle);
  if (!s.ok()) {
    LOG_ERROR("error destroying column family handle, %s\n",
              s.ToString().c_str());
    return false;
  }
  Close();
  return Open();
}

bool DBClient::CfExists(const std::string& cf) {
  return cf_handles_map_.count(cf) == 1;
}

std::vector<std::string> DBClient::GetCfNames() {
  std::vector<std::string> cfs;
  for (const auto& [cf, _] : cf_handles_map_)
    cfs.push_back(cf);
  return cfs;
}

bool DBClient::Put(const std::string& cf, const std::string& key,
                   const std::string& value) {
  if (cf_handles_map_.count(cf) == 0) {
    LOG_ERROR("column family %s does not exist\n", cf.c_str());
    return false;
  }
  DBStatus s =
      db_->Put(WriteOptions(), cf_handles_map_[cf], Slice(key), Slice(value));
  if (!s.ok()) {
    LOG_ERROR("error putting key %s on rocksdb, %s\n", key.c_str(),
              s.ToString().c_str());
    return false;
  }
  return true;
}

bool DBClient::Get(const std::string& cf, const std::string& key,
                   std::string& value) {
  if (cf_handles_map_.count(cf) == 0) {
    LOG_ERROR("column family %s does not exist\n", cf.c_str());
    return false;
  }
  DBStatus s = db_->Get(ReadOptions(), cf_handles_map_[cf], Slice(key), &value);
  if (!s.ok()) {
    if (s.IsNotFound())
      return false;
    LOG_ERROR("error getting key %s on rocksdb, %s\n", key.c_str(),
              s.ToString().c_str());
    return false;
  }
  return true;
}

bool DBClient::GetWithPrefix(const std::string& cf, const std::string& prefix,
                             std::vector<std::string>& keys,
                             std::vector<std::string>& values) {
  if (cf_handles_map_.count(cf) == 0) {
    LOG_ERROR("column family %s does not exist\n", cf.c_str());
    return false;
  }
  Iterator* it = db_->NewIterator(ReadOptions(), cf_handles_map_[cf]);
  for (it->Seek(prefix); it->Valid(); it->Next()) {
    Slice key = it->key();
    if (!key.starts_with(prefix))
      break;
    keys.push_back(it->key().ToString());
    values.push_back(it->value().ToString());
  }
  delete it;
  return true;
}

bool DBClient::Delete(const std::string& cf, const std::string& key) {
  if (cf_handles_map_.count(cf) == 0) {
    LOG_ERROR("column family %s does not exist\n", cf.c_str());
    return false;
  }
  DBStatus s = db_->Delete(WriteOptions(), cf_handles_map_[cf], Slice(key));
  if (!s.ok()) {
    LOG_ERROR("error deleting key %s on rocksdb, %s\n", key.c_str(),
              s.ToString().c_str());
    return false;
  }
  return true;
}

bool DBClient::KeyExists(const std::string& cf, const std::string& key) {
  if (cf_handles_map_.count(cf) == 0) {
    LOG_ERROR("column family %s does not exist\n", cf.c_str());
    return false;
  }
  std::string tmp;
  DBStatus s = db_->Get(ReadOptions(), cf_handles_map_[cf], Slice(key), &tmp);
  if (s.IsNotFound())
    return false;
  return true;
}

DBClient::WriteBatch DBClient::BeginWriteBatch() {
  return WriteBatch(db_, cf_handles_map_);
}

void DBClient::WriteBatch::Put(const std::string& cf, const std::string& key,
                               const std::string& value) {
  if (cf_handles_map_.count(cf) == 0) {
    LOG_ERROR("column family %s does not exist\n", cf.c_str());
    return;
  }
  auto cf_handle = cf_handles_map_.at(cf);
  DBStatus s = batch_.Put(cf_handle, Slice(key), Slice(value));
  if (!s.ok()) {
    LOG_ERROR("error putting key %s on rocksdb, %s\n", key.c_str(),
              s.ToString().c_str());
    return;
  }
}

void DBClient::WriteBatch::Delete(const std::string& cf,
                                  const std::string& key) {
  if (cf_handles_map_.count(cf) == 0) {
    LOG_ERROR("column family %s does not exist\n", cf.c_str());
    return;
  }
  auto cf_handle = cf_handles_map_.at(cf);
  DBStatus s = batch_.Delete(cf_handle, Slice(key));
  if (!s.ok()) {
    LOG_ERROR("error deleting key %s on rocksdb, %s\n", key.c_str(),
              s.ToString().c_str());
    return;
  }
}

bool DBClient::WriteBatch::Commit() {
  DBStatus s = db_->Write(WriteOptions(), &batch_);
  if (!s.ok()) {
    LOG_ERROR("error committing write batch, %s\n", s.ToString().c_str());
    return false;
  }
  return true;
}

}  // namespace flint