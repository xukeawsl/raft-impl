#include "store.h"

#include "util.h"

namespace raft {

Store::Store(const std::string& db_path) : _db_path(db_path), _db(nullptr) {
    // 配置 RocksDB 选项
    _options.create_if_missing = true;
    _options.create_missing_column_families = true;
    _options.max_open_files = 100;
    _options.keep_log_file_num = 3;
    _options.max_log_file_size = 100 * 1024 * 1024;    // 100MB
    _options.max_background_jobs = 4;
}

Store::~Store() {
    if (_db) {
        rocksdb::Status status = _db->Close();
        if (!status.ok()) {
            SPDLOG_ERROR("Failed to close RocksDB: {}", status.ToString());
        }
        delete _db;
    }
}

std::string Store::GetLogIndexKey(int64_t index) {
    std::string index_str = std::to_string(index);
    return kLogPrefix + std::string(kLogIndexWidth - index_str.length(), '0') +
           index_str;
}

bool Store::Open() {
    rocksdb::Status status = rocksdb::DB::Open(_options, _db_path, &_db);
    if (!status.ok()) {
        SPDLOG_ERROR("Failed to open RocksDB: {}", status.ToString());
        return false;
    }
    return true;
}

bool Store::SaveCurrentTerm(int64_t term) {
    std::string value = std::to_string(term);
    rocksdb::Status status =
        _db->Put(rocksdb::WriteOptions(), kCurrentTermKey, value);
    return status.ok();
}

bool Store::SaveVotedFor(int64_t voted_for) {
    std::string value = std::to_string(voted_for);
    rocksdb::Status status =
        _db->Put(rocksdb::WriteOptions(), kVotedForKey, value);
    return status.ok();
}

bool Store::SaveLogEntry(int64_t index, const raft::LogEntry& entry) {
    std::string key = GetLogIndexKey(index);
    std::string value;
    if (!entry.SerializeToString(&value)) {
        SPDLOG_ERROR("Failed to serialize log entry");
        return false;
    }
    rocksdb::Status status = _db->Put(rocksdb::WriteOptions(), key, value);
    return status.ok();
}

bool Store::DeleteLogEntriesFrom(int64_t from_index) {
    std::string start_key = GetLogIndexKey(from_index);
    std::string end_key = GetLogIndexKey(INT64_MAX);

    rocksdb::Slice start(start_key);
    rocksdb::Slice end(end_key);

    rocksdb::Status status = _db->DeleteRange(
        rocksdb::WriteOptions(), _db->DefaultColumnFamily(), start, end);
    return status.ok();
}

int64_t Store::LoadCurrentTerm() {
    std::string value;
    rocksdb::Status status =
        _db->Get(rocksdb::ReadOptions(), kCurrentTermKey, &value);
    if (status.IsNotFound()) {
        return 0;
    }
    if (!status.ok()) {
        SPDLOG_ERROR("Failed to load current term: {}", status.ToString());
        return 0;
    }
    return std::stoll(value);
}

int64_t Store::LoadVotedFor() {
    std::string value;
    rocksdb::Status status =
        _db->Get(rocksdb::ReadOptions(), kVotedForKey, &value);
    if (status.IsNotFound()) {
        return -1;
    }
    if (!status.ok()) {
        SPDLOG_ERROR("Failed to load voted for: {}", status.ToString());
        return -1;
    }
    return std::stoll(value);
}

raft::LogEntry Store::LoadLogEntry(int64_t index) {
    std::string key = GetLogIndexKey(index);
    std::string value;
    rocksdb::Status status = _db->Get(rocksdb::ReadOptions(), key, &value);

    raft::LogEntry entry;
    if (status.ok()) {
        if (!entry.ParseFromString(value)) {
            SPDLOG_ERROR("Failed to parse log entry at index {}", index);
        }
    }
    return entry;
}

std::vector<raft::LogEntry> Store::LoadLogEntries() {
    std::vector<raft::LogEntry> entries;

    std::string start_key = GetLogIndexKey(0);
    std::string end_key = GetLogIndexKey(INT64_MAX);

    rocksdb::Iterator* it = _db->NewIterator(rocksdb::ReadOptions());
    for (it->Seek(start_key); it->Valid() && it->key().ToString() < end_key;
         it->Next()) {
        raft::LogEntry entry;
        if (entry.ParseFromString(it->value().ToString())) {
            entries.push_back(entry);
        } else {
            SPDLOG_ERROR("Failed to parse log entry at key {}",
                         it->key().ToString());
        }
    }

    delete it;
    return entries;
}

int64_t Store::GetLastLogIndex() {
    std::string prefix = kLogPrefix;
    rocksdb::Iterator* it = _db->NewIterator(rocksdb::ReadOptions());

    // 反向迭代：从最后开始往前找
    it->SeekToLast();

    int64_t last_index = 0;
    while (it->Valid()) {
        if (it->key().starts_with(prefix)) {
            std::string key = it->key().ToString();
            try {
                // 直接提取索引部分
                last_index = std::stoll(key.substr(prefix.length()));
                break;    // 找到第一个即退出
            } catch (...) {
                // 继续尝试前一个
                it->Prev();
            }
        } else {
            it->Prev();
        }
    }

    delete it;
    return last_index;
}

// bool Store::SaveSnapshotMetadata(const raft::SnapshotMetadata& metadata) {
//   std::string value;
//   if (!metadata.SerializeToString(&value)) {
//     SPDLOG_ERROR("Failed to serialize snapshot metadata");
//     return false;
//   }
//   rocksdb::Status status = _db->Put(rocksdb::WriteOptions(),
//   kSnapshotMetadataKey, value); return status.ok();
// }

// raft::SnapshotMetadata Store::LoadSnapshotMetadata() {
//   std::string value;
//   rocksdb::Status status = _db->Get(rocksdb::ReadOptions(),
//   kSnapshotMetadataKey, &value);

//     return false;
//   }
//   rocksdb::Status status = _db->Put(rocksdb::WriteOptions(),
//   kSnapshotMetadataKey, value); return status.ok();
// }

// raft::SnapshotMetadata Store::LoadSnapshotMetadata() {
//   std::string value;
//   rocksdb::Status status = _db->Get(rocksdb::ReadOptions(),
//   kSnapshotMetadataKey, &value);

//   raft::SnapshotMetadata metadata;
//   if (status.ok()) {
//     if (!metadata.ParseFromString(value)) {
//       LOG(ERROR) << "Failed to parse snapshot metadata";
//     }
//   }
//   return metadata;
// }

bool Store::SaveSnapshotChunk(int64_t offset, const std::string& data) {
    std::string key = kSnapshotDataPrefix + std::to_string(offset);
    rocksdb::Status status = _db->Put(rocksdb::WriteOptions(), key, data);
    return status.ok();
}

std::string Store::LoadSnapshotChunk(int64_t offset, size_t size) {
    std::string key = kSnapshotDataPrefix + std::to_string(offset);
    std::string value;
    rocksdb::Status status = _db->Get(rocksdb::ReadOptions(), key, &value);

    if (status.ok() && value.size() == size) {
        return value;
    }
    return "";
}

}    // namespace raft