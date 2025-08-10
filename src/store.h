#pragma once

#include <string>
#include <vector>

#include "raft.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace raft {

class Store {
public:
    Store(const std::string& db_path);

    ~Store();

    std::string GetLogIndexKey(int64_t index);

    bool Open();

    bool SaveCurrentTerm(int64_t term);

    bool SaveVotedFor(int64_t voted_for);

    bool SaveLogEntry(int64_t index, const raft::LogEntry& entry);

    bool DeleteLogEntriesFrom(int64_t from_index);

    int64_t LoadCurrentTerm();

    int64_t LoadVotedFor();

    raft::LogEntry LoadLogEntry(int64_t index);

    std::vector<raft::LogEntry> LoadLogEntries();

    int64_t GetLastLogIndex();

    // 快照管理
    //   bool SaveSnapshotMetadata(const raft::SnapshotMetadata& metadata);
    //   raft::SnapshotMetadata LoadSnapshotMetadata();
    bool SaveSnapshotChunk(int64_t offset, const std::string& data);

    std::string LoadSnapshotChunk(int64_t offset, size_t size);

private:
    // RocksDB 键名
    const std::string kCurrentTermKey = "current_term";
    const std::string kVotedForKey = "voted_for";
    const std::string kLogPrefix = "log:";

    const std::string kSnapshotMetadataKey = "snapshot_metadata";
    const std::string kSnapshotDataPrefix = "snapshot:";

    const int32_t kLogIndexWidth = 20;

    std::string _db_path;
    rocksdb::DB* _db;
    rocksdb::Options _options;
};

}    // namespace raft