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

    bool Open();

    bool SaveCurrentTerm(int64_t term);

    bool SaveVotedFor(int64_t voted_for);

    bool SaveLogEntry(int64_t index, const raft::LogEntry& entry);

    int64_t GetLastLogIndex();

    bool DeleteLogEntriesRange(int64_t start_index, int64_t end_index);

    bool DeleteLogEntriesBefore(int64_t index);

    bool DeleteLogEntriesFrom(int64_t from_index);

    int64_t LoadCurrentTerm();

    int64_t LoadVotedFor();

    raft::LogEntry LoadLogEntry(int64_t index);

    std::vector<raft::LogEntry> LoadLogEntries();

    bool SaveSnapshotMetaData(const raft::SnapshotMetaData& meta);

    raft::SnapshotMetaData LoadSnapshotMetaData();

private:
    std::string GetLogIndexKey(int64_t index);

    const std::string kCurrentTermKey = "current_term";
    const std::string kVotedForKey = "voted_for";
    const std::string kLogPrefix = "log:";
    const std::string kSnapShotMetaData = "snapshot_meta_data";
    const int32_t kLogIndexWidth = 20;

    std::string _db_path;
    rocksdb::DB* _db;
    rocksdb::Options _options;
    rocksdb::WriteOptions _write_options;
    rocksdb::ReadOptions _read_options;
};

}    // namespace raft