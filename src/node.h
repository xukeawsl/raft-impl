#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "brpc/channel.h"
#include "brpc/server.h"
#include "bthread/mutex.h"
#include "peer.h"
#include "raft.pb.h"
#include "state_machine.h"
#include "store.h"
#include "util.h"

namespace raft {

struct NodeOptions {
    int64_t node_id;
    std::string peers;
    StateMachine* fsm;
    bool node_owns_fsm;

    NodeOptions() : fsm(nullptr), node_owns_fsm(false) {}
};

class Node : public RaftService {
public:
    Node();

    ~Node();

    void RequestVote(google::protobuf::RpcController* cntl_base,
                     const raft::RequestVoteRequest* request,
                     raft::RequestVoteResponse* response,
                     google::protobuf::Closure* done) override;

    void AppendEntries(google::protobuf::RpcController* cntl_base,
                       const raft::AppendEntriesRequest* request,
                       raft::AppendEntriesResponse* response,
                       google::protobuf::Closure* done) override;

    void InstallSnapshot(google::protobuf::RpcController* cntl_base,
                         const raft::InstallSnapshotRequest* request,
                         raft::InstallSnapshotResponse* response,
                         google::protobuf::Closure* done) override;

    bool Init(const NodeOptions& options);

    void Start();

    void Stop();

    void Apply(Task task);

private:
    enum class State { FOLLOWER, CANDIDATE, LEADER };

    using RequestVoteCallback =
        std::function<void(int64_t, const raft::RequestVoteResponse&, bool)>;

    using AppendEntriesCallback =
        std::function<void(int64_t, const raft::AppendEntriesResponse&, bool)>;

    using InstallSnapshotCallback = std::function<void(
        int64_t, const raft::InstallSnapshotResponse&, bool)>;

    void RunLoop();

    bool IsHeartbeatTimeout() const;

    void ResetElectionTimer();

    bool IsElectionTimeout() const;

    void MakeSnapshot();

    void StartElection();

    void StepDown(int64_t term);

    void BecomeLeader();

    void LeaderSubmitNoOpCommand();

    void BroadcastHeartbeat();

    void ReplicateLog(int64_t peer_id);

    void ApplyCommittedEntries();

    bool IsLogUpToDate(int64_t last_log_term, int64_t last_log_index);

    void SaveCurrentTerm();

    void SaveVotedFor();

    int64_t GetLastLogIndex();

    int64_t GetLastLogTerm();

    int64_t GetLogTerm(int64_t index);

    void DeleteLogEntriesBefore(int64_t index);

    void DeleteLogEntriesFrom(int64_t from_index);

    void LoadPersistentState();

    void CreateSnapshot(int64_t last_included_index);

    void SendSnapshot(int64_t peer_id);

    void AsyncRequestVote(int64_t peer_id,
                          const raft::RequestVoteRequest& request,
                          RequestVoteCallback callback);

    void AsyncAppendEntries(int64_t peer_id,
                            const raft::AppendEntriesRequest& request,
                            AppendEntriesCallback callback);

    void AsyncInstallSnapshot(int64_t peer_id,
                              const raft::InstallSnapshotRequest& request,
                              InstallSnapshotCallback callback);

    void HandleRequestVoteResponse(int64_t peer_id,
                                   const raft::RequestVoteRequest& request,
                                   const raft::RequestVoteResponse& response,
                                   bool failed, int64_t start_term);

    void HandleAppendEntriesResponse(
        int64_t peer_id, const raft::AppendEntriesRequest& request,
        const raft::AppendEntriesResponse& response, bool failed);

    void HandleInstallSnapshotResponse(
        int64_t peer_id, const raft::InstallSnapshotRequest& request,
        const raft::InstallSnapshotResponse& response, bool failed);

    StateMachine* _fsm;
    bool _node_owns_fsm;
    std::map<int64_t, Task> _tasks;

    int64_t _node_id;
    std::string _self_address;
    State _state;
    int64_t _current_term;
    int64_t _voted_for;
    int64_t _commit_index;
    int64_t _last_applied;

    // 日志存储
    std::unique_ptr<Store> _store;

    // Leader状态
    std::map<int64_t, int64_t> _next_index;
    std::map<int64_t, int64_t> _match_index;

    // 节点信息
    int64_t _peers_total_count;        // 包含自身
    std::map<int64_t, Peer> _peers;    // 不包含自身

    // 心跳
    std::chrono::steady_clock::time_point _last_heartbeat_time;

    // 选举超时
    mutable bthread::Mutex _mutex;
    std::chrono::steady_clock::time_point _last_reset_time;
    int64_t _election_timeout_ms;
    std::mt19937_64 _rng;    // 使用64位随机数生成器

    // 选举状态
    std::atomic<int64_t> _votes_received{0};

    // 线程控制
    std::atomic<bool> _running{false};
    std::thread _run_thread;

    // RPC服务
    brpc::Server _server;

    // 快照状态
    int64_t _snapshot_last_index{0};
    int64_t _snapshot_last_term{0};
    std::chrono::steady_clock::time_point _last_snapshot_time;
    std::unordered_map<int64_t, bool> _snapshoting_peers;
};

}    // namespace raft