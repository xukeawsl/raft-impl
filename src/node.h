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
#include "store.h"
#include "util.h"

namespace raft {

class Node : public RaftService {
public:
    Node(int64_t node_id, const std::vector<Peer>& peers);
    ~Node();

    void RequestVote(google::protobuf::RpcController* cntl_base,
                     const raft::RequestVoteRequest* request,
                     raft::RequestVoteResponse* response,
                     google::protobuf::Closure* done) override;

    void AppendEntries(google::protobuf::RpcController* cntl_base,
                       const raft::AppendEntriesRequest* request,
                       raft::AppendEntriesResponse* response,
                       google::protobuf::Closure* done) override;

    void Start();

    void Stop();

    bool SubmitCommand(const std::string& command);

private:
    struct PeerConnection {
        Peer peer;
        std::unique_ptr<brpc::Channel> channel;
        std::chrono::steady_clock::time_point last_attempt_time;
        bool connected;
    };

    enum class State { FOLLOWER, CANDIDATE, LEADER };

    using RequestVoteCallback =
        std::function<void(int64_t, const raft::RequestVoteResponse&, bool)>;

    using AppendEntriesCallback =
        std::function<void(int64_t, const raft::AppendEntriesResponse&, bool)>;

    // using InstallSnapshotCallback = std::function<void(int64_t, const
    // raft::InstallSnapshotResponse&, bool)>;

    void RunLoop();

    bool IsHeartbeatTimeout() const;

    void ResetElectionTimer();

    bool IsElectionTimeout() const;

    void StartElection();

    void BecomeLeader();

    void BroadcastHeartbeat();

    void ReplicateLog(int64_t peer_id);

    void ApplyCommittedEntries();

    bool IsLogUpToDate(int64_t last_log_term, int64_t last_log_index) const;

    void SaveCurrentTerm();

    void SaveVotedFor();

    void SaveLogEntry(int64_t index);

    void SaveCommitIndex();

    void DeleteLogEntriesFrom(int64_t from_index);

    void LoadPersistentState();

    // 快照管理
    // void CreateSnapshot(int64_t last_included_index);
    // void SendSnapshot(int64_t peer_id);

    // 确保通道连接
    bool EnsureChannelConnected(int64_t peer_id);

    // 尝试重新连接所有节点
    void TryReconnectPeers();

    void AsyncRequestVote(int64_t peer_id,
                          const raft::RequestVoteRequest& request,
                          RequestVoteCallback callback);

    void AsyncAppendEntries(int64_t peer_id,
                            const raft::AppendEntriesRequest& request,
                            AppendEntriesCallback callback);
    //   void AsyncInstallSnapshot(int64_t peer_id, const
    //   raft::InstallSnapshotRequest& request,
    //                             InstallSnapshotCallback callback);

    void HandleRequestVoteResponse(int64_t peer_id,
                                   const raft::RequestVoteRequest& request,
                                   const raft::RequestVoteResponse& response,
                                   bool failed, int64_t start_term);

    void HandleAppendEntriesResponse(
        int64_t peer_id, const raft::AppendEntriesRequest& request,
        const raft::AppendEntriesResponse& response, bool failed);

    //   void HandleInstallSnapshotResponse(int64_t peer_id, const
    //   raft::InstallSnapshotRequest& request,
    //                                      const raft::InstallSnapshotResponse&
    //                                      response, bool failed);

    const int64_t _node_id;
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
    std::vector<Peer> _peers;
    std::map<int64_t, PeerConnection> _peer_connections;

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
    // int64_t _snapshot_last_index{0};
    // int64_t _snapshot_last_term{0};
};

}    // namespace raft