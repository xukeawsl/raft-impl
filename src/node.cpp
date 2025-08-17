#include "node.h"

#include <algorithm>
#include <cstdint>
#include <fstream>

#include "brpc/controller.h"
#include "butil/file_util.h"
#include "butil/files/file.h"
#include "butil/time.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

DEFINE_int64(election_timeout_min, 150,
             "Minimum election timeout in milliseconds");
DEFINE_int64(election_timeout_max, 300,
             "Maximum election timeout in milliseconds");
DEFINE_int64(heartbeat_interval_ms, 50, "Heartbeat interval in milliseconds");
DEFINE_int64(snapshot_check_interval_ms, 5000,
             "Snapshot check interval in milliseconds");
DEFINE_int64(snapshot_interval, 1000, "Log entries between snapshots");

namespace raft {

Node::Node()
    : _fsm(nullptr),
      _node_owns_fsm(false),
      _state(State::FOLLOWER),
      _current_term(0),
      _voted_for(-1),
      _commit_index(0),
      _last_applied(0),
      _last_heartbeat_time(std::chrono::steady_clock::now()),
      _rng(std::random_device{}()),
      _last_snapshot_time(std::chrono::steady_clock::now()) {}

Node::~Node() {}

bool Node::Init(const NodeOptions& options) {
    std::vector<raft::Peer> peers;
    std::istringstream iss(options.peers);
    std::string token;

    while (std::getline(iss, token, ',')) {
        if (!token.empty()) {
            raft::Peer peer(token);
            if (!peer.is_empty()) {
                peers.push_back(peer);
            }
        }
    }

    bool found = false;
    for (const auto& peer : peers) {
        if (peer.id == options.node_id) {
            found = true;
            break;
        }
    }

    if (!found) {
        SPDLOG_ERROR("Current node ID {} not found in peer list",
                     options.node_id);
        return false;
    }

    _fsm = options.fsm;
    _node_owns_fsm = options.node_owns_fsm;
    _node_id = options.node_id;

    for (const auto& peer : peers) {
        if (peer.id == _node_id) {
            _self_address = peer.address();
            continue;
        }
        _peers[peer.id] = peer;
        _snapshoting_peers[peer.id] = false;
    }

    _peers_total_count = _peers.size() + 1;

    if (_self_address.empty()) {
        SPDLOG_ERROR("Current node ID {} not found in peers", _node_id);
        return false;
    }

    std::string db_path = "./raft_meta/node_" + std::to_string(_node_id);
    if (!butil::CreateDirectory(butil::FilePath(db_path))) {
        SPDLOG_ERROR("Failed to create database directory:{}", db_path);
        return false;
    }

    _store = std::make_unique<Store>(db_path);
    if (!_store->Open()) {
        SPDLOG_ERROR("Failed to open RocksDB storage");
        return false;
    }

    LoadPersistentState();

    for (auto& [peer_id, peer] : _peers) {
        SPDLOG_INFO("Added peer: {}", _peers[peer.id].to_string());
        if (!peer.init_channel()) {
            SPDLOG_ERROR("Failed to initialize channel for peer:{}",
                         peer.address());
            return false;
        }
    }

    ResetElectionTimer();

    return true;
}

void Node::Start() {
    if (_server.AddService(this, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        SPDLOG_ERROR("Failed to add RaftService to brpc server");
        return;
    }

    brpc::ServerOptions options;
    if (_server.Start(_self_address.c_str(), &options) != 0) {
        SPDLOG_ERROR("Failed to start brpc server on {}", _self_address);
        return;
    }

    SPDLOG_INFO("Node {} started on {}", _node_id, _self_address);
    _running = true;
    _run_thread = std::thread(&Node::RunLoop, this);
}

void Node::Stop() {
    _running = false;
    if (_run_thread.joinable()) {
        _run_thread.join();
    }

    BAIDU_SCOPED_LOCK(_mutex);
    SPDLOG_INFO("tasks map size: {}", _tasks.size());
    for (auto& [index, task] : _tasks) {
        if (task.done) {
            SPDLOG_INFO("done Run {}", index);
            task.done->Run();
        }
    }
    _tasks.clear();

    _server.Stop(0);
    _server.Join();
}

void Node::RunLoop() {
    while (_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        BAIDU_SCOPED_LOCK(_mutex);

        // 每个服务器都独立的创建快照
        MakeSnapshot();

        switch (_state) {
            case State::LEADER: {
                // 向每个服务器发送初始的空AppendEntries
                // RPC（心跳）；在空闲期间重复，以防止选举超时
                if (IsHeartbeatTimeout()) {
                    BroadcastHeartbeat();
                }
            } break;
            case State::CANDIDATE: {
                if (IsElectionTimeout()) {
                    SPDLOG_TRACE(
                        "Node {} election timeout in candidate state, "
                        "restarting election",
                        _node_id);
                    StartElection();
                }
            } break;
            case State::FOLLOWER: {
                // 如果选举超时，没有收到现任Leader的AppendEntries
                // RPC，也没有给Candidate投票：转换为Candidate
                if (IsElectionTimeout()) {
                    SPDLOG_TRACE("Node {} election timeout, starting election",
                                 _node_id);
                    StartElection();
                }
            } break;
        }

        ApplyCommittedEntries();
    }
}

// 检查心跳发送时间间隔
bool Node::IsHeartbeatTimeout() const {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       now - _last_heartbeat_time)
                       .count();
    return elapsed > FLAGS_heartbeat_interval_ms;
}

// 重置选举超时时间
void Node::ResetElectionTimer() {
    std::uniform_int_distribution<int64_t> dist(FLAGS_election_timeout_min,
                                                FLAGS_election_timeout_max);
    _election_timeout_ms = dist(_rng);
    _last_reset_time = std::chrono::steady_clock::now();
}

// 检查选举时间是否超时
bool Node::IsElectionTimeout() const {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       now - _last_reset_time)
                       .count();
    return elapsed > _election_timeout_ms;
}

void Node::MakeSnapshot() {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       now - _last_snapshot_time)
                       .count();
    if (elapsed > FLAGS_snapshot_check_interval_ms) {
        if (_last_applied - _snapshot_last_index > FLAGS_snapshot_interval) {
            CreateSnapshot(_last_applied);
        }
        _last_snapshot_time = now;
    }
}

// 启动选举流程
void Node::StartElection() {
    _state = State::CANDIDATE;    // 转换为候选人状态
    ++_current_term;              // 递增 currentTerm
    _voted_for = _node_id;        // 给自己投票
    _votes_received = 1;          // 接收到的总票数

    SaveCurrentTerm();
    SaveVotedFor();

    ResetElectionTimer();    // 重置选举定时器

    // 如果集群就一个节点, 直接成为主即可
    if (_votes_received > static_cast<int64_t>(_peers_total_count / 2)) {
        BecomeLeader();
        return;
    }

    int64_t last_stored_index = GetLastLogIndex();
    int64_t last_stored_term = GetLastLogTerm();

    raft::RequestVoteRequest request;
    request.set_term(_current_term);
    request.set_candidate_id(_node_id);
    request.set_last_log_index(last_stored_index);
    request.set_last_log_term(last_stored_term);

    // 向所有其他服务器发送异步 RequestVote RPCs
    for (auto& [peer_id, _] : _peers) {
        AsyncRequestVote(
            peer_id, request,
            [this, request](int64_t peer_id,
                            const raft::RequestVoteResponse& response,
                            bool failed) {
                this->HandleRequestVoteResponse(peer_id, request, response,
                                                failed, _current_term);
            });
    }
}

// 如果RPC请求或响应包含任期 T > currentTerm
// 设置currentTerm = T，转换为follower
void Node::StepDown(int64_t term) {
    _current_term = term;
    _state = State::FOLLOWER;
    _voted_for = -1;

    SaveCurrentTerm();
    SaveVotedFor();

    ResetElectionTimer();
    return;
}

// 成为Leader，更新状态并初始化日志复制
void Node::BecomeLeader() {
    SPDLOG_INFO("Node {} became leader for term {}", _node_id, _current_term);
    _state = State::LEADER;

    // 默认要同步的下一个条目的索引为最后一条目录索引+1
    // 已知的已经匹配的索引应该为0
    for (const auto& [peer_id, _] : _peers) {
        _next_index[peer_id] = GetLastLogIndex() + 1;
        _match_index[peer_id] = 0;
    }

    LeaderSubmitNoOpCommand();
}

// 成为 Leader 之后要立即发一条带当前任期的日志条目
// 这样才可以把 commit_index 更新到最新的值
// 但是 last_applied 只能从 0 开始重新执行状态机了
// 所以需要保证状态机执行时幂等的，有两种方案
// 1. raft 协议实现保证幂等，通过添加请求 ID 来实现，执行过的直接跳过
// 2. 状态机内部实现幂等性，也可以采用类似的方法
// 最后为了加快恢复速度，可以引入快照来压缩日志（这样再初始化时 last_applied
// 可以更新到快照索引
void Node::LeaderSubmitNoOpCommand() {
    LogEntry entry;
    entry.set_term(_current_term);

    int64_t new_index = GetLastLogIndex() + 1;
    if (!_store->SaveLogEntry(new_index, entry)) {
        SPDLOG_ERROR("Failed to save log entry");
        return;
    }

    for (auto& [peer_id, _] : _peers) {
        if (_next_index[peer_id] == new_index) {
            ++_next_index[peer_id];
        }
        ReplicateLog(peer_id);
    }
}

// 发送心跳请求，也用于成为Leader后发起日志复制
void Node::BroadcastHeartbeat() {
    for (auto& [peer_id, _] : _peers) {
        if (_next_index[peer_id] <= _snapshot_last_index &&
            _snapshoting_peers[peer_id] == false) {
            SendSnapshot(peer_id);
        } else {
            ReplicateLog(peer_id);
        }
    }

    _last_heartbeat_time = std::chrono::steady_clock::now();
}

// 发起日志复制
void Node::ReplicateLog(int64_t peer_id) {
    if (_state != State::LEADER) {
        return;
    }

    raft::AppendEntriesRequest request;
    request.set_term(_current_term);
    request.set_leader_id(_node_id);

    int64_t next_idx = _next_index[peer_id];
    int64_t prev_log_index = next_idx - 1;
    int64_t prev_log_term = 0;    // 历史如果不存在日志, 则对应任期为0
    int64_t last_stored_index = GetLastLogIndex();

    if (prev_log_index > 0) {
        if (prev_log_index <= last_stored_index) {
            prev_log_term = GetLogTerm(prev_log_index);
        } else {
            _next_index[peer_id] = last_stored_index + 1;
            next_idx = _next_index[peer_id];
            prev_log_index = next_idx - 1;

            if (prev_log_index > 0 && prev_log_index <= _snapshot_last_index) {
                prev_log_term = _snapshot_last_term;
            }
        }
    }

    SPDLOG_TRACE(
        "Node {} replicating log to peer {}: next_idx={}, prev_log_index={}, "
        "prev_log_term={}, last_stored_index={}",
        _node_id, peer_id, next_idx, prev_log_index, prev_log_term,
        last_stored_index);

    request.set_prev_log_index(prev_log_index);
    request.set_prev_log_term(prev_log_term);

    if (next_idx <= last_stored_index) {
        for (int64_t i = next_idx; i <= last_stored_index; ++i) {
            if (i <= _snapshot_last_index) continue;
            auto entry = _store->LoadLogEntry(i);
            if (!entry.has_term()) {
                SPDLOG_ERROR("no log entry in {}", i);
            }
            request.add_entries()->CopyFrom(entry);
        }
    }

    request.set_leader_commit(_commit_index);

    AsyncAppendEntries(
        peer_id, request,
        [this, request](int64_t peer_id,
                        const raft::AppendEntriesResponse& response,
                        bool failed) {
            this->HandleAppendEntriesResponse(peer_id, request, response,
                                              failed);
        });
}

// 应用日志到状态机
void Node::ApplyCommittedEntries() {
    // 如果commitIndex>lastApplied：增加lastApplied，将log[lastApplied]应用于状态机
    while (_last_applied < _commit_index) {
        ++_last_applied;

        if (_last_applied <= _snapshot_last_index) {
            continue;
        }

        auto it = _tasks.find(_last_applied);
        if (it != _tasks.end()) {
            _fsm->on_apply(it->second);
            _tasks.erase(it);
        }
    }
}

// 检查请求方的日志是否更新
bool Node::IsLogUpToDate(int64_t last_log_term, int64_t last_log_index) {
    int64_t last_stored_index = GetLastLogIndex();
    if (last_stored_index == 0) {
        SPDLOG_INFO("Node {} has no logs stored", _node_id);
        return true;
    }

    int64_t last_stored_term = GetLastLogTerm();

    SPDLOG_INFO("Node {} has last log entry: term={}, index={}", _node_id,
                last_stored_term, last_stored_index);

    return (last_log_term > last_stored_term) ||
           (last_log_term == last_stored_term &&
            last_log_index >= last_stored_index);
}

// 持久化当前节点的任期
void Node::SaveCurrentTerm() {
    if (!_store->SaveCurrentTerm(_current_term)) {
        SPDLOG_ERROR("Failed to save current term");
    }
}

// 持久化投票信息
void Node::SaveVotedFor() {
    if (!_store->SaveVotedFor(_voted_for)) {
        SPDLOG_ERROR("Failed to save voted for");
    }
}

int64_t Node::GetLastLogIndex() {
    int64_t last_stored_index = _store->GetLastLogIndex();
    if (last_stored_index <= _snapshot_last_index) {
        return _snapshot_last_index;
    }
    return last_stored_index;
}

int64_t Node::GetLastLogTerm() { return GetLogTerm(GetLastLogIndex()); }

int64_t Node::GetLogTerm(int64_t index) {
    if (index <= _snapshot_last_index) {
        return _snapshot_last_term;
    }
    return _store->LoadLogEntry(index).term();
}

void Node::DeleteLogEntriesBefore(int64_t index) {
    if (!_store->DeleteLogEntriesBefore(index)) {
        SPDLOG_ERROR("Failed to delete log entries before index {}", index);
    }
}

// 删除从指定索引开始的日志条目
void Node::DeleteLogEntriesFrom(int64_t from_index) {
    if (!_store->DeleteLogEntriesFrom(from_index)) {
        SPDLOG_ERROR("Failed to delete log entries from index {}", from_index);
    }
}

// 加载持久化状态到内存
void Node::LoadPersistentState() {
    _current_term = _store->LoadCurrentTerm();
    _voted_for = _store->LoadVotedFor();

    auto meta = _store->LoadSnapshotMetaData();
    _snapshot_last_index = meta.last_included_index();
    _snapshot_last_term = meta.last_included_term();

    // 因为快照的是已经应用到状态机的，所以可以用来初始化 last_applied 和
    // commit_index
    _last_applied = _commit_index = _snapshot_last_index;

    SPDLOG_INFO(
        "Loaded persistent state: term={}, voted_for={}, "
        "snapshot_last_index={}, snapshot_last_term={}",
        _current_term, _voted_for, _snapshot_last_index, _snapshot_last_term);
}

// 创建快照
void Node::CreateSnapshot(int64_t last_included_index) {
    // 检查索引是否有效（必须在快照之后且不超过当前日志最大索引）
    int64_t last_log_index = GetLastLogIndex();
    if (last_included_index <= _snapshot_last_index ||
        last_included_index > last_log_index) {
        SPDLOG_WARN(
            "Invalid snapshot index: {}, current snapshot index: {}, last log "
            "index: {}",
            last_included_index, _snapshot_last_index, last_log_index);
        return;
    }

    auto last_entry = _store->LoadLogEntry(last_included_index);
    if (last_entry.term() == 0) {
        SPDLOG_ERROR("Log entry at index {} not found", last_included_index);
        return;
    }

    raft::SnapshotMetaData meta;
    meta.set_last_included_index(last_included_index);
    meta.set_last_included_term(last_entry.term());

    std::string snapshot_data;
    _fsm->on_snapshot_save(snapshot_data);
    meta.set_data(snapshot_data);

    if (!_store->SaveSnapshotMetaData(meta)) {
        SPDLOG_ERROR("Failed to save snapshot metadata");
        return;
    }

    if (last_included_index > _snapshot_last_index) {
        _snapshot_last_index = last_included_index;
        _snapshot_last_term = last_entry.term();
        DeleteLogEntriesBefore(last_included_index);
    }

    SPDLOG_INFO(
        "Snapshot created: last_included_index={}, last_included_term={}",
        last_included_index, last_entry.term());
}

void Node::SendSnapshot(int64_t peer_id) {
    auto meta = _store->LoadSnapshotMetaData();

    // 单分块快照安装请求
    raft::InstallSnapshotRequest request;
    request.set_term(_current_term);
    request.set_leader_id(_node_id);
    request.mutable_meta()->CopyFrom(meta);
    request.set_offset(0);
    request.set_done(true);

    _snapshoting_peers[peer_id] = true;

    AsyncInstallSnapshot(
        peer_id, request,
        [this, request](int64_t peer_id,
                        const raft::InstallSnapshotResponse& response,
                        bool failed) {
            this->HandleInstallSnapshotResponse(peer_id, request, response,
                                                failed);
        });
}

// 异步发起投票请求
void Node::AsyncRequestVote(int64_t peer_id,
                            const raft::RequestVoteRequest& request,
                            RequestVoteCallback callback) {
    auto* cntl = new brpc::Controller();
    auto* response = new raft::RequestVoteResponse();

    struct RequestVoteCallbackWrapper {
        Node* node;
        brpc::Controller* cntl;
        raft::RequestVoteResponse* response;
        int64_t peer_id;
        RequestVoteCallback callback;

        static void Invoke(RequestVoteCallbackWrapper* arg) {
            std::unique_ptr<RequestVoteCallbackWrapper> wrapper(arg);
            wrapper->callback(wrapper->peer_id, *wrapper->response,
                              wrapper->cntl->Failed());
            delete wrapper->cntl;
            delete wrapper->response;
        }
    };

    auto* wrapper =
        new RequestVoteCallbackWrapper{this, cntl, response, peer_id, callback};
    google::protobuf::Closure* done =
        brpc::NewCallback(&RequestVoteCallbackWrapper::Invoke, wrapper);

    RaftService_Stub stub(_peers[peer_id].channel.get());
    stub.RequestVote(cntl, &request, response, done);
}

// 异步发起日志复制请求
void Node::AsyncAppendEntries(int64_t peer_id,
                              const raft::AppendEntriesRequest& request,
                              AppendEntriesCallback callback) {
    auto* cntl = new brpc::Controller();
    auto* response = new raft::AppendEntriesResponse();

    struct AppendEntriesCallbackWrapper {
        Node* node;
        brpc::Controller* cntl;
        raft::AppendEntriesResponse* response;
        int64_t peer_id;
        AppendEntriesCallback callback;

        static void Invoke(AppendEntriesCallbackWrapper* arg) {
            std::unique_ptr<AppendEntriesCallbackWrapper> wrapper(arg);
            wrapper->callback(wrapper->peer_id, *wrapper->response,
                              wrapper->cntl->Failed());
            delete wrapper->cntl;
            delete wrapper->response;
        }
    };

    auto* wrapper = new AppendEntriesCallbackWrapper{this, cntl, response,
                                                     peer_id, callback};
    google::protobuf::Closure* done =
        brpc::NewCallback(&AppendEntriesCallbackWrapper::Invoke, wrapper);

    RaftService_Stub stub(_peers[peer_id].channel.get());
    stub.AppendEntries(cntl, &request, response, done);
}

// 异步发起快照安装请求
void Node::AsyncInstallSnapshot(int64_t peer_id,
                                const raft::InstallSnapshotRequest& request,
                                InstallSnapshotCallback callback) {
    auto* cntl = new brpc::Controller();
    auto* response = new raft::InstallSnapshotResponse();

    struct InstallSnapshotCallbackWrapper {
        Node* node;
        brpc::Controller* cntl;
        raft::InstallSnapshotResponse* response;
        int64_t peer_id;
        InstallSnapshotCallback callback;

        static void Invoke(InstallSnapshotCallbackWrapper* arg) {
            std::unique_ptr<InstallSnapshotCallbackWrapper> wrapper(arg);
            wrapper->callback(wrapper->peer_id, *wrapper->response,
                              wrapper->cntl->Failed());
            delete wrapper->cntl;
            delete wrapper->response;
        }
    };

    auto* wrapper = new InstallSnapshotCallbackWrapper{this, cntl, response,
                                                       peer_id, callback};
    google::protobuf::Closure* done =
        brpc::NewCallback(&InstallSnapshotCallbackWrapper::Invoke, wrapper);

    RaftService_Stub stub(_peers[peer_id].channel.get());
    stub.InstallSnapshot(cntl, &request, response, done);
}

// 请求投票异步回调处理
void Node::HandleRequestVoteResponse(int64_t peer_id,
                                     const raft::RequestVoteRequest& request,
                                     const raft::RequestVoteResponse& response,
                                     bool failed, int64_t start_term) {
    BAIDU_SCOPED_LOCK(_mutex);

    if (_state != State::CANDIDATE || start_term != _current_term) {
        return;
    }

    if (failed) {
        SPDLOG_TRACE("RequestVote to peer {} failed", peer_id);
        return;
    }

    if (response.term() > _current_term) {
        StepDown(request.term());
        return;
    }

    if (response.vote_granted()) {
        ++_votes_received;

        // 如果获得大多数服务器的投票：成为领导者
        if (_votes_received > static_cast<int64_t>(_peers_total_count / 2)) {
            BecomeLeader();
        }
    }
}

// 日志复制异步回调处理
void Node::HandleAppendEntriesResponse(
    int64_t peer_id, const raft::AppendEntriesRequest& request,
    const raft::AppendEntriesResponse& response, bool failed) {
    BAIDU_SCOPED_LOCK(_mutex);

    if (_state != State::LEADER) {
        return;
    }

    if (failed) {
        SPDLOG_TRACE("AppendEntries to peer {} failed", peer_id);
        return;
    }

    if (response.term() > _current_term) {
        StepDown(request.term());
        return;
    }

    SPDLOG_TRACE("AppendEntries response from peer {}: success={}, term={}",
                 peer_id, response.success(), response.term());

    if (response.success()) {
        // 如果成功：为跟随者更新nextIndex和matchIndex
        // 下面取 max 是因为在异步回调的过程中可能会有新的日志被追加
        int64_t next_idx =
            request.prev_log_index() + request.entries_size() + 1;
        _next_index[peer_id] = std::max(_next_index[peer_id], next_idx);
        _match_index[peer_id] = std::max(_match_index[peer_id], next_idx - 1);
    } else {    // 如果AppendEntries因为日志不一致而失败：递减NextIndex并重试
        if (_next_index[peer_id] > 1) {
            SPDLOG_TRACE("AppendEntries retry");
            --_next_index[peer_id];
        }
    }

    // 如果存在一个N，使得N>commitIndex，大多数的matchIndex[i]≥N
    // 并且log[N].term == currentTerm：设置commitIndex = N
    int64_t last_stored_index = GetLastLogIndex();
    int64_t old_commit_index = _commit_index;
    for (int64_t i = _commit_index + 1; i <= last_stored_index; ++i) {
        auto entry = _store->LoadLogEntry(i);

        if (entry.term() != _current_term) continue;

        int64_t count = 1;
        for (auto& [_, match] : _match_index) {
            if (match >= i) ++count;
        }

        if (count > static_cast<int64_t>(_peers_total_count / 2)) {
            _commit_index = i;
        }
    }

    if (old_commit_index != _commit_index) {
        SPDLOG_TRACE("Node {} updated commitIndex: {} -> {}", _node_id,
                     old_commit_index, _commit_index);
    }
}

// 快照安装回调处理
void Node::HandleInstallSnapshotResponse(
    int64_t peer_id, const raft::InstallSnapshotRequest& request,
    const raft::InstallSnapshotResponse& response, bool failed) {
    BAIDU_SCOPED_LOCK(_mutex);

    _snapshoting_peers[peer_id] = false;

    if (_state != State::LEADER) {
        return;
    }

    if (failed) {
        SPDLOG_TRACE("InstallSnapshot to peer {} failed", peer_id);
        return;
    }

    if (response.term() > _current_term) {
        StepDown(response.term());
        return;
    }

    _next_index[peer_id] = std::max(_next_index[peer_id],
                                    request.meta().last_included_index() + 1);
    _match_index[peer_id] =
        std::max(_match_index[peer_id], request.meta().last_included_index());

    SPDLOG_INFO("update Node {} next_index:{}, match_index:{}", peer_id,
                _next_index[peer_id], _match_index[peer_id]);
}

// 请求投票接收处理
void Node::RequestVote(google::protobuf::RpcController* cntl_base,
                       const raft::RequestVoteRequest* request,
                       raft::RequestVoteResponse* response,
                       google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    BAIDU_SCOPED_LOCK(_mutex);

    response->set_term(_current_term);
    response->set_vote_granted(false);

    // 如果 term < currentTerm，则返回 false
    if (request->term() < _current_term) {
        return;
    }

    if (request->term() > _current_term) {
        StepDown(request->term());
    }

    SPDLOG_TRACE(
        "Node {} received RequestVote from {}: term={}, last_log_index={}, "
        "last_log_term={}",
        _node_id, request->candidate_id(), request->term(),
        request->last_log_index(), request->last_log_term());

    // 如果 votedFor 是 null 或
    // candidateId，并且候选人的日志至少与接收人的日志一样新，则投票
    if ((_voted_for == -1 || _voted_for == request->candidate_id()) &&
        IsLogUpToDate(request->last_log_term(), request->last_log_index())) {
        response->set_vote_granted(true);
        _voted_for = request->candidate_id();
        ResetElectionTimer();
        SaveVotedFor();
    }
}

// 日志复制接收处理
void Node::AppendEntries(google::protobuf::RpcController* cntl_base,
                         const raft::AppendEntriesRequest* request,
                         raft::AppendEntriesResponse* response,
                         google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    BAIDU_SCOPED_LOCK(_mutex);

    response->set_term(_current_term);
    response->set_success(false);

    // 如果 term < currentTerm，则返回 false
    if (request->term() < _current_term) {
        return;
    }

    ResetElectionTimer();

    if (request->term() > _current_term) {
        StepDown(request->term());
    }

    // 大于 0 说明是携带日志的
    if (request->prev_log_index() > 0) {
        if (GetLastLogIndex() < request->prev_log_index()) {
            return;
        }

        // 如果对应索引有日志，检查对应 term 是否匹配
        int64_t log_term = GetLogTerm(request->prev_log_index());
        if (log_term != request->prev_log_term()) {
            SPDLOG_WARN(
                "Log entry different in index {}, local term {}, remote term "
                "{}",
                request->prev_log_index(), log_term, request->prev_log_term());
            return;
        }
    }

    int64_t index = request->prev_log_index();
    for (const auto& new_entry : request->entries()) {
        ++index;

        // 如果一个现有的条目与一个新的条目相冲突（相同的索引但不同的任期）
        // 删除现有的条目和后面所有的条目
        if (index > _snapshot_last_index && index <= GetLastLogIndex()) {
            auto existing_entry = _store->LoadLogEntry(index);
            if (!existing_entry.has_term()) {
                SPDLOG_ERROR("Fail to load existing entry");
            } else if (existing_entry.term() != new_entry.term()) {
                DeleteLogEntriesFrom(index);
            }
        }

        // 添加日志中任何尚未出现的新条目
        if (index > GetLastLogIndex()) {
            if (!_store->SaveLogEntry(index, new_entry)) {
                LOG(ERROR) << "Failed to save log entry at index " << index;
                return;
            }

            if (new_entry.has_command()) {
                raft::Task task;
                task.data = new_entry.command();
                _tasks[index] = task;
            }
        }
    }

    // 如果leaderCommit > commitIndex，设置commitIndex = min(leaderCommit,
    // 最后一个新条目的索引)
    if (request->leader_commit() > _commit_index) {
        _commit_index = std::min(request->leader_commit(), GetLastLogIndex());

        ApplyCommittedEntries();
    }

    response->set_success(true);
}

void Node::InstallSnapshot(google::protobuf::RpcController* cntl_base,
                           const raft::InstallSnapshotRequest* request,
                           raft::InstallSnapshotResponse* response,
                           google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    BAIDU_SCOPED_LOCK(_mutex);

    response->set_term(_current_term);

    // 如果term < currentTerm就立即回复
    if (request->term() < _current_term) {
        return;
    }

    ResetElectionTimer();

    if (_snapshot_last_index == request->meta().last_included_index() &&
        _snapshot_last_term == request->meta().last_included_term()) {
        return;
    }

    SPDLOG_INFO("Start to install snapshot");

    if (request->offset() != 0 || request->done() != true) {
        cntl_base->SetFailed("Not support chunk snapshot install");
        return;
    }

    if (request->term() > _current_term) {
        StepDown(request->term());
    }

    // 保存快照文件，丢弃具有较小索引的任何现有或部分快照
    if (!_store->SaveSnapshotMetaData(request->meta())) {
        cntl_base->SetFailed("Failed to save snapshot meta data");
        return;
    }

    _snapshot_last_index = request->meta().last_included_index();
    _snapshot_last_term = request->meta().last_included_term();
    _commit_index = std::max(_commit_index, _snapshot_last_index);
    _last_applied = std::max(_last_applied, _snapshot_last_index);

    int64_t last_store_index = GetLastLogIndex();
    int64_t last_store_term = GetLastLogTerm();

    if (last_store_index == _snapshot_last_index &&
        last_store_term == _snapshot_last_term) {
        // 如果现存的日志条目与快照中最后包含的日志条目具有相同的索引值和任期号，则保留其后的日志条目
        DeleteLogEntriesBefore(last_store_index);
    } else {
        // 丢弃整个日志
        DeleteLogEntriesFrom(0);
    }

    // 使用快照重置状态机
    _fsm->on_snapshot_load(request->meta().data());
}

void Node::Apply(Task task) {
    BAIDU_SCOPED_LOCK(_mutex);

    if (_state != State::LEADER) {
        // 重定向到 Leader
        return;
    }

    LogEntry entry;
    entry.set_term(_current_term);
    entry.set_command(task.data);

    int64_t new_index = GetLastLogIndex() + 1;
    if (!_store->SaveLogEntry(new_index, entry)) {
        SPDLOG_ERROR("Failed to save log entry");
        return;
    }

    _tasks[new_index] = task;

    for (auto& [peer_id, _] : _peers) {
        if (_next_index[peer_id] == new_index) {
            ++_next_index[peer_id];
        }
        ReplicateLog(peer_id);
    }
}

}    // namespace raft