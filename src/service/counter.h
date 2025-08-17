#pragma once

#include "counter.pb.h"
#include "node.h"
#include "util.h"

namespace example {

class Counter : public raft::StateMachine {
public:
    Counter() : _value(0) {}

    bool start(int64_t node_id, const std::string& peers) {
        _node = std::make_unique<raft::Node>();

        raft::NodeOptions options;
        options.fsm = this;
        options.node_owns_fsm = false;
        options.node_id = node_id;
        options.peers = peers;

        if (!_node->Init(options)) {
            return false;
        }

        _node->Start();

        return true;
    }

    void stop() {
        if (_node) {
            _node->Stop();
        }
    }

    void fetch_add(const FetchAddRequest* request, CounterResponse* response,
                   google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        std::string data;
        if (!request->SerializePartialToString(&data)) {
            SPDLOG_ERROR("Fail to serialize request");
            response->set_success(false);
            return;
        }

        raft::Task task;
        task.data = std::move(data);
        task.request = request;
        task.response = response;
        task.done = done_guard.release();

        return _node->Apply(std::move(task));
    }

    void get(CounterResponse* response) {
        response->set_success(true);
        response->set_value(_value.load());
    }

    void on_apply(const raft::Task& task) override {
        brpc::ClosureGuard done_guard(task.done);

        int64_t prev = 0;

        // 存在 done 的说明是处理自身收到的请求，否则就是同步过来的请求
        if (task.done) {
            auto request =
                static_cast<const example::FetchAddRequest*>(task.request);
            auto response =
                static_cast<example::CounterResponse*>(task.response);
            prev = _value.fetch_add(request->value());
            response->set_success(true);
            response->set_value(prev);
        } else {
            example::FetchAddRequest request;
            CHECK(request.ParseFromString(task.data));
            prev = _value.fetch_add(request.value());
        }

        SPDLOG_INFO("Counter on_apply prev:{}, curr:{}", prev, _value.load());
    }

    void on_snapshot_save(std::string& data) override {
        // 对于 Counter 状态机来说，就保存 value 就行了
        data = std::to_string(_value);
        SPDLOG_INFO("on_snapshot_save value:{}", _value.load());
    }

    void on_snapshot_load(const std::string& data) override {
        _value = stoll(data);
        SPDLOG_INFO("on_snapshot_load value:{}", _value.load());
    }

private:
    std::unique_ptr<raft::Node> _node;
    std::atomic<int64_t> _value;
};

}    // namespace example