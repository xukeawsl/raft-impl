#pragma once

#include "counter.h"
#include "counter.pb.h"
#include "util.h"

namespace example {

class CounterServiceImpl : public CounterService {
public:
    explicit CounterServiceImpl(Counter* counter) : _counter(counter) {}

    void fetch_add(::google::protobuf::RpcController* controller,
                   const ::example::FetchAddRequest* request,
                   ::example::CounterResponse* response,
                   ::google::protobuf::Closure* done) {
        return _counter->fetch_add(request, response, done);
    }

    void get(::google::protobuf::RpcController* controller,
             const ::example::GetRequest* request,
             ::example::CounterResponse* response,
             ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        return _counter->get(response);
    }

private:
    Counter* _counter;
};

}    // namespace example