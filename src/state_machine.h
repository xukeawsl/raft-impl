#pragma once

#include "util.h"

namespace raft {

class StateMachine {
public:
    virtual ~StateMachine() {}

    virtual void on_apply(const Task& task) {}

    virtual void on_shutdown() {}

    virtual void on_snapshot_save(std::string& data) {}

    virtual void on_snapshot_load(const std::string& data) {}
};

}    // namespace raft