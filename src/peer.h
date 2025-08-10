#pragma once

#include <string>

#include "butil/endpoint.h"

namespace raft {

struct Peer {
    butil::EndPoint addr;
    int64_t id;

    Peer();

    explicit Peer(butil::EndPoint addr_);

    Peer(butil::EndPoint addr_, int64_t id_);

    Peer(const std::string& str);

    void reset();

    bool is_empty() const;

    int parse(const std::string& str);

    std::string address() const;

    std::string to_string() const;
};

}    // namespace raft