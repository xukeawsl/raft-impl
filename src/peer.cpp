#include "peer.h"

namespace raft {

Peer::Peer() : id(0) {}

Peer::Peer(butil::EndPoint addr_) : addr(addr_), id(0) {}

Peer::Peer(butil::EndPoint addr_, int64_t id_) : addr(addr_), id(id_) {}

Peer::Peer(const std::string& str) {
    if (parse(str) != 0) {
        throw std::invalid_argument("invalid peer str: " + str);
    }
}

bool Peer::init_channel() {
    auto new_channel = std::make_shared<brpc::Channel>();

    brpc::ChannelOptions options;
    options.timeout_ms = 100;
    options.max_retry = 1;

    if (new_channel->Init(addr, &options) != 0) {
        return false;
    }

    channel.swap(new_channel);

    return true;
}

void Peer::reset() {
    addr.ip = butil::IP_ANY;
    addr.port = 0;
    id = 0;
}

bool Peer::is_empty() const {
    return (addr.ip == butil::IP_ANY && addr.port == 0 && id == 0);
}

int Peer::parse(const std::string& str) {
    reset();
    char ip_str[64];
    if (2 >
        sscanf(str.c_str(), "%[^:]%*[:]%d%*[:]%ld", ip_str, &addr.port, &id)) {
        reset();
        return -1;
    }
    if (0 != butil::str2ip(ip_str, &addr.ip)) {
        reset();
        return -1;
    }
    return 0;
}

std::string Peer::address() const { return butil::endpoint2str(addr).c_str(); }

std::string Peer::to_string() const {
    char str[128];
    snprintf(str, sizeof(str), "%s:%ld", butil::endpoint2str(addr).c_str(), id);
    return std::string(str);
}

}    // namespace raft