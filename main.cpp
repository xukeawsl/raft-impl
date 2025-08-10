#include <iostream>

#include "node.h"

DEFINE_int64(node_id, 0, "Raft node ID");
DEFINE_string(peers, "", "Comma-separated peer addresses in format ip:port:id");

std::vector<raft::Peer> parse_peers(const std::string& peers_str) {
    std::vector<raft::Peer> peers;
    std::istringstream iss(peers_str);
    std::string token;

    while (std::getline(iss, token, ',')) {
        if (!token.empty()) {
            raft::Peer peer(token);
            if (!peer.is_empty()) {
                peers.push_back(peer);
            }
        }
    }

    return peers;
}

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (!gflags::ReadFromFlagsFile("../gflags.conf", argv[0], true)) {
        return EXIT_FAILURE;
    }

    if (!raft::util::init_spdlog(FLAGS_node_id)) {
        return EXIT_FAILURE;
    }

    std::vector<raft::Peer> peers = parse_peers(FLAGS_peers);

    bool found = false;
    for (const auto& peer : peers) {
        if (peer.id == FLAGS_node_id) {
            found = true;
            break;
        }
    }

    if (!found) {
        SPDLOG_ERROR("Current node ID {} not found in peer list",
                     FLAGS_node_id);
        return EXIT_FAILURE;
    }

    raft::Node node(FLAGS_node_id, peers);
    node.Start();

    std::string cmd;

    while (true) {
        std::cout << "> ";
        std::getline(std::cin, cmd);

        if (cmd == "exit" || cmd == "quit") {
            break;
        }

        if (!node.SubmitCommand(cmd)) {
            SPDLOG_WARN("Not leader, command not submitted");
        } else {
            SPDLOG_INFO("Command submitted: {}", cmd);
        }
    }

    node.Stop();

    raft::util::flush_spdlog();

    return EXIT_SUCCESS;
}