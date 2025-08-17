#include <iostream>

#include "node.h"
#include "service/counter_service.h"

DEFINE_int64(service_port, 8054, "Service Port");
DEFINE_int64(node_id, 0, "Raft node ID");
DEFINE_string(peers, "", "Comma-separated peer addresses in format ip:port:id");

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (!gflags::ReadFromFlagsFile("../gflags.conf", argv[0], true)) {
        return EXIT_FAILURE;
    }

    if (!raft::util::init_spdlog(FLAGS_node_id)) {
        return EXIT_FAILURE;
    }

    brpc::Server server;

    example::Counter counter;
    example::CounterServiceImpl service(&counter);

    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE)) {
        SPDLOG_ERROR("Fail to add service");
        return EXIT_FAILURE;
    }

    if (server.Start(FLAGS_service_port, nullptr)) {
        SPDLOG_ERROR("Fail to start server");
        return EXIT_FAILURE;
    }

    if (!counter.start(FLAGS_node_id, FLAGS_peers)) {
        return EXIT_FAILURE;
    }

    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }

    counter.stop();
    server.Stop(0);
    server.Join();

    SPDLOG_INFO("Stop raft service");

    raft::util::flush_spdlog();

    return EXIT_SUCCESS;
}