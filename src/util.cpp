#include "util.h"

DEFINE_string(log_file, "raft_impl", "Log file path");
DEFINE_uint64(log_rotate_size, 1048576 * 5, "Log rotation size in bytes");
DEFINE_uint64(log_rotate_count, 3, "Log rotation count");
DEFINE_uint64(log_thread_pool_q_size, 8192, "Log thread pool queue size");
DEFINE_uint64(log_thread_pool_t_num, std::thread::hardware_concurrency(),
              "Log thread pool thread num");

namespace raft {

namespace util {

// 初始化全局 spdlog 配置
bool init_spdlog(int64_t node_id) {
    try {
        std::string log_file =
            FLAGS_log_file + "_" + std::to_string(node_id) + ".log";

        spdlog::init_thread_pool(FLAGS_log_thread_pool_q_size,
                                 FLAGS_log_thread_pool_t_num);
        auto console_sink =
            std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            log_file, FLAGS_log_rotate_size, FLAGS_log_rotate_count);
#ifndef NDEBUG
        spdlog::set_default_logger(std::make_shared<spdlog::async_logger>(
            "debug_logger", spdlog::sinks_init_list({console_sink, file_sink}),
            spdlog::thread_pool(),
            spdlog::async_overflow_policy::overrun_oldest));
#else    // Release
        spdlog::set_default_logger(std::make_shared<spdlog::async_logger>(
            "release_logger", file_sink, spdlog::thread_pool(),
            spdlog::async_overflow_policy::block));
#endif

        switch (SPDLOG_ACTIVE_LEVEL) {
            case SPDLOG_LEVEL_TRACE:
                spdlog::set_level(spdlog::level::trace);
                break;
            case SPDLOG_LEVEL_DEBUG:
                spdlog::set_level(spdlog::level::debug);
                break;
            case SPDLOG_LEVEL_INFO:
                spdlog::set_level(spdlog::level::info);
                break;
            case SPDLOG_LEVEL_WARN:
                spdlog::set_level(spdlog::level::warn);
                break;
            case SPDLOG_LEVEL_ERROR:
                spdlog::set_level(spdlog::level::err);
                break;
            case SPDLOG_LEVEL_CRITICAL:
                spdlog::set_level(spdlog::level::critical);
                break;
            case SPDLOG_LEVEL_OFF:
                spdlog::set_level(spdlog::level::off);
                break;
            default:
                break;
        }

        spdlog::set_pattern("[%Y-%m-%d %T.%f] [%^%l%$] [%s:%#] %v");
    } catch (const std::exception &e) {
        spdlog::error("Failed to initialize logger: {}", e.what());
        return false;
    }

    SPDLOG_INFO("Logger initialized successfully");
    return true;
}

void flush_spdlog() { spdlog::shutdown(); }

}    // namespace util

}    // namespace raft