#pragma once

#include "gflags/gflags.h"
#include "spdlog/async.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

namespace raft {

namespace util {

// 初始化全局 spdlog 配置
bool init_spdlog(int64_t node_id);

// 停止 spdlog, 把日志刷到磁盘
void flush_spdlog();

}    // namespace util

}    // namespace raft