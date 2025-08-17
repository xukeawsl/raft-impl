#include <gflags/gflags.h>
#include <brpc/channel.h>
#include <butil/logging.h>
#include <thread>
#include <atomic>
#include <iostream>
#include "counter.pb.h"  // 由proto生成的头文件

DEFINE_string(server_addr, "127.0.0.1:8000", "服务器地址");
DEFINE_int32(total_requests, 1000, "请求总数");
DEFINE_int32(thread_num, 4, "并发线程数");

std::atomic<int> g_success_count(0);
std::atomic<int> g_fail_count(0);

// 线程工作函数
void send_requests(brpc::Channel* channel, int requests_per_thread) {
    example::CounterService_Stub stub(channel);
    
    for (int i = 0; i < requests_per_thread; ++i) {
        brpc::Controller cntl;
        example::FetchAddRequest request;
        example::CounterResponse response;
        
        request.set_value(1);  // 每次增加1
        
        stub.fetch_add(&cntl, &request, &response, nullptr);
        
        if (cntl.Failed()) {
            LOG(ERROR) << "RPC失败: " << cntl.ErrorText();
            g_fail_count++;
        } else {
            if (response.success()) {
                g_success_count++;
            } else {
                LOG(ERROR) << "操作失败: " << response.redirect();
                g_fail_count++;
            }
        }
    }
}

// 获取当前计数器值
int64_t get_current_value(brpc::Channel* channel) {
    example::CounterService_Stub stub(channel);
    brpc::Controller cntl;
    example::GetRequest request;
    example::CounterResponse response;
    
    stub.get(&cntl, &request, &response, nullptr);
    
    if (cntl.Failed()) {
        LOG(ERROR) << "获取当前值失败: " << cntl.ErrorText();
        return -1;
    }
    
    if (!response.success()) {
        LOG(ERROR) << "服务端返回失败: " << response.redirect();
        return -1;
    }
    
    if (!response.has_value()) {
        LOG(ERROR) << "响应中缺少value字段";
        return -1;
    }
    
    return response.value();
}

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    
    // 初始化BRPC Channel
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_BAIDU_STD;
    options.timeout_ms = 3000;
    options.max_retry = 3;
    
    if (channel.Init(FLAGS_server_addr.c_str(), &options) != 0) {
        LOG(ERROR) << "初始化Channel失败";
        return -1;
    }
    
    // 获取初始值
    int64_t initial_value = get_current_value(&channel);
    if (initial_value >= 0) {
        std::cout << "初始计数器值: " << initial_value << std::endl;
    }
    
    // 计算每个线程的请求数
    const int requests_per_thread = FLAGS_total_requests / FLAGS_thread_num;
    const int remaining_requests = FLAGS_total_requests % FLAGS_thread_num;
    
    // 创建线程池
    std::vector<std::thread> threads;
    threads.reserve(FLAGS_thread_num);
    
    auto start_time = butil::gettimeofday_us();
    
    // 启动工作线程
    for (int i = 0; i < FLAGS_thread_num; ++i) {
        int requests = requests_per_thread + (i < remaining_requests ? 1 : 0);
        threads.emplace_back(send_requests, &channel, requests);
    }
    
    // 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }
    
    auto end_time = butil::gettimeofday_us();
    double total_time_sec = (end_time - start_time) / 1000000.0;
    
    // 获取最终值
    int64_t final_value = get_current_value(&channel);
    
    // 打印统计信息
    std::cout << "\n===== 请求统计 =====\n"
              << "总请求数: " << FLAGS_total_requests << "\n"
              << "成功请求: " << g_success_count << "\n"
              << "失败请求: " << g_fail_count << "\n"
              << "最终计数器值: " << final_value << "\n";
    
    if (initial_value >= 0 && final_value >= 0) {
        int64_t expected_value = initial_value + g_success_count;
        std::cout << "预期计数器值: " << expected_value << "\n";
        if (final_value == expected_value) {
            std::cout << "✅ 计数器校验成功 (实际值 = 预期值)\n";
        } else {
            std::cout << "❌ 计数器校验失败 (差值 = " 
                      << (final_value - expected_value) << ")\n";
        }
    }
    
    std::cout << "总耗时: " << total_time_sec << " 秒\n"
              << "QPS: " << FLAGS_total_requests / total_time_sec << std::endl;
    
    return 0;
}