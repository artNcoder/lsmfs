#include "lsf_profiler.h"
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <cstring>    // for std::strerror
#include <cerrno>     // for errno
#include <chrono>
#include <thread>

namespace profiler {

struct FuncStats {
    std::atomic<uint64_t> calls{0};
    std::atomic<uint64_t> total_ns{0};
};

static std::mutex                                    __stats_mutex;
static std::unordered_map<std::string, FuncStats>    __func_stats;
static std::string                                   __output_dir;
static std::atomic<bool>                             __running{false};
static std::thread                                   __background_thread;

// 把 __func_stats 写入磁盘一次
static void write_snapshot() {
    std::lock_guard<std::mutex> lk(__stats_mutex);
    std::ofstream ofs(__output_dir + "/func_stats.txt", std::ofstream::trunc);
    if (!ofs) return;
    ofs << "=== Function Timing Stats ===\n";
    for (auto &kv : __func_stats) {
        auto calls    = kv.second.calls.load();
        double total_ms = kv.second.total_ns.load() / 1e6;
        ofs
          << kv.first
          << ": calls=" << calls
          << ", total=" << total_ms << " ms"
          << ", avg=" << (calls ? total_ms/calls : 0.0) << " ms\n";
    }
}

// 后台线程：每 interval_s 秒刷一次
static void thread_func(int interval_s) {
    while (__running.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(interval_s));
        if (!__running.load()) break;
        write_snapshot();
    }
}

void init(const std::string& output_dir, int interval_s) {
    __output_dir = output_dir;
    // mkdir -p
    if (mkdir(__output_dir.c_str(), 0755) && errno != EEXIST) {
        std::cerr
           << "profiler: mkdir `" << __output_dir 
           << "` failed: " << std::strerror(errno) << "\n";
    }
    write_snapshot();                // 写表头
    __running = true;
    __background_thread = std::thread(thread_func, interval_s);
}

void shutdown() {
    __running = false;
    if (__background_thread.joinable())
        __background_thread.join();
    write_snapshot();                // 写最后一次
}

// ------------------------------------------------------------------
// FuncProfiler
// ------------------------------------------------------------------

FuncProfiler::FuncProfiler(const char* func_name)
  : _name(func_name)
  , _t0(std::chrono::high_resolution_clock::now())
{}

FuncProfiler::~FuncProfiler() {
    if (!enabled.load()) return;
    auto dt = std::chrono::high_resolution_clock::now() - _t0;
    uint64_t ns = std::chrono::duration_cast<
                     std::chrono::nanoseconds>(dt).count();
    std::lock_guard<std::mutex> lk(__stats_mutex);
    auto &st = __func_stats[_name];
    st.calls++;
    st.total_ns += ns;
}

} // namespace profiler