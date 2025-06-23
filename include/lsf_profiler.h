#ifndef LSF_PROFILER_H
#define LSF_PROFILER_H

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace profiler {

// 运行期开关，默认 false（关闭统计）
inline std::atomic<bool> enabled{false};

// 初始化：指定输出目录和写快照间隔（秒）
void init(const std::string& output_dir, int interval_s = 10);
// 关闭：停止后台线程并写最后一次快照
void shutdown();

// RAII 计时器——始终生效，析构时根据 enabled 决定是否累加
struct FuncProfiler {
    FuncProfiler(const char* func_name);
    ~FuncProfiler();
private:
    const char* _name;
    std::chrono::time_point<std::chrono::high_resolution_clock> _t0;
};

// 替换掉原来基于宏的开关：不管 enabled 是什么，
// 这行都会插入一个 FuncProfiler 对象，析构时再 decide。
#define PROFILE_FUNC() ::profiler::FuncProfiler __profiler_obj__(__func__)

} // namespace profiler

#endif // LSF_PROFILER_H