#include "fuse_stats.h"
#include <fstream>
#include <algorithm>
#include <sys/stat.h>

FuseStats& FuseStats::getInstance() {
    static FuseStats instance;
    return instance;
}
FuseStats::FuseStats() {
    mkdir("/tmp/fuse_stats", 0777);
}

FuseStats::~FuseStats() {
    stopContinuousOutput();
    saveToFile();
}

void FuseStats::startContinuousOutput(int interval_sec) {
    if (running) return;
    
    saveInterval = interval_sec;
    running = true;
    workerThread = std::thread([this] { saveLoop(); });
}

void FuseStats::stopContinuousOutput() {
    if (!running) return;
    
    running = false;
    if (workerThread.joinable()) {
        workerThread.join();
    }
}

void FuseStats::saveLoop() {
    while (running) {
        std::this_thread::sleep_for(std::chrono::seconds(saveInterval));
        saveToFile();
    }
}

bool FuseStats::isTemporary(const std::string& filename) const {
    static const std::vector<std::regex> patterns{
        std::regex(R"(^\..+\.sw[pxo]$)"),  // Vim交换文件
        std::regex(R"(^\.\#.+)"),          // Emacs自动保存
        std::regex(R"(~$)"),               // 备份文件
        std::regex(R"(\.tmp$)"),           // 通用临时文件
        std::regex(R"(\.bak$)"),           // 备份文件
        std::regex(R"(^4913$)"),           // Vim异常文件
        std::regex(R"(^Untitled)")         // 未命名文档
    };

    return std::any_of(patterns.begin(), patterns.end(),
        [&filename](const auto& pat){
            return std::regex_match(filename, pat);
        });
}

std::string FuseStats::formatSize(uintmax_t bytes) const {
    constexpr const char* units[] = {"B", "KB", "MB", "GB"};
    double size = bytes;
    int unit = 0;

    while (size >= 1024 && unit < 3) {
        size /= 1024;
        ++unit;
    }

    std::stringstream ss;
    ss << std::fixed << std::setprecision(2) << size << " " << units[unit];
    return ss.str();
}

void FuseStats::recordFile(const std::string& path, size_t size) {
    std::lock_guard<std::mutex> lock(statsMutex);
    
    // 过滤临时文件
    std::string filename = path.substr(path.find_last_of('/') + 1);
    
    if (isTemporary(filename)) return;

    files[path] = size;
   
    size_t pos = path.find_last_of('/');
    std::string name = (pos != std::string::npos) ? path.substr(pos+1) : path;
    
    if (++nameCount[name] > 1) {
        // 记录重复但不需要在此处理
    }
    
}

void FuseStats::recordDir(const std::string& path) {
    std::lock_guard<std::mutex> lock(statsMutex);
    dirs.push_back(path);
}

void FuseStats::saveToFile() {
    std::lock_guard<std::mutex> lock(statsMutex);

    std::ofstream report("/tmp/fuse_stats/latest.log");

    // 写入实时统计
    report << "=== Real-time FUSE Statistics ===\n";
    report << "Timestamp: " << time(nullptr) << "\n\n";

    // 当前活动文件
    report << "Active Files (" << files.size() << "):\n";
    for (const auto& [path, size] : files) {
        report << path << " \t" << size << " bytes\n";
    }

    // 目录信息
    report << "\nDirectories (" << dirs.size() << "):\n";
    for (const auto& dir : dirs) {
        report << dir << "\n";
    }

    // 新增文件总大小计算
    uintmax_t total_size = 0;
    for (const auto& [_, size] : files) {
        total_size += size;
    }

    report << "=== Storage Summary ===\n";
    report << "Total Files: " << files.size() << "\n";
    report << "Total Size:  " << formatSize(total_size) << "\n\n";

    // 重复文件统计

    /*
    report << "\nDuplicate Analysis:\n";
    std::unordered_map<std::string, int> duplicates;
    for (const auto& [name, count] : nameCount) {
        if (count > 1) duplicates[name] = count;
    }
    
    report << "Total Duplicates: " << duplicates.size() << "\n";
    for (const auto& [name, count] : duplicates) {
        report << name << ": " << count << " instances\n";
    }*/
}
