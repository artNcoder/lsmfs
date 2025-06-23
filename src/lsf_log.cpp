#include "lsf_log.h"
#include <ctime>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <cstdarg>
#include <vector>
#include <mutex>

namespace LSF
{
    Logger *Logger::instance = nullptr;
    std::mutex Logger::mutex_;

    Logger::Logger()
    {
        level_ = INFO;
        enabled_ = false;
    }

    Logger::~Logger()
    {
        if (log_file.is_open())
        {
            log_file.close();
        }
    }

    void Logger::openLogFile()
    {
        if (!log_file.is_open())
        {
            auto now = std::time(nullptr);
            auto *tm_info = std::localtime(&now);

            char datetime_buf[32];
            std::strftime(datetime_buf, sizeof(datetime_buf), "%Y%m%d_%H%M%S", tm_info);

             // std::string log_filename = "lsf_" + std::string(datetime_buf) + ".log";
            // log_file.open(log_filename, std::ios::app);

            std::filesystem::path log_dir = std::filesystem::current_path();
            std::error_code ec;
            std::filesystem::create_directories(log_dir, ec);
            if (ec) {
                std::cerr << "Cannot create log dir: " << ec.message() << "\n";
                // 失败时回退到当前目录
                log_dir = std::filesystem::current_path();
            }
            // 拼完整文件名
            std::string fname = std::string("lsf_") + datetime_buf + ".log";
            std::filesystem::path full = log_dir / fname;
            log_filename_ = full.string();

            // 5）打开日志
            log_file.open(log_filename_, std::ios::app);
            if (!log_file.is_open()) {
                std::cerr << "Failed to open log file: " << log_filename_ << "\n";
            }
        }
    }

    void Logger::closeLogFile()
    {
        if (log_file.is_open())
        {
            log_file.close();
        }
    }

    void Logger::setLoggingEnabled(bool enabled)
    {
        enabled_ = enabled;
        if (enabled)
        {
            openLogFile();
        }
        else
        {
            closeLogFile();
        }
    }

    Logger *Logger::getInstance()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (instance == nullptr)
        {
            instance = new Logger();
        }
        return instance;
    }

    void Logger::setLogLevel(LogLevel level)
    {
        level_ = level;
    }

    std::string Logger::getTimestamp()
    {
        /*
            std::time_t now = std::time(nullptr);
            std::string timestamp = std::ctime(&now);
            timestamp.pop_back(); // 移除换行符
            return timestamp;*/
        using namespace std::chrono;

        // 获取当前系统时间
        auto now = system_clock::now();
        auto in_time_t = system_clock::to_time_t(now);

        // 转换为本地时间
        std::tm buf;
#ifdef _WIN32
        localtime_s(&buf, &in_time_t);
#else
        localtime_r(&in_time_t, &buf);
#endif

        // 格式化时分秒部分
        char time_str[64];
        std::strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &buf);

        // 获取毫秒数
        auto ms = duration_cast<microseconds>(now.time_since_epoch()) % 1000000;

        std::ostringstream oss;
        oss << time_str << '.' << std::setfill('0') << std::setw(3) << ms.count();
        return oss.str();
    }

    std::string Logger::getLevelString(LogLevel level)
    {
        switch (level)
        {
        case DEBUG:
            return "DEBUG";
        case INFO:
            return "INFO";
        case WARNING:
            return "WARNING";
        case ERROR:
            return "ERROR";
        case FATAL:
            return "FATAL";
        default:
            return "UNKNOWN";
        }
    }

    void Logger::rotateIfNeeded() {
        if (!log_file.is_open()) return;
        try {
            auto sz = std::filesystem::file_size(log_filename_);
            if (sz >= max_file_size_) {
                log_file.close();
                // 轮转旧日志
                std::string rotated = log_filename_ + ".1";
                std::filesystem::remove(rotated);
                std::filesystem::rename(log_filename_, rotated);
                // 新开一份
                log_file.open(log_filename_, std::ios::app);
            }
        }
        catch (const std::filesystem::filesystem_error &e) {
            // 如果查文件大小或重命名失败，不让程序 crash
            std::cerr << "Log rotate error: " << e.what() << "\n";
        }
    }

    void Logger::log(LogLevel level, const std::string &message,
                     const std::string &file, int line, const char *func)
    {
        if (level < level_ || !enabled_)
            return;

        std::lock_guard<std::mutex> lock(mutex_);

        rotateIfNeeded();    // ← 检查并轮转

        std::stringstream ss;
        ss << "[" << getTimestamp() << "] "
           << "[" << getLevelString(level) << "] "
           << "[" << file << ":" << line << "] "
           << "[" << func << "] "
           << message;

        log_file << ss.str() << std::endl;
        std::cout << ss.str() << std::endl;
        log_file.flush();
    }

    // 添加格式化输出方法
    void Logger::logf(LogLevel level, const char *file, int line, const char *func, const char *format, ...)
    {
        if (level < level_ || !enabled_)
            return;

        // 首先尝试计算需要的缓冲区大小
        va_list args1;
        va_start(args1, format);
        va_list args2;
        va_copy(args2, args1);

        const int buffer_size = vsnprintf(nullptr, 0, format, args1) + 1;
        va_end(args1);

        // 分配缓冲区
        std::vector<char> buffer(buffer_size);

        // 格式化字符串
        vsnprintf(buffer.data(), buffer_size, format, args2);
        va_end(args2);

        // 输出日志
        log(level, std::string(buffer.data()), file, line, func);
    }

}
