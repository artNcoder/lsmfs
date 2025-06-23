#ifndef LSF_LOG_H
#define LSF_LOG_H

#include <iostream>
#include <fstream>
#include <string>
#include <mutex>
#include <cstdarg>
#include <vector>
#include <filesystem> 

namespace LSF
{
    class Logger
    {
    public:
        enum LogLevel
        {
            DEBUG,
            INFO,
            WARNING,
            ERROR,
            FATAL
        };

        static Logger *getInstance();
        void setLogLevel(LogLevel level);
        void log(LogLevel level, const std::string &message,
                 const std::string &file, int line, const char *func);
        // 添加格式化输出方法
        void logf(LogLevel level, const char *file, int line, const char *func, const char *format, ...);
        // 删除拷贝构造和赋值操作
        Logger(const Logger &) = delete;
        Logger &operator=(const Logger &) = delete;
		void setLoggingEnabled(bool enabled); // 添加开关控制方法
		void openLogFile(); // 添加打开日志文件的方法
	    void closeLogFile(); // 添加关闭日志文件的方法
		void enable() { enabled_ = true; }
		void disable() { enabled_ = false; }
		bool isEnabled() const { return enabled_; }
    private:
        Logger();
        ~Logger();
        std::string getTimestamp();
        std::string getLevelString(LogLevel level);

        static Logger *instance;
        static std::mutex mutex_;
        std::ofstream log_file;
        LogLevel level_;
		bool enabled_;

        std::string log_filename_;
        std::uintmax_t max_file_size_ = 2 * 1024 * 1024; // 2MB
        void rotateIfNeeded();
    };

// 定义便捷的日志宏
#define LOG_DEBUG(message) LSF::Logger::getInstance()->log(LSF::Logger::DEBUG, message, __FILE__, __LINE__, __FUNCTION__)
#define LOG_INFO(message) LSF::Logger::getInstance()->log(LSF::Logger::INFO, message, __FILE__, __LINE__, __FUNCTION__)
#define LOG_WARNING(message) LSF::Logger::getInstance()->log(LSF::Logger::WARNING, message, __FILE__, __LINE__, __FUNCTION__)
#define LOG_ERROR(message) LSF::Logger::getInstance()->log(LSF::Logger::ERROR, message, __FILE__, __LINE__, __FUNCTION__)
#define LOG_FATAL(message) LSF::Logger::getInstance()->log(LSF::Logger::FATAL, message, __FILE__, __LINE__, __FUNCTION__)

// 添加格式化输出的宏
#define LOG_DEBUGF(fmt, ...) LSF::Logger::getInstance()->logf(LSF::Logger::DEBUG, __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define LOG_INFOF(fmt, ...) LSF::Logger::getInstance()->logf(LSF::Logger::INFO, __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define LOG_WARNINGF(fmt, ...) LSF::Logger::getInstance()->logf(LSF::Logger::WARNING, __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define LOG_ERRORF(fmt, ...) LSF::Logger::getInstance()->logf(LSF::Logger::ERROR, __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define LOG_FATALF(fmt, ...) LSF::Logger::getInstance()->logf(LSF::Logger::FATAL, __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)

}

#endif // LOGGER_H
