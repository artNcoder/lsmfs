#ifndef LSF_UTILS_H
#define LSF_UTILS_H

#include <vector>
#include <functional>

// 实现 ScopedExecutor 类
class ScopedExecutor 
{
public:
    template<typename F>
    ScopedExecutor(F&& f) : func_(std::forward<F>(f)) {}
    
    ~ScopedExecutor() {
        if (func_) {
            func_();
        }
    }

    // 禁用拷贝
    ScopedExecutor(const ScopedExecutor&) = delete;
    ScopedExecutor& operator=(const ScopedExecutor&) = delete;

private:
    std::function<void()> func_;
};

class ResourceManager
{
public:
	
	template<typename F>
    void push_back(F&& f) {
        functions_.push_back(std::forward<F>(f));
    }
    ~ResourceManager()
    {	
	    // 反向执行清理函数
        for (auto it = functions_.rbegin(); it != functions_.rend(); ++it) {
            (*it)();
        }
    }


    void releaseall()
    {
        functions_.clear();
    }

private:
    std::vector<std::function<void()>> functions_;
	
};





#endif
