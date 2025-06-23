// thread_pool.h 
#pragma once
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <vector>
#include <queue>

class ThreadPool {
public:
  ThreadPool(size_t n): stop_(false) {
    for(size_t i=0;i<n;i++){
      workers_.emplace_back([this] {
        while(true){
          std::function<void()> task;
          { 
            std::unique_lock<std::mutex> lk(mtx_);
            cv_.wait(lk, [this]{ return stop_ || !tasks_.empty(); });
            if(stop_ && tasks_.empty()) return;
            task = std::move(tasks_.front()); tasks_.pop();
          }
          task();
        }
      });
    }
  }
  ~ThreadPool(){
    {
      std::unique_lock<std::mutex> lk(mtx_);
      stop_ = true;
    }
    cv_.notify_all();
    for(auto &t: workers_) t.join();
  }
  void enqueue(std::function<void()> f){
    {
      std::unique_lock<std::mutex> lk(mtx_);
      tasks_.push(std::move(f));
    }
    cv_.notify_one();
  }
private:
  std::vector<std::thread>    workers_;
  std::queue<std::function<void()>> tasks_;
  std::mutex                  mtx_;
  std::condition_variable     cv_;
  bool                        stop_;
};