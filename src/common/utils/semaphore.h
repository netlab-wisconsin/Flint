#pragma once
#include <mutex>
#include <condition_variable>
#include <chrono>

namespace flint {

class Semaphore {
private:
  int count_;
  int c_;
  std::mutex mu_;
  std::condition_variable cv_;

public:
  /// Default to be a binary semaphore
  explicit Semaphore(int c = 1) : count_(0), c_(c) {}

  void Reset(int c) {
    std::lock_guard<std::mutex> lock(mu_);
    c_ = c;
    count_ = 0;
  }

  void Wait() {
    std::unique_lock<std::mutex> lock(mu_);
    if (c_ == 0)
      return;
    cv_.wait(lock, [this] {
      return count_ > 0;
    });
    count_ = std::max(count_ - 1, 0);
  }

  bool Wait(unsigned timeout_us) {
    std::unique_lock<std::mutex> lock(mu_);
    bool not_expired = cv_.wait_for(lock, std::chrono::microseconds(timeout_us), [this] {
      return count_ > 0;
    });
    if (not_expired)
      count_ = std::max(count_ - 1, 0);
    return not_expired;
  }

  void Signal() {
    std::lock_guard<std::mutex> lock(mu_);
    count_ = std::min(count_ + 1, c_);
    if (count_ == 1)
      cv_.notify_one();
  }

  inline int Count() const { return count_; }
};

};