#pragma once
#include <atomic>
#include <memory>

namespace flint {
/// Credit: https://martong.github.io/high-level-cpp-rcu_informatics_2017.pdf
template <typename T>
class RCU {
 private:
  std::shared_ptr<T> sp_;

 public:
  RCU() = default;

  ~RCU() = default;

  RCU(const RCU& other) = delete;

  RCU& operator=(const RCU& other) = delete;

  RCU(RCU&& other) = delete;

  RCU& operator=(RCU&& other) = delete;

  RCU(const std::shared_ptr<const T>& r) : sp_(r) {}

  RCU(std::shared_ptr<const T>&& r) : sp_(std::move(r)) {}

  std::shared_ptr<const T> Read() const {
    return std::atomic_load_explicit(&sp_, std::memory_order_consume);
  }

  std::shared_ptr<T> Read() {
    return std::atomic_load_explicit(&sp_, std::memory_order_consume);
  }

  void Reset(const std::shared_ptr<const T>& r) {
    std::atomic_store_explicit(&sp_, r, std::memory_order_release);
  }

  void Reset(std::shared_ptr<const T>&& r) {
    std::atomic_store_explicit(&sp_, std::move(r), std::memory_order_release);
  }

  template <typename R>
  void Update(R&& func) {
    std::shared_ptr<const T> sp_l =
        std::atomic_load_explicit(&sp_, std::memory_order_consume);
    std::shared_ptr<T> r;
    do {
      if (sp_l) {
        r = std::make_shared<T>(*sp_l);
      }
      std::forward<R>(func)(r.get());
    } while (!std::atomic_compare_exchange_strong_explicit(
        &sp_, &sp_l, std::shared_ptr<const T>(std::move(r)),
        std::memory_order_release, std::memory_order_consume));
  }
};

}  // namespace flint