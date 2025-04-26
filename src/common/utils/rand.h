#pragma once

#include <random>

namespace flint {

class SlowRand {
private:
  std::random_device rand_dev_;
  std::mt19937_64 mt_;
  std::uniform_int_distribution<uint64_t> dist_;

public:
  SlowRand() : mt_(rand_dev_()), dist_(0, UINT64_MAX) {}

  uint64_t operator()() { 
    return dist_(mt_); 
  }
};

/// @brief Stolen from eRPC's code.
class FastRand {
private:
  uint64_t seed_;

public:
  FastRand() {
    SlowRand slow_rand;
    seed_ = slow_rand();
  }

  uint32_t operator()() {
    seed_ = seed_ * 1103515245 + 12345;
    return static_cast<uint32_t>(seed_ >> 32);
  }
};

class BinaryRand {
private:
  std::random_device rand_dev_;
  std::minstd_rand0 mr_;
  std::bernoulli_distribution dist_;

public:
  BinaryRand(float prob) : mr_(rand_dev_()), dist_(prob) {}

  bool operator()() {
    return dist_(mr_);
  } 
};

};