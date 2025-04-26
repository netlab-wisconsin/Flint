#pragma once
#include "misc.h"

#include <numa.h>
#include <stdint.h>
#include <thread>
#include <vector>

namespace flint {

inline unsigned GetCoresNum(int numa_node) {
  rt_assert(numa_node <= numa_max_node(), "Invalid numa node.");
  int cpu_num_max = numa_num_configured_cpus();
  unsigned cores = 0;
  for (int i = 0; i < cpu_num_max; i++) {
    if (numa_node == numa_node_of_cpu(i))
      cores++;
  }
  return cores;
}

inline std::vector<int> GetCoresOnNuma(int numa_node) {
  rt_assert(numa_node <= numa_max_node(), "Invalid numa node.");
  int cpu_num_max = numa_num_configured_cpus();
  std::vector<int> cores;
  for (int i = 0; i < cpu_num_max; i++) {
    if (numa_node == numa_node_of_cpu(i))
      cores.emplace_back(i);
  }
  return cores;
}

inline void BindToCore(std::thread& thread, int numa_node, int numa_local_index) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);

  const std::vector<int> lcore_vec = GetCoresOnNuma(numa_node);
  if ((size_t) numa_local_index >= lcore_vec.size())
    return;

  const size_t global_index = lcore_vec.at(numa_local_index);
  CPU_SET(global_index, &cpuset);
  int rc = pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t),
                                  &cpuset);
  rt_assert(rc == 0, "Error setting thread affinity");
}

inline void BindToCore(std::thread& thread, int core) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  int rc = pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t),
                                  &cpuset);
  rt_assert(rc == 0, "Error setting thread affinity");
}

};