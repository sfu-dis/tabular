/*
 * Copyright (C) 2022 Data-Intensive Systems Lab, Simon Fraser University. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// A Topology-aware thread pool.
//
// The "thread pool" consists of a set of "local pools" each of which is a set of threads belonging
// to the same resource type (e.g., NUMA node). By default each software thread is pinned to a
// hyperthread, but the local pools can be customised to support more. Local pools don't have to be
// homogeneous - each local pool can present a different number of cores/hyperthreads, however, we
// do assume cache coherence betweent local pools/hyperthreads globally.
//
// In principle this design allows supporting different interconnects, e.g., NUMA machines or
// compute resources on CXL, both group/expose compute cores as NUMA nodes. The current
// implementation assumes the proc file system in Linux to detect topology; partially based off of
// the implementation in CoroBase/ERMIA (https://github.com/sfu-dis/corobase).

#pragma once

#include <condition_variable>
#include <fstream>
#include <functional>
#include <future>
#include <mutex>
#include <thread>

#include <numa.h>
#include <sys/stat.h>

#include <glog/logging.h>

#include <noname_defs.h>
#include "config.h"

namespace noname {
namespace thread {

// Describes a physical CPU core and its associated hyperthreads
struct CPUCore {
  uint32_t node;
  std::vector<uint32_t> sys_cpus;  // System-given "CPU" number

  CPUCore(uint32_t node) : node(node) {}

  // Add a hyperthread to the physical core
  // @t: system-given CPU number of the hyperthread
  void AddSystemCPU(uint32_t t) { sys_cpus.push_back(t); }
};

// Thread encapsulation - capable of pinning to a core and run customised tasks
struct Thread {
  static const uint8_t kStateHasWork = 1;
  static const uint8_t kStateSleep = 2;
  static const uint8_t kStateNoWork = 3;

  typedef std::function<void()> Task;

  std::thread thd;
  pthread_attr_t thd_attr;
  uint32_t node;  // Max 65k nodes
  uint32_t core;  // Max 65k cores per node
  uint32_t sys_cpu;  // OS-given CPU number
  uint32_t in_core_id;  // ID of myself as a thread pinned to the core
  std::atomic<bool> shutdown;
  std::atomic<bool> free;
  std::atomic<uint8_t> state;
  Task task;
  char *task_input;
  bool sleep_when_idle;

  std::condition_variable trigger;
  std::mutex trigger_lock;

  Thread();
  Thread(uint32_t node, uint32_t core, uint32_t sys_cpu, uint32_t in_core_id);

  void HandleTask();

  static void *StaticHandleTask(void *context) {
      ((Thread *)context)->HandleTask();
      return nullptr;
  }

  inline uint8_t GetState() { return state.load(std::memory_order_acquire); }
  inline void SetState(uint8_t new_state) { state.store(new_state, std::memory_order_release); }

  // No CC whatsoever, caller must know what it's doing
  template<typename Func, typename... Args>
  void StartTask(Func&& func, Args&&... args) {
    using return_type = std::invoke_result<Func, Args...>::type;
    std::function<return_type()> task_wrapper = std::bind(std::forward<Func>(func), std::forward<Args>(args)...);
    auto task_wrapper_ptr = std::make_shared<std::packaged_task<return_type()>>(task_wrapper);
    std::function<void()> wrapper_function = [task_wrapper_ptr]() {
      (*task_wrapper_ptr)();
    };
    task = wrapper_function;
    auto previous = GetState();
    if (previous == kStateSleep) {
      while (GetState() != kStateNoWork) {
        trigger.notify_one();
      }
      SetState(kStateHasWork);
    } else {
      LOG_IF(FATAL, previous != kStateNoWork);
      CHECK_EQ(previous, kStateNoWork);
    }
    SetState(kStateHasWork);
  }

  inline void Join() { while (state.load(std::memory_order_acquire) == kStateHasWork) {} }
  inline bool TryJoin() { return state.load(std::memory_order_acquire) != kStateHasWork; }
  void Destroy();
};

// Gathers all the threads of a single node
struct NodeThreadPool {
  std::vector<Thread *> threads;
  uint32_t node CACHE_ALIGNED;

  // Constructor
  // @cpu_cores: an arry of CPU cores that cover this node
  // @node: NUMA node
  NodeThreadPool(std::vector<CPUCore> &cpu_cores, uint32_t node);
  ~NodeThreadPool();

  NodeThreadPool(const NodeThreadPool& other) = delete;               // copy constructor
  NodeThreadPool& operator=(const NodeThreadPool& other) = delete;    // copy assignment operator
  NodeThreadPool(NodeThreadPool&& other) noexcept;           // move constructor
  NodeThreadPool& operator=(NodeThreadPool&& other) noexcept; // move assignment operator

  // Get a new thread
  // @dedicated_core: whether to dedicate a physical core to it
  Thread *GetThread(bool dedicated_core);

  // Release a thread back to the pool
  // @t: the Thread to release
  inline void PutThread(Thread *t) {
    t->free = true;
  }
};

// A topology-aware thread pool that manages individual groups of compute resources (local NUMA or
// remote) as NUMA nodes. 
struct ThreadPool {
  // All the local pools
  std::vector<NodeThreadPool> local_pools;

  // All detected CPU cores + HT information
  std::vector<CPUCore> cpu_cores;

  config_t config;
  // Discover CPU topology
  // @out_cpu_cores: all the discovered cores
  bool DetectTopology(std::vector<CPUCore> &out_cpu_cores);

  ThreadPool();
  explicit ThreadPool(config_t config);

  // Get a thread from a NUMA node
  // @node: the specified NUMA node
  // @dedicated_core: whether to dedicate a physical core to the thread alone
  inline Thread *GetThread(uint32_t node, bool dedicated_core) {
    return local_pools[node].GetThread(dedicated_core);
  }

  // Get a thread from any NUMA node
  // GetThread will return a thread with a dedicated core if available regardless of `dedicated_core` parameter.
  // @dedicated_core: whether to dedicate a physical core to the thread alone
  inline Thread *GetThread(bool dedicated_core) {
    for (uint32_t i = 0; i < local_pools.size(); i++) {
      auto *t = local_pools[i].GetThread(true);
      if (t) {
        return t;
      }
    }
    if (dedicated_core) {
      // Cannot find an available thread with a dedicated core.
      return nullptr;
    }
    for (uint32_t i = 0; i < local_pools.size(); i++) {
      auto *t = local_pools[i].GetThread(false);
      if (t) {
        return t;
      }
    }
    return nullptr;
  }

  // Release the thread back to the pool
  // @t: Thread to be released
  inline void PutThread(Thread *t) {
    local_pools[t->node].PutThread(t);
  }
};

}  // namespace thread
}  // namespace noname 
