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

#include <string>
#include <vector>

#include "thread.h"

namespace noname {
namespace thread {

ThreadPool::ThreadPool(config_t config) :
    config(config) {
  bool detected = DetectTopology(cpu_cores);
  LOG_IF(FATAL, !detected);
  uint64_t num_thread_pools = numa_max_node() + 1;

  for (uint16_t i = 0; i < num_thread_pools; i++) {
    local_pools.emplace_back(cpu_cores, i);
  }
}

ThreadPool::ThreadPool() : ThreadPool(config_t()) {}

bool ThreadPool::DetectTopology(std::vector<CPUCore> &out_cpu_cores) {
  // FIXME(tzwang): Linux-specific way of querying NUMA topology
  //
  // We used to query /sys/devices/system/node/nodeX/cpulist to get a list of
  // all cores for this node, but it could be a comma-separated list (x, y, z)
  // or a range (x-y). So we just iterate each cpu dir here until dir not
  // found.
  struct stat info;
  if (stat("/sys/devices/system/node", &info) != 0) {
    return false;
  }

  for (uint32_t node = 0; node < numa_max_node() + 1; ++node) {
    uint32_t core = 0;
    while (core < std::thread::hardware_concurrency()) {
      std::string dir_name = "/sys/devices/system/node/node" +
                              std::to_string(node) + "/cpu" + std::to_string(core);
      struct stat info;
      if (stat(dir_name.c_str(), &info) != 0) {
        // Doesn't exist, continue to next to get all cores in the same node
        ++core;
        continue;
      }
      LOG_IF(FATAL, !(info.st_mode & S_IFDIR));

      // Make sure it's a physical thread, not a hyper-thread: Query
      // /sys/devices/system/cpu/cpuX/topology/thread_siblings_list, if the first number matches X,
      // then it's a physical core [1] (might not work in virtualized environments like Xen). [1]
      // https://stackoverflow.com/questions/7274585/linux-find-out-hyper-threaded-core-id
      std::string sibling_file_name = "/sys/devices/system/cpu/cpu" +
                                      std::to_string(core) +
                                      "/topology/thread_siblings_list";
      char cpu_buf[8];
      memset(cpu_buf, 0, 8);
      std::vector<uint32_t> sys_cpus;
      std::ifstream sibling_file(sibling_file_name);
      while (sibling_file.good()) {
        memset(cpu_buf, 0, 8);
        sibling_file.getline(cpu_buf, 256, ',');
        sys_cpus.push_back(atoi(cpu_buf));
      }

      if (core == sys_cpus[0]) {
        auto &cpu_core = out_cpu_cores.emplace_back(node);
        for (uint32_t i = 0; i < sys_cpus.size(); ++i) {
          cpu_core.AddSystemCPU(sys_cpus[i]);
        }

        for (auto sc : cpu_core.sys_cpus) {
          LOG(INFO) << "Core " << core << ": system CPU " << sc;
        }
      }
      ++core;
    }
  }
  return true;
}

Thread::Thread()
    : node(0),
      core(0),
      sys_cpu(0),
      in_core_id(0),
      shutdown(false),
      free(true),
      state(kStateSleep),
      task(nullptr),
      sleep_when_idle(true) {
  thd = std::thread(Thread::StaticHandleTask, this);
}

Thread::Thread(uint32_t node, uint32_t core, uint32_t sys_cpu, uint32_t in_core_id)
    : node(node),
      core(core),
      sys_cpu(sys_cpu),
      in_core_id(in_core_id),
      shutdown(false),
      free(true),
      state(kStateSleep),
      task(nullptr),
      sleep_when_idle(true) {
  thd = std::thread(Thread::StaticHandleTask, this);
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(sys_cpu, &cpuset);
  auto rc = pthread_setaffinity_np(thd.native_handle(), sizeof(cpu_set_t), &cpuset);
  CHECK_EQ(rc, 0);
  LOG(INFO) << "Pinned thread " << in_core_id << " to system CPU " << sys_cpu
            << " on core " << core;
}

void Thread::HandleTask() {
  std::unique_lock<std::mutex> lock(trigger_lock);
  if (free) {
    trigger.wait(lock);
    SetState(kStateNoWork);
    while (GetState() != kStateHasWork && !shutdown.load(std::memory_order_acquire)) {
      /** spin **/
    }
  }

  while (!shutdown.load(std::memory_order_acquire)) {
    auto s = GetState();
    CHECK(!free);
    if (s == kStateHasWork) {
      task();
      SetState(kStateNoWork);
    }

    uint8_t expected_state = kStateNoWork;
    if (sleep_when_idle && state.compare_exchange_strong(expected_state, kStateSleep)) {
      // FIXME(tzwang): add a work queue so we can continue if there is more work to do
      trigger.wait(lock);
      SetState(kStateNoWork);

      // Somebody woke me up, wait for work to do or shutdown
      while (GetState() != kStateHasWork && !shutdown.load(std::memory_order_acquire)) {
        /** spin **/
      }
    }  // else can't sleep, go check another round
  }
}

// No CC whatsoever, caller must know what it's doing
// template<typename Func, typename... Args>
// void Thread::StartTask(Func&& func, Args&&... args) {
// }

void Thread::Destroy() {
  // Has to be joined before destroyed
  CHECK(GetState() != kStateHasWork);
  shutdown.store(true, std::memory_order_release);
  trigger.notify_one();
}

NodeThreadPool::NodeThreadPool(std::vector<CPUCore> &cpu_cores, uint32_t node) : node(node) {
  CHECK(!numa_run_on_node(node));

  uint32_t total_numa_nodes = numa_max_node() + 1;
  uint32_t current_core = 0;

  // Create a Thread per hyperthread in this node; could potentially support more
  for (auto &c : cpu_cores) {
    if (c.node == node) {
      for (uint32_t i = 0; i < c.sys_cpus.size(); ++i) {
        threads.push_back(new Thread(node, current_core, c.sys_cpus[i], i));
      }
      ++current_core;
    }
  }
}

NodeThreadPool::~NodeThreadPool() {
  for (auto t : threads) {
    t->Join();
    t->Destroy();
    t->thd.join();
  }
}

// move constructor
NodeThreadPool::NodeThreadPool(NodeThreadPool &&other) noexcept {
  threads.insert(threads.end(), other.threads.begin(), other.threads.end());
  node = other.node;

  other.threads.clear();
}

// move assignment operator
NodeThreadPool &NodeThreadPool::operator=(NodeThreadPool &&other) noexcept {
  for (auto t : threads) {
    t->Join();
    t->Destroy();
    t->thd.join();
  }
  threads.clear();

  threads.insert(threads.end(), other.threads.begin(), other.threads.end());
  node = other.node;

  other.threads.clear();

  return *this;
}

Thread *NodeThreadPool::GetThread(bool dedicated_core) {
  for (auto t : threads) {
    auto is_free = t->free.load(std::memory_order_acquire);
    if (is_free && (t->in_core_id == 0 || !dedicated_core)) {
      if (t->free.compare_exchange_strong(is_free, false)) {
        // Successfully allocated the thread
        return t;
      }
    }
  }
  return nullptr;
}

}  // namespace thread
}  // namespace noname
