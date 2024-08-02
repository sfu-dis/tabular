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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <noname_defs.h>

#include <thread/thread.h>
#include <cmath>
#include "config/config.h"

// Simple thread pool creation - really nothing much; verify the output matches numactl -H output.
TEST(ThreadTest, Topology) {
  noname::thread::ThreadPool tpool(noname::thread::config_t(true));
}

// Mock task function
void MockTaskString(char* task_input) {
  // Convert char* input to std::string
  std::string s(task_input);

  // Append "_processed" to the task input
  s += "_processed";

  // Write back the processed string to the input
  strncpy(task_input, s.c_str(), s.length() + 1);
}

TEST(ThreadTest, ExecutionTest) {
  noname::thread::ThreadPool tpool(noname::thread::config_t(true));
  // Get a thread, and if dedicated core needed,
  noname::thread::Thread* t = tpool.GetThread(true);

  // Verify if we actually got a thread
  ASSERT_NE(t, nullptr);

  // Initialize a task input
  char task_input[32] = "test_input";

  // Start a task
  t->StartTask(MockTaskString, task_input);
  // Wait until the thread finish the task
  t->Join();

  // Check if the task was processed
  EXPECT_STREQ(task_input, "test_input_processed");

  // Release the thread back to the pool
  tpool.PutThread(t);
}

const int kNumOfThreads = 80;

std::atomic<int> results[kNumOfThreads * 2];

void StimulateComputation() {
  double base = 5.6;
  for (int i = 0; i < 5e7; i++) {
    base = std::pow(base, 2);
    base = std::sqrt(base);
  }
}

void MockTaskSquareNumber(char* task_input) {
  int* input = reinterpret_cast<int*>(task_input);
  int result = (*input) * (*input);
  results[*input] = result;
  StimulateComputation();
}

TEST(ThreadPoolTest, MultiTaskTest) {
  for (auto& result : results) {
    result = -1;
  }

  noname::thread::ThreadPool tpool(noname::thread::config_t(true));
  std::vector<noname::thread::Thread *> threads;
  for (int i = 0; i < kNumOfThreads; i++) {
    int* task_input = new int(i);
    auto thread = tpool.GetThread(false);
    if (thread) {
      thread->StartTask(MockTaskSquareNumber, reinterpret_cast<char*>(task_input));
      threads.push_back(thread);
    } else {
      std::cout << "No more threads available, try increasing the pool size.\n";
      delete task_input;
    }
  }

  // Wait for all the threads to join
  for (auto &t : threads) {
    t->Join();
    tpool.PutThread(t);
  }

  for (int i = 0; i < kNumOfThreads; i++) {
    ASSERT_EQ(results[i], i * i);
  }
}

TEST(ThreadPoolTest, TaskCountExceedingThreadLimitTest) {
  for (auto& result : results) {
    result = -1;
  }

  noname::thread::ThreadPool tpool(noname::thread::config_t(true));
  std::vector<noname::thread::Thread *> threads;
  for (int i = 0; i < kNumOfThreads; i++) {
    int* task_input = new int(i);
    auto thread = tpool.GetThread(false);
    if (thread) {
      thread->StartTask(MockTaskSquareNumber, reinterpret_cast<char*>(task_input));
      threads.push_back(thread);
    } else {
      std::cout << "No more threads available, try increasing the pool size.\n";
      delete task_input;
    }
  }

  // Wait for all the threads to join
  for (auto &t : threads) {
    t->Join();
    tpool.PutThread(t);
  }

  threads.clear();

  // Start another batch of tasks to update second half of the array
  for (int i = kNumOfThreads; i < kNumOfThreads * 2; i++) {
    int* task_input = new int(i);
    auto thread = tpool.GetThread(false);
    if (thread) {
      thread->StartTask(MockTaskSquareNumber, reinterpret_cast<char*>(task_input));
      threads.push_back(thread);
    } else {
      std::cout << "No more threads available, try increasing the pool size.\n";
      delete task_input;
    }
  }

  for (auto &t : threads) {
    t->Join();
    tpool.PutThread(t);
  }

  for (int i = 0; i < kNumOfThreads * 2; i++) {
    ASSERT_EQ(results[i], i * i);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
