/**
 * Copyright 2020 IBM
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

#ifndef UTIL_SYNC_QUEUE_H_
#define UTIL_SYNC_QUEUE_H_

#include <mutex>
#include <queue>

/**
 * A thread safe blocking queue.
 */
template<class T>
class SynchronizedQueue {
private:
  std::mutex mutex;
  std::condition_variable monitor;
  std::queue<T> queue;

public:
  void push(T elem);
  T pop();
};

template<class T>
void SynchronizedQueue<T>::push(T elem) {
  {
    std::unique_lock<std::mutex> lock(mutex);
    queue.push(elem);
  }
  this->monitor.notify_one();
}

template<class T>
T SynchronizedQueue<T>::pop() {
  std::unique_lock<std::mutex> lock(mutex);
  monitor.wait(lock, [=]() {
    return !queue.empty();
  });

  T elem(queue.front());
  queue.pop();

  return elem;
}

#endif /* UTIL_SYNC_QUEUE_H_ */
