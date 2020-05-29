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

#ifndef IO_KAFKA_INPUT_STREAM_H_
#define IO_KAFKA_INPUT_STREAM_H_

#include "msg-input-stream.h"
#include <librdkafka/rdkafkacpp.h>

const int TIMEOUT_MS = 500;

class KafkaInputStream: public MsgInputStream {
private:
  std::string topic;
  std::string brokers;
  std::string group_id;

  RdKafka::Conf *conf;
  RdKafka::KafkaConsumer *consumer;
public:
  KafkaInputStream(std::string topic, std::string brokers, std::string group_id);
  virtual ~KafkaInputStream();
  // prevent copy construction and assignment
  KafkaInputStream(const KafkaInputStream &other) = delete;
  KafkaInputStream& operator=(const KafkaInputStream &other) = delete;

  virtual int open() override;
  virtual void close() override;
  virtual int recv(std::string &next_msg) override;
};

#endif /* IO_KAFKA_INPUT_STREAM_H_ */
