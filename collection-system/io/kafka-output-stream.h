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

#ifndef IO_KAFKA_OUTPUT_STREAM_H_
#define IO_KAFKA_OUTPUT_STREAM_H_

#include "msg-output-stream.h"
#include <librdkafka/rdkafkacpp.h>

const int TIMEOUT_MS = 500;

class KafkaOutputStream: public MsgOutputStream {
private:
  std::string topic;
  std::string brokers;
  std::string buffer_max_msgs;
  std::string buffer_max_ms;
  std::string batch_num_msgs;

  RdKafka::Conf *conf;
  RdKafka::Conf *topic_conf;
  RdKafka::Topic *rdkafka_topic;
  RdKafka::Producer *producer;

public:
  KafkaOutputStream(std::string topic, std::string brokers,
      std::string buffer_max_msgs = "20000", std::string buffer_max_ms = "100",
      std::string batch_num_msgs = "5000");
  virtual ~KafkaOutputStream();
  // prevent copy construction and assignment
  KafkaOutputStream(const KafkaOutputStream &other) = delete;
  KafkaOutputStream& operator=(const KafkaOutputStream &other) = delete;

  virtual int open() override;
  virtual void close() override;
  virtual int send(const std::string &msg_str, int partition,
      const std::string *key = nullptr) override;
  virtual int send_batch(const std::vector<std::string> &msg_batch) override;
  virtual void flush() const override;
  virtual std::string str() const override;
};

#endif /* IO_KAFKA_OUTPUT_STREAM_H_ */
