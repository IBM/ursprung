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

#include "kafka-output-stream.h"
#include "logger.h"
#include "error.h"
#include "config.h"

KafkaOutputStream::KafkaOutputStream(std::string topic, std::string brokers,
    std::string buffer_max_msgs, std::string buffer_max_ms, std::string batch_num_msgs) :
    topic { topic },
    brokers { brokers },
    buffer_max_msgs { buffer_max_msgs },
    buffer_max_ms { buffer_max_ms },
    batch_num_msgs { batch_num_msgs } {
  conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  topic_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
  rdkafka_topic = nullptr;
  producer = nullptr;
}

KafkaOutputStream::~KafkaOutputStream() {
  if (conf) delete conf;
  if (topic_conf) delete topic_conf;
  if (rdkafka_topic) delete rdkafka_topic;
  if (producer) delete producer;
}

int KafkaOutputStream::open() {
  std::string errstr;

  // set brokers
  if (conf->set("metadata.broker.list", brokers, errstr) != RdKafka::Conf::CONF_OK) {
    LOGGER_LOG_ERROR("Couldn't set brokers for KafkaInputStream: " << errstr);
    return ERROR_NO_RETRY;
  }
  // set max msg number allowed on producer queue
  if (conf->set("queue.buffering.max.messages", buffer_max_msgs, errstr) != RdKafka::Conf::CONF_OK) {
      std::cerr << errstr << std::endl;
      return ERROR_NO_RETRY;
  }
  // set batch wait time for producer queue
  if (conf->set("queue.buffering.max.ms", buffer_max_ms, errstr) != RdKafka::Conf::CONF_OK) {
      std::cerr << errstr << std::endl;
      return ERROR_NO_RETRY;
  }
  // set maximum size of single batch
  if (conf->set("batch.num.messages", batch_num_msgs, errstr) != RdKafka::Conf::CONF_OK) {
      std::cerr << errstr << std::endl;
      return ERROR_NO_RETRY;
  }
  // disable statistics
  if (conf->set("statistics.interval.ms", "0", errstr) != RdKafka::Conf::CONF_OK) {
      std::cerr << errstr << std::endl;
      return ERROR_NO_RETRY;
  }
  // wait for all in-sync replicas to ACK message before commit
  if (topic_conf->set("request.required.acks", "-1", errstr) != RdKafka::Conf::CONF_OK) {
      std::cerr << errstr << std::endl;
      return ERROR_NO_RETRY;
  }
  // set SASL authentication options
  if (!Config::config[Config::CKEY_KAFKA_SASL_USER].empty() &&
      !Config::config[Config::CKEY_KAFKA_SASL_PASS].empty()) {
    if (conf->set("security.protocol", "sasl_plaintext", errstr) != RdKafka::Conf::CONF_OK) {
      LOGGER_LOG_ERROR("Failed to set security protocol for KafkaInputStream: " << errstr);
      return ERROR_NO_RETRY;
    }
    if (conf->set("sasl.mechanisms", "SCRAM-SHA-512", errstr) != RdKafka::Conf::CONF_OK) {
      LOGGER_LOG_ERROR("Failed to set sasl mechanism for KafkaInputStream: " << errstr);
      return ERROR_NO_RETRY;
    }
    if (conf->set("sasl.username", Config::config[Config::CKEY_KAFKA_SASL_USER],
        errstr) != RdKafka::Conf::CONF_OK) {
      LOGGER_LOG_ERROR("Failed to set sasl username for KafkaInputStream: " << errstr);
      return ERROR_NO_RETRY;
    }
    if (conf->set("sasl.password", Config::config[Config::CKEY_KAFKA_SASL_PASS],
        errstr) != RdKafka::Conf::CONF_OK) {
      LOGGER_LOG_ERROR("Failed to set sasl password for KafkaInputStream: " << errstr);
      return ERROR_NO_RETRY;
    }
  }

  // create consumer
  producer = RdKafka::Producer::create(conf, errstr);
  if (!producer) {
    LOGGER_LOG_ERROR("Couldn't create Kafka producer: " << errstr);
    return ERROR_NO_RETRY;
  }

  // Create topic handle
  rdkafka_topic = RdKafka::Topic::create(producer, topic, topic_conf, errstr);
  if (!rdkafka_topic) {
    LOGGER_LOG_ERROR("Couldn't create Kafka topic: " << errstr);
    return ERROR_NO_RETRY;
  }

  return NO_ERROR;
}

void KafkaOutputStream::close() {
  // nothing to do
}

int KafkaOutputStream::send(const std::string &msg_str, int target_idx,
    const std::string *key) {
  RdKafka::ErrorCode resp;
  while (true) {
    resp = producer->produce(rdkafka_topic,
        target_idx,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(msg_str.c_str()),
        msg_str.size(),
        key,
        nullptr);

    producer->poll(0);
    // if queue is full, wait and retry
    if (resp == RdKafka::ERR__QUEUE_FULL) {
      producer->poll(200);
      continue;
    }

    if (resp != RdKafka::ERR_NO_ERROR) {
      LOGGER_LOG_ERROR("Error while sending record to Kafka: " << RdKafka::err2str(resp));
      return ERROR_NO_RETRY;
    } else {
      break;
    }
  }

  return NO_ERROR;
}

void KafkaOutputStream::flush() const {
  auto msgs_to_send = producer->outq_len();

  while (producer->outq_len() > 0) {
    producer->poll(500);
  }
}

int KafkaOutputStream::send_batch(const std::vector<std::string> &msg_batch) {
  LOGGER_LOG_WARN("Method not supported!");
  return NO_ERROR;
}

std::string KafkaOutputStream::str() const {
  return brokers + ":" + topic;
}
