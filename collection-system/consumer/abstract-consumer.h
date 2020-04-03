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

#ifndef CONSUMER_ABSTRACT_CONSUMER_H_
#define CONSUMER_ABSTRACT_CONSUMER_H_

#include <vector>

#include "intermediate-message.h"
#include "msg-input-stream.h"
#include "msg-output-stream.h"

typedef std::vector<std::string> msgs_t;
typedef std::shared_ptr<IntermediateMessage> im_t;

/**
 * The AbstractConsumer is the base class for any provenance-source
 * specific consumer. Each source (e.g. auditd) requires its own
 * consumer implementation.
 */
class AbstractConsumer {
private:
  /**
   * Build an intermediate message from the message specified in the string.
   * Then the other private APIs can be implemented in terms of the IntermediateMessage
   * APIs. If you don't want to use those APIs, return a nullptr from your subclass's
   * version of this function.
   */
  virtual im_t build_intermediate_message(ConsumerSource csrc, const std::string &msgin) =0;

  /**
   * Build an intermediate message from the message specified in the string.
   * Then the other private APIs can be implemented in terms of the IntermediateMessage
   * APIs. If you don't want to use those APIs, return a nullptr from your subclass's
   * version of this function.
   *
   * In some cases, an incoming message can trigger multiple messages to be generated
   * (e.g. in case of a directory rename event, a new rename event for each individual
   * file in that directory has to be generated). Hence, this method returns a list of
   * IntermediateMessages.
   */
  virtual std::vector<im_t> build_intermediate_messages(ConsumerSource csrc, const std::string &msgin) =0;

  /**
   * Action handler, which takes a received message and evaluates its set of rules
   * on the message. If the conditions of a rule are met, the corresponding
   * actions will be triggered.
   */
  virtual int evaluate_rules(im_t msg);

  /**
   * Receive messages from Kafka and build message batch.
   */
  int recv();

  /**
   * Normalize the batch of messages to correctly format them
   * for their target destination.
   */
  int normalize_batch();

  /**
   * Commit message batch to the destination provenance store (e.g.
   * DB2 or Postgres).
   */
  int send();

protected:
  uint32_t max_batch_size;
  uint32_t batch_size;

  ConsumerSource c_src;
  ConsumerDestination c_dest;
  std::unique_ptr<MsgInputStream> inStream;
  std::unique_ptr<MsgOutputStream> outStream;

  msgs_t msg_buffer;
  //std::shared_ptr<RuleEngine> rule_engine;

public:
  AbstractConsumer(ConsumerSource csrc, MsgInputStream *in,
      ConsumerDestination cdest, MsgOutputStream *out,
      uint32_t batchsize = 10000);
  virtual ~AbstractConsumer();

  int run();
  int initRuleEngine(std::string rules_file);
  int shutdownRuleEngine();
};

#endif /* CONSUMER_ABSTRACT_CONSUMER_H_ */
