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

#include "event.h"
#include "msg-input-stream.h"
#include "msg-output-stream.h"
#include "rule-engine.h"

typedef std::vector<evt_t> msgs_t;

/**
 * The AbstractConsumer is the base class for any provenance-source
 * specific consumer. Each source (e.g. auditd) requires its own
 * consumer implementation.
 */
class AbstractConsumer {
private:
  static const int BATCH_TIMEOUT = 5000;

  /*
   * Any consumer-specific processing that should happen on event
   * receiving needs to go in here.
   */
  virtual int receive_event(ConsumerSource csrc, evt_t event) =0;
  /*
   * Action handler, which takes a received message and evaluates its set of rules
   * on the message. If the conditions of a rule are met, the corresponding
   * actions will be triggered.
   *
   * The base clase provides a base implementation of this method (that just
   * goes through all the rules for the received event and executes the
   * matching ones) but inheriting classes can override it to add custom processing.
   */
  virtual int evaluate_rules(evt_t msg);

protected:
  uint32_t batch_size;

  ConsumerSource c_src;
  ConsumerDestination c_dst;
  std::unique_ptr<MsgInputStream> in_stream;
  std::unique_ptr<MsgOutputStream> out_stream;
  std::unique_ptr<RuleEngine> rule_engine;
  msgs_t msg_buffer;

public:
  AbstractConsumer(ConsumerSource csrc, std::unique_ptr<MsgInputStream> in,
      ConsumerDestination cdest, std::unique_ptr<MsgOutputStream> out,
      uint32_t batchsize = 10000);
  virtual ~AbstractConsumer();

  /*
   * Run the main consumer loop, i.e. receive message batch from input
   * source, normalize batch for output destination, and send.
   */
  int run();
};

#endif /* CONSUMER_ABSTRACT_CONSUMER_H_ */
