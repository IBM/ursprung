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

#ifndef PROV_AUDITD_CONSUMER_H
#define PROV_AUDITD_CONSUMER_H

#include <unordered_set>

#include "abstract-consumer.h"
#include "rule-engine.h"

class AuditdConsumer: public AbstractConsumer {
private:
  /*
   * List of tracees that are actively traced by this consumer through
   * a standardout capture rule.
   *
   * TODO std::string should be replaced by a custom object such as Tracee
   */
  std::unordered_set<std::string> active_tracees;

  virtual int receive_event(ConsumerSource csrc, evt_t event) override;
  virtual int evaluate_rules(evt_t msg) override;

public:
  AuditdConsumer(ConsumerSource csrc, std::unique_ptr<MsgInputStream> in,
      ConsumerDestination cdst, std::unique_ptr<MsgOutputStream> out,
      uint32_t batchsize = 10000) :
      AbstractConsumer(csrc, std::move(in), cdst, std::move(out), batchsize) {}
  ~AuditdConsumer() {};
};

#endif
