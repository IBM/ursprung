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

#include <signal.h>
#include <chrono>

#include "abstract-consumer.h"
#include "config.h"
#include "signal-handling.h"

AbstractConsumer::AbstractConsumer(ConsumerSource csrc,
    std::unique_ptr<MsgInputStream> in, ConsumerDestination cdst,
    std::unique_ptr<MsgOutputStream> out, uint32_t batchsize) :
    c_src(csrc), in_stream(std::move(in)),
    c_dst(cdst), out_stream(std::move(out)),
    batch_size(batchsize) {
  if (!Config::config[Config::CKEY_RULES_FILE].empty()) {
    rule_engine = std::make_unique<RuleEngine>(Config::config[Config::CKEY_RULES_FILE]);
  }
  in_stream->open();
  out_stream->open();
}

AbstractConsumer::~AbstractConsumer() {
  if (rule_engine) {
    rule_engine->shutdown();
  }
  in_stream->close();
  out_stream->close();
}

int AbstractConsumer::run() {
  signal_handling::setup_handlers();
  // This is only needed to be able to run the unit tests for different consumers
  // (as each consumer unit test will set this to 0 after finishing)
  signal_handling::running = 1;

  std::string next_msg;
  int rc;
  while (signal_handling::running) {
    // build batch
    bool batch_done = false;
    auto batch_start = std::chrono::steady_clock::now();
    while (!batch_done && signal_handling::running) {
      rc = in_stream->recv(next_msg);
      if (rc == NO_ERROR) {
        evt_t evt = Event::deserialize_event(next_msg);
        if (!evt) {
          LOGGER_LOG_ERROR("Problems while receiving event " << next_msg << " Skipping event.");
          continue;
        }
        if (receive_event(c_src, evt)) {
          LOGGER_LOG_ERROR("Problems while processing event " << next_msg << " Skipping event.");
          continue;
        }
        msg_buffer.push_back(evt);

        // find and execute any matching rules
        if (evaluate_rules(evt) != NO_ERROR) {
          LOGGER_LOG_ERROR("Problems while executing rules, some provenance " <<
              "might be lost");
        }
      } else if (rc == ERROR_NO_RETRY || rc == ERROR_EOF) {
        signal_handling::running = false;
      } else {
        // log and ignore error
        LOGGER_LOG_DEBUG("Got error " << rc << " during receive. Continuing.");
      }

      if (msg_buffer.size() > batch_size) {
        batch_done = true;
      } else {
      // check if the batch has timed out and if so, send it
        auto curr_time = std::chrono::steady_clock::now();
        long elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                curr_time - batch_start).count();
        if (elapsed_time >= BATCH_TIMEOUT && msg_buffer.size() > 0) {
          LOGGER_LOG_DEBUG("Batch timed out and will be sent with size " << msg_buffer.size());
          batch_done = true;
        }
      }
    }

    // normalize messages for destination
    LOGGER_LOG_INFO("Submitting batch of size " << msg_buffer.size());
    std::vector<std::string> normalized_msgs;
    for (evt_t evt : msg_buffer) {
      normalized_msgs.push_back(evt->format_for_dst(c_dst));
    }

    // send messages
    rc = out_stream->send_batch(normalized_msgs);
    if (rc != NO_ERROR) {
      LOGGER_LOG_ERROR("Problems while sending batch. Messages might have been lost.");
      // TODO better error handling
    }

    // clear buffer
    msg_buffer.clear();
  }

  return NO_ERROR;
}

/**
 * Default implementation of evaluate_rules. Just matches
 * the incoming message against all rules and executes all
 * matching rules.
 */
int AbstractConsumer::evaluate_rules(evt_t msg) {
  if (rule_engine) {
    std::vector<uint32_t> rules_to_execute = rule_engine->evaluate_conditions(msg);
    return rule_engine->run_actions(rules_to_execute, msg);
  }
  return NO_ERROR;
}
