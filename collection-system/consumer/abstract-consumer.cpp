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

// signal handling
namespace signal_handling {
static volatile sig_atomic_t running = 1;

void term_handler(int sig) {
  LOG_INFO("Shutting down...");
  running = 0;
}

static void setup_handlers() {
  struct sigaction sa;
  // unblock all signals
  sigfillset(&sa.sa_mask);
  sigprocmask(SIG_UNBLOCK, &sa.sa_mask, 0);
  sa.sa_flags = 0;
  sigemptyset(&sa.sa_mask);
  // by default, set ignore handler for all signals
  sa.sa_handler = SIG_IGN;
  for (int i = 1; i < NSIG; i++)
    sigaction(i, &sa, NULL);
  // now set the handlers for SIGTERM and SIGINT
  sa.sa_handler = term_handler;
  sigaction(SIGTERM, &sa, NULL);
  sa.sa_handler = term_handler;
  sigaction(SIGINT, &sa, NULL);
}
}

AbstractConsumer::AbstractConsumer(ConsumerSource csrc,
    std::unique_ptr<MsgInputStream> in, ConsumerDestination cdst,
    std::unique_ptr<MsgOutputStream> out, uint32_t batchsize) :
    c_src(csrc), in_stream(std::move(in)),
    c_dst(cdst), out_stream(std::move(out)),
    batch_size(batchsize) {
  if (!Config::config[Config::CKEY_RULES_FILE].empty()) {
    rule_engine = std::make_unique<RuleEngine>(Config::config[Config::CKEY_RULES_FILE]);
  }
}

AbstractConsumer::~AbstractConsumer() {
  if (rule_engine) {
    rule_engine->shutdown();
  }
}

int AbstractConsumer::run() {
  signal_handling::setup_handlers();

  std::string next_msg;
  int rc;
  while (signal_handling::running) {
    // build batch
    bool batch_done = false;
    auto batch_start = std::chrono::steady_clock::now();
    while (!batch_done && signal_handling::running) {
      rc = in_stream->recv(next_msg);
      if (rc == NO_ERROR) {
        im_t im = build_intermediate_message(c_src, next_msg);
        msg_buffer.push_back(im);

        // TODO deal with directory renames here
        // find and execute any matching rules
        if (evaluate_rules(im) != NO_ERROR) {
          LOG_ERROR("Problems while executing rules, some provenance " <<
              "might be lost");
        }
      } else if (rc == ERROR_NO_RETRY) {
        signal_handling::running = false;
      } else {
        // log and ignore error
        LOG_WARN("Got error " < rc << " during receive. Continuing.");
      }

      if (msg_buffer.size() > batch_size) {
        batch_done = true;
      } else {
      // check if the batch has timed out and if so, send it
        auto curr_time = std::chrono::steady_clock::now();
        long elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                batch_start - curr_time).count();
        if (elapsed_time >= BATCH_TIMEOUT && msg_buffer.size() > 0) {
          LOG_DEBUG("Batch timed out and will be sent with size " << msg_buffer.size());
          batch_done = true;
        }
      }
    }

    // normalize messages for destination
    std::vector<std::string> normalized_msgs;
    for (im_t im : msg_buffer) {
      normalized_msgs.push_back(im->normalize(c_dst, ","));
    }

    // send messages
    rc = out_stream->send_batch(normalized_msgs);
    if (rc != NO_ERROR) {
      LOG_ERROR("Problems while sending batch. Messages might have been lost.");
      // TODO better error handling
    }
  }

  return NO_ERROR;
}

/**
 * Default implementation of evaluate_rules. Just matches
 * the incoming message against all rules and executes all
 * matching rules.
 */
int AbstractConsumer::evaluate_rules(im_t msg) {
  if (rule_engine) {
    std::vector<uint32_t> rules_to_execute = rule_engine->evaluate_conditions(msg);
    return rule_engine->run_actions(rules_to_execute, msg);
  }
  return NO_ERROR;
}
