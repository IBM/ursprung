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

#include <string>

#include "auditd-consumer.h"
#include "logger.h"
#include "provd-client.h"

int AuditdConsumer::receive_event(ConsumerSource csrc, evt_t event) {
  // nothing specific to do for the auditd consumer
  return NO_ERROR;
}

int AuditdConsumer::evaluate_rules(evt_t msg) {
  if (rule_engine && rule_engine->has_rules()) {
    // Check if we got an exit event and then see, if we're tracing the exited process
    if (msg->get_type() == SYSCALL_EVENT) {
      if (msg->get_value("syscallName") == "exit_group") {
        std::string node_name = msg->get_value("nodeName");
        std::string pid_str = msg->get_value("pid");
        std::string tracee = pid_str + node_name;

        LOG_DEBUG("Received exit syscall for " << tracee);
        if (active_tracees.find(tracee) != active_tracees.end()) {
          // we're tracing the process that just exited, signal the tracer
          int err;
          ProvdClient client;
          if ((err = client.connect_to_server(node_name)) < 0) return ERROR_NO_RETRY;
          if ((err = client.submit_stop_trace_proc_request(
              (uint32_t) std::stoi(pid_str))) < 0) return ERROR_NO_RETRY;
          if ((err = client.disconnect_from_server()) < 0) return ERROR_NO_RETRY;
          // remove tracee from list of active tracees
          active_tracees.erase(tracee);
          LOG_DEBUG("Removed " << tracee << " from active tracees.");
        }
      }
    }

    // Check if we need to perform any actions based on the received event
    std::vector<uint32_t> rule_ids = rule_engine->evaluate_conditions(msg);
    for (uint32_t rule_id : rule_ids) {
      std::vector<std::string> action_types = rule_engine->get_action_types(rule_id);
      for (auto action_type : action_types) {
        if (action_type == CAPTURESOUT_RULE) {
          // store the process in the list of active tracees so we can
          // check for subsequent exit events
          std::string nodeName = msg->get_value("nodeName");
          std::string pidStr = msg->get_value("pid");
          active_tracees.insert(pidStr + nodeName);
          LOG_DEBUG("Inserted " << (pidStr + nodeName) << " into active tracees");
          break;
        }
      }
    }
    return rule_engine->run_actions(rule_ids, msg);
  }

  return NO_ERROR;
}
