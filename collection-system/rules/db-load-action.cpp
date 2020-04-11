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

#include <regex>

#include "action.h"
#include "db-output-stream.h"

const std::regex DB_LOAD_SYNTAX = std::regex("DBLOAD (.*) INTO "
    "(FILE (.*)|DB (.*):(.*)@(.*)/(.*) USING (.*))");

DBLoadAction::DBLoadAction(std::string action) {
  // check that action has correct syntax
  if (!std::regex_match(action, DB_LOAD_SYNTAX)) {
    LOG_ERROR("DBLoadAction " << action << " is not specified correctly.");
    throw std::invalid_argument(action + " not specified correctly.");
  }

  // parse action and extract different fields
  size_t into_pos = action.find("INTO", 0);
  event_field = action.substr(DB_LOAD_RULE.length() + 1, into_pos - (DB_LOAD_RULE.length() + 2));

  // parse output destination and set up stream
  if (init_output_stream(action, into_pos) != NO_ERROR) {
    throw std::invalid_argument(action + " not specified correctly.");
  }
}

int DBLoadAction::execute(std::shared_ptr<IntermediateMessage> msg) {
  LOG_DEBUG("Executing DBLoadAction " << this->str());

  std::string path = msg->get_value(event_field);
  if (path.empty()) {
    LOG_ERROR("Event field " << event_field << " was empty. Not executing action " << this->str());
    return ERROR_NO_RETRY;
  }

  std::vector<std::string> records;
  // TODO make sure to add info and remove header
  std::ifstream in_file(path);
  std::string line;
  while (std::getline(in_file, line)) {
    records.push_back(line);
  }
  int rc = out->send_batch(records);
  if (rc != NO_ERROR) {
    LOG_ERROR("Problems while bulk loading data from " << path << " into DB."
        << " Provenance may be incomplete. Action: " << this->str());
  }
  return rc;
}

std::string DBLoadAction::str() const {
  return "DBLOAD " + event_field + " INTO " + out->str();
}

std::string DBLoadAction::get_type() const {
  return DB_LOAD_RULE;
}
