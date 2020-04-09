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
//#include "provd-client.h"

StdoutCaptureAction::StdoutCaptureAction(std::string action) {
  // check that the action has the right syntax
  if (!std::regex_match(action, CAPTURESOUT_SYNTAX)) {
    LOG_ERROR( "StdoutCaptureAction " << action << " is not specified correctly.");
    throw std::invalid_argument(action + " not specified correctly.");
  }

  // parse action
  size_t match_pos = action.find("MATCH", 0);
  size_t fields_pos = action.find("FIELDS", match_pos);
  matching_string = action.substr(match_pos + 6, fields_pos - (match_pos + 5 + 2));
  matching_regex = std::regex(matching_string);

  size_t delim_pos = action.find("DELIM", fields_pos);
  std::string fields_string = action.substr(fields_pos + 7, delim_pos - (fields_pos + 6 + 2));
  std::stringstream ss(fields_string);
  std::string field;
  while (getline(ss, field, ',')) {
    try {
      fields.push_back(new LogLoadField(field));
    } catch (std::invalid_argument &e) {
      LOG_ERROR("Problems while parsing LogLoad field " << field << ". "
          << "Reason invalid argument to " << e.what() << ". "
          << "Field will not be added to the LogLoad fields.");
    }
  }

  size_t into_pos = action.find("INTO", delim_pos);
  delimiter = action.substr(delim_pos + 6, into_pos - (delim_pos + 5 + 2));

  size_t using_pos = action.find("USING", into_pos);
  std::string connection_string = action.substr(into_pos + 4 + 1,
      using_pos - (into_pos + 4 + 2));
  size_t at_pos = connection_string.find("@", 0);
  std::string user_password = connection_string.substr(0, at_pos);
  size_t colon_pos = user_password.find(":");
  target_db.username = user_password.substr(0, colon_pos);
  target_db.password = user_password.substr(colon_pos + 1, user_password.length());

  std::string server_table = connection_string.substr(at_pos + 1, connection_string.length());
  size_t slash_pos = server_table.find("/");
  target_db.hostname = server_table.substr(0, slash_pos);
  target_db.tablename = server_table.substr(slash_pos + 1, server_table.length());
  target_db.db_schema = action.substr(using_pos + 6, action.length() - (using_pos + 6));

  // set up output stream
  out = new DBOutputStream(DEFAULT_DSN, target_db.username, target_db.password,
      target_db.db_schema, target_db.tablename, false);
  out->open();
}

int StdoutCaptureAction::execute(std::shared_ptr<IntermediateMessage> msg) {
  // retrieve name and pid of node and process to trace
  std::string node_name = msg->get_value("nodeName");
  std::string pid_str = msg->get_value("pid");
  LOG_DEBUG("Executing stdout capture action for " << node_name << ":" << pid_str);

  // send trace request to provd daemon on target node
  uint32_t pid = atoi(pid_str.c_str());
//  ProvdClient client;
//  if ((err = client.connectToServer(nodeName)) < 0) {
//    LOG_ERROR("Couldn't connect to provd server on " << nodeName);
//    return ERROR_NO_RETRY;
//  }
//  if ((err = client.submitTraceProcRequest(pid, matchingString)) < 0) {
//    LOG_ERROR("Couldn't submit trace proc request.");
//    return ERROR_NO_RETRY;
//  }

  // receive matching lines from target node until process finishes
  std::vector<std::string> records;
  bool found_entries = false;
  std::string line;
  uint32_t line_counter = 0;
//  while (client.receiveLine(&line) > 0) {
//    LOG_DEBUG("Received matching line " << line);
//    foundEntries = true;
//    // we found a matching line, extract the information
//    std::string record = extractRecordFromLine(line, delimiter, fields, msg);
//    if (record.size() > 0) {
//      records.push_back(record);
//      lineCounter++;
//    }
//  }
  LOG_DEBUG("StdoutCapture Action done, received " << line_counter << " lines.");

  // finish up
//  if ((err = client.disconnectFromServer()) < 0) {
//    LOG_ERROR("Problems while disconnecting from provd server " << err);
//    return ERROR_NO_RETRY;
//  }
  if (found_entries) {
    if (out->send_batch(records) != NO_ERROR) {
      LOG_ERROR("Problems while bulk loading data from  into DB."
          << " Provenance may be incomplete. Action: " << this->str());
    }
  }

  return NO_ERROR;
}

std::string StdoutCaptureAction::str() const {
  return "CAPTURESOUT";
}

std::string StdoutCaptureAction::get_type() const {
  return CAPTURESOUT_RULE;
}
