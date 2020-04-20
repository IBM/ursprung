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

const std::regex CAPTURESOUT_SYNTAX = std::regex("CAPTURESOUT MATCH (.)* FIELDS "
    "(.)* DELIM (.*) INTO (FILE (.*)|DB (.*):(.*)@(.*) USING (.*)/(.*))");

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

  // parse output destination and set up stream
   if (init_output_stream(action, into_pos) != NO_ERROR) {
     throw std::invalid_argument(action + " not specified correctly.");
   }
}

int StdoutCaptureAction::execute(evt_t msg) {
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
  // convert the fields vector to a string
  std::stringstream ss;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (i != 0) {
      ss << ",";
    }
    ss << fields[i]->str();
  }

  return "CAPTURESOUT MATCH " + matching_string + " FIELDS " + ss.str() + " DELIM "
      + delimiter + " INTO " + out->str();
}

std::string StdoutCaptureAction::get_type() const {
  return CAPTURESOUT_RULE;
}
