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

DBLoadAction::DBLoadAction(std::string action) {
  // check that action has correct syntax
  if (!std::regex_match(action, DB_LOAD_SYNTAX)) {
    LOG_ERROR("DBLoadAction " << action << " is not specified correctly.");
    throw std::invalid_argument(action + " not specified correctly.");
  }

  // parse action and extract different fields
  size_t into_pos = action.find("INTO", 0);
  event_field = action.substr(DB_LOAD_RULE.length() + 1, into_pos - (DB_LOAD_RULE.length() + 2));

  size_t using_pos = action.find("USING", 0);
  std::string connection_string = action.substr(into_pos + 4 + 1, using_pos - (into_pos + 4 + 2));
  size_t at_pos = connection_string.find("@", 0);

  std::string user_password = connection_string.substr(0, at_pos);
  size_t colon_pos = user_password.find(":");
  target_db.username = user_password.substr(0, colon_pos);
  target_db.password = user_password.substr(colon_pos + 1, user_password.length());

  std::string server_table = connection_string.substr(at_pos + 1, connection_string.length());
  size_t slash_pos = server_table.find("/");
  target_db.hostname = server_table.substr(0, slash_pos);
  target_db.tablename = server_table.substr(slash_pos + 1, server_table.length());

  size_t header_pos = action.find("HEADER", 0);
  if (header_pos != std::string::npos) {
    header = true;
    target_db.db_schema = action.substr(using_pos + 6, (header_pos - 1) - (using_pos + 6));
  } else {
    header = false;
    target_db.db_schema = action.substr(using_pos + 6, action.length() - (using_pos + 6));
  }

  // set up output stream
  out = new DBOutputStream(DEFAULT_DSN, target_db.username, target_db.password,
      target_db.db_schema, target_db.tablename, false);
  ((DBOutputStream*) out)->set_add_info();
  if (header)
    ((DBOutputStream*) out)->set_header();
  out->open();
}

int DBLoadAction::execute(std::shared_ptr<IntermediateMessage> msg) {
  LOG_DEBUG("Executing DBLoadAction " << this->str());

  std::string path = msg->get_value(event_field);
  if (path.empty()) {
    LOG_ERROR("Event field " << event_field << " was empty. Not executing action " << this->str());
    return ERROR_NO_RETRY;
  }

  std::vector<std::string> records;
  // TODO read all the values from the file at 'path' here and send them
  int rc = out->send_batch(records);
  if (rc != NO_ERROR) {
    LOG_ERROR("Problems while bulk loading data from " << path << " into DB."
        << " Provenance may be incomplete. Action: " << this->str());
  }
  return rc;
}

std::string DBLoadAction::str() const {
  return "DBLOAD " + event_field + " INTO " + target_db.username + ":" + "@" + target_db.hostname
      + "/" + target_db.tablename + " USING " + target_db.db_schema + (header ? " HEADER" : "");
}

std::string DBLoadAction::get_type() const {
  return DB_LOAD_RULE;
}
