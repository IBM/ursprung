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

const std::regex DB_TRANSFER_SYNTAX = std::regex("DBTRANSFER (.*)/[a-zA-Z0-9]* FROMDSN "
    "[a-zA-Z0-9]* TO (.*):(.*)@(.*):[0-9]*/(.*) USING (.*)");

DBTransferAction::DBTransferAction(std::string action) {
  // check that the action has the right syntax
  if (!std::regex_match(action, DB_TRANSFER_SYNTAX)) {
    LOG_ERROR("DBTransferAction " << action << " is not specified correctly.");
    // TODO here we should also enforce the two requirement:
    // 1. attributes need to be specified explicitely in the select clause
    // 2. the first selected attribute has to be the state attribute
    throw std::invalid_argument(action + " not specified correctly.");
  }

  // parse action
  size_t from_dsn_pos = action.find("FROMDSN", 0);
  std::string query_state_field = action.substr(DB_TRANSFER_RULE.length() + 1,
      from_dsn_pos - (DB_TRANSFER_RULE.length() + 2));
  size_t slash_pos = query_state_field.find("/");
  query = query_state_field.substr(0, slash_pos);
  state_attribute_name = query_state_field.substr(slash_pos + 1, query_state_field.length());

  size_t into_pos = action.find("INTO", 0);
  odbc_dsn = action.substr(from_dsn_pos + 7 + 1, into_pos - (from_dsn_pos + 7 + 2));

  // parse output destination and set up stream
  if (init_output_stream(action, into_pos) != NO_ERROR) {
    throw std::invalid_argument(action + " not specified correctly.");
  }

  // initialize action state
  if (init_state(action, into_pos) != NO_ERROR) {
    throw std::invalid_argument(action + " could not create state.");
  }

  // set up ODBC connection to source database
  source_db_wrapper = new OdbcWrapper(odbc_dsn, "", "");
  if (source_db_wrapper->connect() != ODBC_SUCCESS) {
    LOG_ERROR("Error while connecting to source DB " << odbc_dsn);
    throw DBConnectionException();
  }
}

DBTransferAction::~DBTransferAction() {
  // destructor handles disconnection
  delete source_db_wrapper;
}

int DBTransferAction::execute(std::shared_ptr<IntermediateMessage> msg) {
  LOG_DEBUG("Executing DBTransferAction " << this->str());

  // restore any existing state
  int rc;
  if (query_state.empty()) {
    // check if there has been any state saved
    char state_buffer[1024];
    rc = state_backend->lookup_state(state_buffer, rule_id);
    if (rc == ERROR_NO_RETRY) {
      LOG_ERROR("Problems while trying to restore state for " << this->str()
          << ". Will work" << " with existing query state " << query_state);
    } else {
      std::string query_state_string =
          (rc == NO_ERROR) ? std::string(state_buffer) : "";
      if (!query_state_string.empty()) {
        LOG_INFO("DBTransferAction " << this->str() << ": restored "
            << query_state_string << " state from disk.");
        query_state = query_state_string;
      } else {
        // initialize state in the database
        rc = state_backend->insert_state(rule_id, query_state);
        if (rc != NO_ERROR) {
          LOG_ERROR("Problems while adding state for rule " << this->str()
              << ". State can't be backed up at the moment.");
        } else {
          LOG_INFO("DBTransferAction " << this->str() << ": no existing state found");
        }
      }
    }
  }

  // construct query and send to database
  std::string prepared_query = query;
  prepared_query.append(" where ").append(state_attribute_name).append(" is not null ");
  if (!query_state.empty()) {
    // there is state so we have to add the correct WHERE clause to the query
    prepared_query.append(" and ").append(state_attribute_name).append(" > '").append(
        query_state).append("'");
  }
  prepared_query.append(" order by ").append(state_attribute_name).append(" desc");
  if (source_db_wrapper->submit_query(prepared_query) != ODBC_SUCCESS) {
    LOG_ERROR("Error while submitting query to DB. Can't retrieve data from source db."
        << " Provenance may be incomplete. Action: " << this->str());
    return ERROR_NO_RETRY;
  }

  // extract rows
  std::vector<std::string> records;
  std::string row;
  char row_buffer[1024];
  bool stream_empty = true;
  bool first_row = true;
  while (source_db_wrapper->get_row(row_buffer) == ODBC_SUCCESS) {
    row = std::string(row_buffer);
    if (first_row) {
      std::stringstream ss(row);
      std::string item;
      getline(ss, item, ',');
      // TODO correctly synchronize on queryState once action handling is multi-threaded
      query_state = item;
      rc = state_backend->update_state(rule_id, query_state);
      if (rc != NO_ERROR) {
        LOG_ERROR("Problems while updating state for rule " << this->str()
            << ". State can't be backed up at the moment.");
      }
      first_row = false;
    }
    records.push_back(row);
    if (stream_empty) {
      stream_empty = false;
    }
  }

  // It is possible, that we triggered a DBTransfer action but didn't find
  // any new data (e.g. when the action is triggered by a WRITE to a DB file
  // but when the WRITE is received, the data hasn't actually been flushed to
  // disk). Hence, we check here whether we have any data to send or not.
  if (!stream_empty) {
    rc = out->send_batch(records);
    if (rc != NO_ERROR) {
      LOG_ERROR("Problems while adding newly retrieved db data to DB."
          << " Provenance may be incomplete. Action: " << this->str());
    }
  } else {
    LOG_DEBUG("DBTransferAction " << this->str() << " didn't receive any new data.");
  }

  return rc;
}

std::string DBTransferAction::str() const {
  return "DBTRANSFER " + query + " FROM " + odbc_dsn + " INTO " + out->str();
}

std::string DBTransferAction::get_type() const {
  return DB_TRANSFER_RULE;
}
