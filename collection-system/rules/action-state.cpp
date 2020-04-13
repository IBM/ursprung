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

#include "action-state.h"
#include "error.h"
#include "logger.h"

/*------------------------------
 * FileStateBackend
 *------------------------------*/

int FileStateBackend::connect() {
  // TODO implement
  return NO_ERROR;
}

int FileStateBackend::disconnect() {
  // TODO implement
  return NO_ERROR;
}

int FileStateBackend::insert_state(std::string rule_id, std::string state, std::string target) {
  // TODO implement
  return NO_ERROR;
}

int FileStateBackend::update_state(std::string rule_id, std::string state, std::string target) {
  // TODO implement
  return NO_ERROR;
}

int FileStateBackend::lookup_state(char *state_buffer, std::string rule_id, std::string target) {
  // TODO implement
  return NO_ERROR;
}

/*------------------------------
 * DBStateBackend
 *------------------------------*/

DBStateBackend::DBStateBackend(std::string dsn_name, std::string username,
    std::string password) :
    dsn(dsn_name), user(username), pw(password) {
  db_conn = std::make_unique<OdbcConnector>(dsn, user, pw);
}

int DBStateBackend::connect() {
  if (db_conn->connect() != DB_SUCCESS) {
    LOG_ERROR("Error while connecting to source DB " << dsn);
    return ERROR_NO_RETRY;
  }
  return NO_ERROR;
}

int DBStateBackend::disconnect() {
  db_conn->disconnect();
  return NO_ERROR;
}

int DBStateBackend::insert_state(std::string rule_id, std::string state, std::string target) {
  // TODO either store prepared query as const or use actual prepared query
  std::string prepared_query = "INSERT INTO rulestate (id,target,state) values ('"
      + rule_id + "','" + target + "','" + state + "')";
  if (db_conn->submit_query(prepared_query) != DB_SUCCESS) {
    LOG_ERROR("Error while inserting new state: state " << state
        << ", rule " << rule_id << ", target " << target);
    return ERROR_NO_RETRY;
  }
  return NO_ERROR;
}

int DBStateBackend::update_state(std::string rule_id, std::string state, std::string target) {
  // TODO either store prepared query as const or use actual prepared query
  std::string prepared_query = "UPDATE rulestate SET state='" + state
      + "' WHERE id='" + rule_id + "' AND target='" + target + "'";
  if (db_conn->submit_query(prepared_query) != DB_SUCCESS) {
    LOG_ERROR("Error while updating state: state " << state
        << ", rule " << rule_id << ", target " << target);
    return ERROR_NO_RETRY;
  }
  return NO_ERROR;
}

int DBStateBackend::lookup_state(char *state_buffer, std::string rule_id, std::string target) {
  // TODO either store prepared query as const or use actual prepared query
  std::string prepared_query = "SELECT state FROM rulestate WHERE id='" + rule_id
      + "'" + " AND target='" + target + "'";
  if (db_conn->submit_query(prepared_query) != DB_SUCCESS) {
    LOG_ERROR("Error while retrieving state from DB: rule " << rule_id << ", target "
        << target << ". Can't retrieve existing state.");
    return ERROR_NO_RETRY;
  }

  // we have existing state
  std::string row;
  db_rc rc = db_conn->get_row(state_buffer);
  if (rc == DB_SUCCESS)
    return NO_ERROR;
  else if (rc == DB_NO_DATA)
    return ERROR_EOF;
  else
    return ERROR_NO_RETRY;
}
