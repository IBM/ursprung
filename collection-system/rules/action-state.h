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

#ifndef RULES_ACTION_STATE_H_
#define RULES_ACTION_STATE_H_

#include <memory>
#include <string>
#include <fstream>

#include "db-connector.h"

/**
 * An action state backend manages operations on the state for an action, e.g.
 * retrieving existing state from the backend, or deleting and updating state
 * in the backend. Backend implementations should inherit from this base class.
 */
class ActionStateBackend {
public:
  ActionStateBackend() {};
  virtual ~ActionStateBackend() {};

  virtual int connect() = 0;
  virtual int disconnect() = 0;
  /**
   * Inserts a new record for the state of the specified
   * rule into the backend store.
   */
  virtual int insert_state(std::string rule_id, std::string state, std::string target = "") = 0;
  /**
   * Update the state of the specified rule in the backend store.
   */
  virtual int update_state(std::string rule_id, std::string state, std::string target = "") = 0;
  /**
   * Read back the state of an action from the specified
   * rule from the backend store.
   */
  virtual int lookup_state(char *state_buffer, std::string rule_id, std::string target = "") = 0;
};

/**
 * A FileStateBackend manages state in a file. It will manage the
 * state in a file called 'state' in the current directory.
 */
class FileStateBackend: public ActionStateBackend {
private:
  std::ofstream out_file;

public:
  FileStateBackend() {};
  virtual ~FileStateBackend() {}

  virtual int connect() override;
  virtual int disconnect() override;
  virtual int insert_state(std::string rule_id, std::string state,
      std::string target = "") override;
  virtual int update_state(std::string rule_id, std::string state,
      std::string target = "") override;
  virtual int lookup_state(char *state_buffer, std::string rule_id,
      std::string target = "") override;
};

/**
 * A DBStateBackend manages state in a database. It requires an ODBC connection
 * to the target database and the expects the following table to be present in
 * the DB schema:
 *
 * CREATE table rulestate(
 *   id varchar(32) not null,
 *   actionname varchar(32),
 *   target varchar(128) not null,
 *   state varchar(64),
 *   primary key(id,target) enforced
 * );
 */
class DBStateBackend: public ActionStateBackend {
private:
  std::string dsn;
  std::string user;
  std::string pw;
  std::unique_ptr<DBConnector> db_conn;

public:
  DBStateBackend(std::string dsn_name, std::string username, std::string password);
  virtual ~DBStateBackend() {}

  virtual int connect() override;
  virtual int disconnect() override;
  virtual int insert_state(std::string rule_id, std::string state,
      std::string target = "") override;
  virtual int update_state(std::string rule_id, std::string state,
      std::string target = "") override;
  virtual int lookup_state(char *state_buffer, std::string rule_id,
      std::string target = "") override;
};

#endif /* RULES_ACTION_STATE_H_ */
