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

#include <iostream>
#include <fstream>
#include <sstream>
#include <regex>
#include <exception>
#include <ctime>
#include <iomanip>
#include <cstdlib>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include "action.h"
#include "odbc-wrapper.h"
#include "logger.h"
#include "db-output-stream.h"
//#include "provd-client.h"

// 4KB buffer size to store log file lines (we assume that 4K
// is enough to hold the longest line in any log file.
#define MAX_LINE_LENGTH 4096

// regex constants for each rule
const std::regex DB_LOAD_SYNTAX = std::regex("DBLOAD [a-zA-Z]* INTO "
    "(.*):(.*)@(.*):[0-9]*/(.*) USING (.*)");
const std::regex DB_TRANSFER_SYNTAX = std::regex("DBTRANSFER (.*)/[a-zA-Z0-9]* FROMDSN "
    "[a-zA-Z0-9]* TO (.*):(.*)@(.*):[0-9]*/(.*) USING (.*)");
const std::regex LOG_LOAD_SYNTAX = std::regex("LOGLOAD [a-zA-Z0-9]* MATCH (.)* FIELDS "
    "(.)* DELIM (.*) INTO (.*):(.*)@(.*):[0-9]*/(.*) USING (.*)");
const std::regex TRACK_SYNTAX = std::regex("TRACK (.*) INTO (.*):(.*)@(.*):[0-9]*");
const std::regex CAPTURESOUT_SYNTAX = std::regex("CAPTURESOUT MATCH (.)* FIELDS "
    "(.)* DELIM (.*) INTO (.*):(.*)@(.*):[0-9]*/(.*) USING (.*)");

// string constants
// TODO make configurable
const std::string REPO_LOCATION = "/opt/ibm/metaocean/contenttracking";
const std::string DATE_FORMAT = "%Y-%m-%d %H:%M:%S";
const std::string DEFAULT_DSN = "ursprung-dsn";

/*------------------------------
 * Helper functions
 *------------------------------*/

/**
 * Inserts a new record for the state of the specified
 * rule into the database.
 */
int insert_state(OdbcWrapper *db_connection, std::string rule_id,
    std::string state, std::string target = "") {
  // TODO either store prepared query as const or use actual prepared query
  std::string prepared_query = "INSERT INTO rulestate (id,target,state) values ('"
      + rule_id + "','" + target + "','" + state + "')";
  if (db_connection->submit_query(prepared_query) != ODBC_SUCCESS) {
    LOG_ERROR("Error while inserting new state: state " << state
        << ", rule " << rule_id << ", target " << target);
    return ERROR_NO_RETRY;
  }
  return NO_ERROR;
}

/**
 * Update the state of the specified rule in the database.
 */
int update_state(OdbcWrapper *db_connection, std::string rule_id,
    std::string state, std::string target = "") {
  // TODO either store prepared query as const or use actual prepared query
  std::string prepared_query = "UPDATE rulestate SET state='" + state
      + "' WHERE id='" + rule_id + "' AND target='" + target + "'";
  if (db_connection->submit_query(prepared_query) != ODBC_SUCCESS) {
    LOG_ERROR("Error while updating state: state " << state
        << ", rule " << rule_id << ", target " << target);
    return ERROR_NO_RETRY;
  }
  return NO_ERROR;
}

/**
 * Read back the state of an action from the specified
 * rule from the database.
 */
int lookup_state(OdbcWrapper *db_connection, char *state_buffer,
    std::string rule_id, std::string target = "") {
  // TODO either store prepared query as const or use actual prepared query
  std::string prepared_query = "SELECT state FROM rulestate WHERE id='" + rule_id
      + "'" + " AND target='" + target + "'";
  if (db_connection->submit_query(prepared_query) != ODBC_SUCCESS) {
    LOG_ERROR("Error while retrieving state from DB: rule " << rule_id << ", target "
        << target << ". Can't retrieve existing state.");
    return ERROR_NO_RETRY;
  }

  // we have existing state
  odbc_rc odbcRc;
  std::string row;
  odbcRc = db_connection->get_row(state_buffer);
  if (odbcRc == ODBC_SUCCESS)
    return NO_ERROR;
  else if (odbcRc == ODBC_NO_DATA)
    return ERROR_EOF;
  else
    return ERROR_NO_RETRY;
}

/**
 * Convert a date field by adding the specified time offset.
 *
 * TODO make data format configurable
 * At the moment we're assuming a fixed date format
 * of 'YYYY-mm-dd HH:MM:SS'.
 */
std::string convert_date_field(std::string date, LogLoadField *field) {
  // convert string to struct tm
  struct tm tm;
  strptime(date.c_str(), DATE_FORMAT.c_str(), &tm);
  // check if we moved to a new day
  if (tm.tm_hour + field->get_timeoffset() >= 24) {
    // TODO Make conversion more robust (using, e.g., external library or <ctime> tools)
    // Currently, this doesn't work for many corner cases, e.g. months breaks, leap years, etc.
    tm.tm_mday = tm.tm_mday + 1;
  }
  tm.tm_hour = (tm.tm_hour + field->get_timeoffset()) % 24;
  // convert new date back to string
  char new_date[32];
  strftime(new_date, sizeof(new_date), "%Y-%m-%d %H:%M:%S", &tm);
  return std::string(new_date);
}

/**
 * Parse the return code of an hg command.
 */
int parse_hg_return_code(hg_handle *handle, std::string &out, std::string &err) {
  hg_header *header;
  char buff[4096];
  int n;
  int exitcode = 0;
  int exit = 0;

  while (!exit) {
    header = hg_read_header(handle);
    switch (header->channel) {
    case o:
      while ((n = hg_rawread(handle, buff, 4096)), n > 0) {
        out = out + std::string(buff);
      }
      break;
    case e:
      while ((n = hg_rawread(handle, buff, 4096)), n > 0) {
        err = err + std::string(buff);
      }
      break;
    case I:
    case L:
      break;
    case r:
      exitcode = hg_exitcode(handle);
      exit = 1;
      break;
    case unknown_channel:
      break;
    }
  }

  return exitcode;
}

/**
 * Extract a record from a line based on a specified
 * delimiter and a set of LogLoadFields.
 */
std::string extract_record_from_line(std::string line, std::string delimiter,
    std::vector<LogLoadField*> fields, std::shared_ptr<IntermediateMessage> msg) {
  size_t pos = 0;
  std::stringstream record;
  std::string token;
  std::vector<std::string> tokens;

  while ((pos = line.find(delimiter)) != std::string::npos) {
    token = line.substr(0, pos);
    tokens.push_back(token);
    line.erase(0, pos + delimiter.length());
  }
  tokens.push_back(line);

  // TODO add range check on fieldIDs and tokens
  for (unsigned int k = 0; k < fields.size(); k++) {
    LogLoadField *field = fields.at(k);
    if (field->is_range_field()) {
      std::string field_value;
      int parse_until = field->get_until_field_id() == -1 ?
              tokens.size() - 1 : field->get_until_field_id();

      for (int j = field->get_field_id(); j <= parse_until; j++) {
        field_value += tokens.at(j) + ((j == parse_until) ? "" : " ");
      }

      if (field->is_timestamp_field()) {
        record << convert_date_field(field_value, field);
      } else {
        record << field_value;
      }
      record << ((k == fields.size() - 1) ? "" : ",");
    } else if (field->is_event_field()) {
      std::string event_field_val = msg->get_value(field->get_field_name());
      record << event_field_val << ((k == fields.size() - 1) ? "" : ",");
    } else if (field->is_composite_field()) {
      std::vector<int> ids = field->get_field_ids();
      for (unsigned int j = 0; j < ids.size(); j++) {
        record << tokens.at(ids.at(j));
      }
      record << ",";
    } else {
      if (field->is_timestamp_field()) {
        record << convert_date_field(tokens.at(field->get_field_id()), field)
            << ((k == fields.size() - 1) ? "" : ",");
      } else {
        record << tokens.at(field->get_field_id())
            << ((k == fields.size() - 1) ? "" : ",");
      }
    }
  }
  record << std::endl;

  return record.str();
}

/*------------------------------
 * Action
 *------------------------------*/

std::shared_ptr<Action> Action::parse_action(std::string action) {
  try {
    if (action.substr(0, 6) == DB_LOAD_RULE) {
      return std::make_shared<DBLoadAction>(action);
    } else if (action.substr(0, 10) == DB_TRANSFER_RULE) {
      return std::make_shared<DBTransferAction>(action);
    } else if (action.substr(0, 7) == LOG_LOAD_RULE) {
      return std::make_shared<LogLoadAction>(action);
    } else if (action.substr(0, 5) == TRACK_RULE) {
      return std::make_shared<TrackAction>(action);
    } else if (action.substr(0, 11) == CAPTURESOUT_RULE) {
      return std::make_shared<StdoutCaptureAction>(action);
    } else {
      LOG_WARN("No action matched for provided action " << action);
      return nullptr;
    }
  } catch (const std::invalid_argument &e) {
    throw;
  } catch (const DBConnectionException &e) {
    throw;
  }
}

void Action::run_consumer() {
  while (running) {
    LOG_DEBUG(rule_id << " - waiting for action to consume");
    std::shared_ptr<IntermediateMessage> msg = action_queue->pop();
    if (msg) {
      LOG_DEBUG(rule_id << " - Received new message, executing action");

#ifdef PERF
      std::string val = msg->get_value("eventTime");
      LOG_DEBUG("Got event time " << val);

      // extract milliseconds from time
      size_t dotPos = val.find(".", 0);
      std::string timeNoMillis = val.substr(0, dotPos);
      int millis = std::stoi(val.substr(dotPos + 1, 3));

      // convert to timestamp
      std::tm t = { 0 };
      std::istringstream ss(timeNoMillis);
      ss >> std::get_time(&t, "%Y-%m-%d %H:%M:%S");
      time_t timestamp = std::mktime(&t);
      long timestampMillis = timestamp * 1000 + millis;

      // get current timestamp
      long ms = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count();
      LOG_DEBUG("Got current time " << ms);

      // compute difference and log it
      long lat = ms - timestampMillis;
      LOG_PERF("Rulelatency: " << lat);
#endif

      execute(std::move(msg));
    }
  }
  LOG_INFO(rule_id << " - finished");
}

void Action::start_action_consumers(int num_threads) {
  LOG_INFO(rule_id << " - starting action consumer");
  for (int i = 0; i < num_threads; i++) {
    consumer_threads.push_back(std::thread(&Action::run_consumer, this));
  }
}

void Action::stop_action_consumers() {
  running = false;
  // push a nullptr for each active thread to unblock pop()
  for (unsigned int i = 0; i < consumer_threads.size(); i++) {
    action_queue->push(std::unique_ptr<IntermediateMessage>(nullptr));
  }
  for (std::thread &t : consumer_threads) {
    t.join();
  }
}

Action::Action() :
    action_queue(new SynchronizedQueue<std::shared_ptr<IntermediateMessage>>()),
    running(true),
    out() {}

Action::~Action() {
  if (action_queue)
    delete action_queue;
  if (out) {
    out->flush();
    delete out;
  }
}

/*------------------------------
 * DBLoadAction
 *------------------------------*/

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
    LOG_ERROR("Event field " >> event_field << " was empty. Not executing action " >> this->str());
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

/*------------------------------
 * DBTransferAction
 *------------------------------*/

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

  size_t to_pos = action.find("TO", 0);
  odbc_dsn = action.substr(from_dsn_pos + 7 + 1, to_pos - (from_dsn_pos + 7 + 2));

  size_t using_pos = action.find("USING", 0);
  std::string connection_string = action.substr(to_pos + 2 + 1, using_pos - (to_pos + 2 + 2));
  size_t at_pos = connection_string.find("@", 0);

  std::string user_password = connection_string.substr(0, at_pos);
  size_t colon_pos = user_password.find(":");
  target_db.username = user_password.substr(0, colon_pos);
  target_db.password = user_password.substr(colon_pos + 1, user_password.length());

  std::string server_table = connection_string.substr(at_pos + 1, connection_string.length());
  slash_pos = server_table.find("/");
  target_db.hostname = server_table.substr(0, slash_pos);
  target_db.tablename = server_table.substr(slash_pos + 1, server_table.length());
  target_db.db_schema = action.substr(using_pos + 6, action.length() - (using_pos + 6));

  // set up ODBC connections and output stream
  source_db_wrapper = new OdbcWrapper(odbc_dsn, "", "");
  target_db_wrapper = new OdbcWrapper(DEFAULT_DSN, target_db.username, target_db.password);
  if (source_db_wrapper->connect() != ODBC_SUCCESS) {
    LOG_ERROR("Error while connecting to source DB " << odbc_dsn);
    throw DBConnectionException();
  }

  out = new DBOutputStream(DEFAULT_DSN, target_db.username, target_db.password,
      target_db.db_schema, target_db.tablename, false);
  out->open();
}

DBTransferAction::~DBTransferAction() {
  // destructor handles disconnection
  delete source_db_wrapper;
}

int DBTransferAction::execute(std::shared_ptr<IntermediateMessage> msg) {
  LOG_DEBUG("Executing DBTransferAction " << this->str());

  int rc = NO_ERROR;
  char row_buffer[1024];
  bool first_row = true;
  std::string row;
  std::string prepared_query = query;

  // restore any existing state
  if (query_state.empty()) {
    // check if there has been any state saved
    char state_buffer[1024];
    rc = lookup_state(target_db_wrapper, state_buffer, rule_id);
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
        rc = insert_state(target_db_wrapper, rule_id, query_state);
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

  // extract rows and write to tmp file
  std::vector<std::string> records;
  bool streamEmpty = true;
  while (source_db_wrapper->get_row(row_buffer) == ODBC_SUCCESS) {
    row = std::string(row_buffer);
    if (first_row) {
      std::stringstream ss(row);
      std::string item;
      getline(ss, item, ',');
      // TODO correctly synchronize on queryState once action handling is multi-threaded
      query_state = item;
      rc = update_state(target_db_wrapper, rule_id, query_state);
      if (rc != NO_ERROR) {
        LOG_ERROR("Problems while updating state for rule " << this->str()
            << ". State can't be backed up at the moment.");
      }
      first_row = false;
    }
    records.push_back(row);
    if (streamEmpty) {
      streamEmpty = false;
    }
  }

  // It is possible, that we triggered a DBTransfer action but didn't find
  // any new data (e.g. when the action is triggered by a WRITE to a DB file
  // but when the WRITE is received, the data hasn't actually been flushed to
  // disk). Hence, we check here whether we have any data to send or not.
  if (!streamEmpty) {
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
  return "DBTRANSFER " + query + " FROM " + odbc_dsn + " TO "
      + target_db.username + "@" + target_db.hostname + "/"
      + target_db.tablename + " USING " + target_db.db_schema;
}

std::string DBTransferAction::get_type() const {
  return DB_TRANSFER_RULE;
}

/*------------------------------
 * LogLoadAction
 *------------------------------*/

LogLoadAction::LogLoadAction(std::string action) {
  // check that the action has the right syntax
  if (!std::regex_match(action, LOG_LOAD_SYNTAX)) {
    LOG_ERROR("LogLoadAction " << action << " is not specified correctly.");
    throw std::invalid_argument(action + " not specified correctly.");
  }

  // parse action
  size_t match_pos = action.find("MATCH", 0);
  event_field = action.substr(LOG_LOAD_RULE.length() + 1,
      match_pos - (LOG_LOAD_RULE.length() + 2));

  size_t fields_pos = action.find("FIELDS", match_pos);
  matching_string = std::regex("(.*)" + action.substr(
      match_pos + 6, fields_pos - (match_pos + 5 + 2)) + "(.*)");

  size_t delim_pos = action.find("DELIM", fields_pos);
  std::string fields_string = action.substr(fields_pos + 7, delim_pos - (fields_pos + 6 + 2));
  std::stringstream ss(fields_string);
  std::string field;
  while (getline(ss, field, ',')) {
    try {
      fields.push_back(new LogLoadField(field));
    } catch (std::invalid_argument &e) {
      LOG_ERROR("Problems while parsing LogLoad field " << field
          << ". Reason invalid argument to " << e.what()
          << ". Field will not be added to the LogLoad fields.");
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

  // set up ODBC connections and output stream
  target_db_wrapper = new OdbcWrapper(DEFAULT_DSN, target_db.username, target_db.password);
  if (target_db_wrapper->connect() != ODBC_SUCCESS) {
    LOG_ERROR("Error while connecting to target DB " << DEFAULT_DSN);
    throw DBConnectionException();
  }

  out = new DBOutputStream(DEFAULT_DSN, target_db.username, target_db.password,
      target_db.db_schema, target_db.tablename, false);
  out->open();
}

LogLoadAction::~LogLoadAction() {
  for (unsigned int i = 0; i < fields.size(); i++) {
    delete fields.at(i);
  }
  // destructor handles disconnection
  delete target_db_wrapper;
}

int LogLoadAction::execute(std::shared_ptr<IntermediateMessage> msg) {
  LOG_DEBUG("Executing LogLoadAction " << this->str());
  int rc = NO_ERROR;

  // get inode of file to deal with log rollover
  std::string path = msg->get_value(event_field);
  struct stat sb;
  if (stat(path.c_str(), &sb) < 0) {
    LOG_ERROR("stat() failed with " << strerror(errno) << "."
        << " Can't retrieve inode for " << path << ". "
        << "Exiting LogLoadAction " << this->str()
        << " for received message " << msg->normalize(CD_DB2));
    return ERROR_NO_RETRY;
  }
  unsigned long long inode = sb.st_ino;

  // retrieve existing state
  std::pair<long long int, unsigned long long> state;
  if (parsing_state.find(path) == parsing_state.end()) {
    // check if we have state stored in the database
    char stateBuffer[1024];
    rc = lookup_state(target_db_wrapper, stateBuffer, rule_id, path);
    if (rc == ERROR_NO_RETRY) {
      LOG_ERROR("Problems while trying to restore state for " << this->str()
          << ". Will start" << " parsing " << path << " from 0.");
      state.first = 0;
      state.second = inode;
      parsing_state[path] = state;
      // try to add state to DB
      insert_state(target_db_wrapper, rule_id, std::to_string(parsing_state[path].first) + ","
          + std::to_string(parsing_state[path].second), path);
    } else {
      std::string existing_state = (rc == NO_ERROR) ? std::string(stateBuffer) : "";
      if (!existing_state.empty()) {
        // we had state for this value in the database
        // the format of the retrieved state is 'offset,inode'
        size_t pos = existing_state.find(",");
        state.first = std::stoll(existing_state.substr(0, pos));
        state.second = std::stoll(existing_state.substr(pos + 1, std::string::npos));
        parsing_state[path] = state;
        LOG_INFO("LogLoadAction " << this->str() << ": restored map state");
      } else {
        // we haven't seen this value before so initialize it in the map and the database
        state.first = 0;
        state.second = inode;
        parsing_state[path] = state;
        rc = insert_state(target_db_wrapper, rule_id, std::to_string(parsing_state[path].first)
            + "," + std::to_string(parsing_state[path].second), path);
        if (rc != NO_ERROR) {
          LOG_ERROR("Problems while adding state for new rule " << this->str()
              << ". State can't be backed up at the moment.");
        } else {
          LOG_INFO("LogLoadAction " << this->str() << ": no existing state found");
        }
      }
    }
  }

  // check if inode has changed
  if (parsing_state[path].second != inode) {
    // if inode has changed due to log rollover (i.e. same path but different inode)
    // reset the parsing state to store the new inode and start parsing from 0 again
    parsing_state[path].first = 0;
    parsing_state[path].second = inode;
    // TODO could there still be unread data in the old file?
  }

  // open log file and seek to last parsed position
  std::ifstream infile(path);
  infile.seekg(parsing_state[path].first);

  // We are reading the file in batches of 4KB and extracting data line by
  // line. If we're at the end of the file and we have not ended the current
  // line, we store the partial line in lineFragment. On the next action trigger,
  // we read until the end of the previously stored partial line and combine it
  // with the lineFragment to restore the full line.
  //
  // NB: This code is not yet able to combine lines *across* read calls and hence,
  // we assume that no line exceeds 4KB, i.e. we can always read at least one line break
  // per read call.
  char buffer[MAX_LINE_LENGTH];
  std::vector<std::string> records;
  std::string line;
  bool found_entries = false;
  bool read = true;

  while (read) {
    infile.read(buffer, MAX_LINE_LENGTH);
    size_t bytes_read = infile.gcount();

    // error handling
    if (infile.eof()) {
      read = false;
    }
    if (infile.bad()) {
      LOG_ERROR("Error while reading, bad stream. LogLoad not reading from " << path);
      read = false;
    }

    int lineoffset = 0;
    for (uint i = 0; i < bytes_read; i++) {
      if (buffer[i] == '\n') {
        // read one line
        int line_length = i - lineoffset;
        if (line_overflow > 0) {
          line_length += line_overflow;
        }

        std::vector<char> line(line_length);
        if (line_overflow > 0) {
          LOG_DEBUG("Appending previous line fragment to currently read line.");
          for (unsigned j = 0; j < line_fragment.size(); j++) {
            line[j] = line_fragment[j];
          }
          for (unsigned j = lineoffset; j < i; j++) {
            line[j - lineoffset + line_overflow] = buffer[j];
          }
          line_overflow = 0;
        } else {
          // insert inserts from [lineoffset, i), i.e. buffer[i] is *excluded*
          for (unsigned j = lineoffset; j < i; j++) {
            line[j - lineoffset] = buffer[j];
          }
        }

        lineoffset = i + 1;
        std::string line_str(line.begin(), line.end());
        LOG_DEBUG("Read line '" << line_str << "'");

        // match the line to find possible records to extract
        if (std::regex_match(line_str, matching_string)) {
          std::string record = extract_record_from_line(line_str, delimiter, fields, msg);
          records.push_back(record);
          found_entries = true;
        }
      }
      if (i == bytes_read - 1 && buffer[i] != '\n') {
        // we're not ending with a new line
        // store everything from the last line break in a buffer
        line_fragment.resize(bytes_read - lineoffset);
        for (unsigned j = lineoffset; j < bytes_read; j++) {
          line_fragment[j - lineoffset] = buffer[j];
        }
        line_overflow = bytes_read - lineoffset;
        std::string line_fragment_str(line_fragment.begin(), line_fragment.end());
        LOG_DEBUG("Read data doesn't end with new line, storing broken line '"
            << line_fragment_str << "' with line overflow " << line_overflow);
      }
    }

    parsing_state[path].first = parsing_state[path].first + bytes_read;
  }
  // TODO this needs to be synchronized if we make action handling multi-threaded
  rc = update_state(target_db_wrapper, rule_id, std::to_string(parsing_state[path].first) + ","
      + std::to_string(parsing_state[path].second), path);
  if (rc != NO_ERROR) {
    LOG_ERROR("Problems while updating state for rule " << this->str()
        << ". State can't be backed up at the moment.");
  }
  infile.close();

  if (found_entries) {
    rc = out->send_batch(records);
    if (rc != NO_ERROR) {
      LOG_ERROR("Problems while bulk loading data into DB."
          << " Provenance may be incomplete. Action: " << this->str());
    }
  }

  return rc;
}

std::string LogLoadAction::str() const {
  // convert the fields vector to a string
  std::stringstream ss;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (i != 0) {
      ss << ",";
    }
    ss << fields[i];
  }

  return "LOGLOAD " + event_field + " MATCH " + " FIELDS " + ss.str() + " DELIM "
      + delimiter + " INTO " + target_db.username + "@" + target_db.hostname
      + "/" + target_db.tablename + " USING " + target_db.db_schema;
}

std::string LogLoadAction::get_type() const {
  return LOG_LOAD_RULE;
}

/*------------------------------
 * TrackAction
 *------------------------------*/

TrackAction::TrackAction(std::string action) {
  // check that the action has the right syntax
  if (!std::regex_match(action, TRACK_SYNTAX)) {
    LOG_ERROR("TrackAction " << action << " is not specified correctly.");
    throw std::invalid_argument(action + " not specified correctly.");
  }

  // parse action
  size_t into_pos = action.find("INTO", 0);
  path_regex = std::regex(action.substr(TRACK_RULE.length() + 1,
      into_pos - (TRACK_RULE.length() + 2)));
  path_regex_str = action.substr(TRACK_RULE.length() + 1, into_pos - (TRACK_RULE.length() + 2));

  std::string connection_string = action.substr(into_pos + 4 + 1,
      action.length() - (into_pos + 4 + 2));
  size_t at_pos = connection_string.find("@", 0);
  std::string user_password = connection_string.substr(0, at_pos);
  size_t colon_pos = user_password.find(":");
  target_db.username = user_password.substr(0, colon_pos);
  target_db.password = user_password.substr(colon_pos + 1, user_password.length());
  target_db.hostname = connection_string.substr(at_pos + 1, connection_string.length());

  // set up ODBC and repo connections
  target_db_wrapper = new OdbcWrapper(DEFAULT_DSN, target_db.username,
      target_db.password);
  if (target_db_wrapper->connect() != ODBC_SUCCESS) {
    LOG_ERROR("Error while connecting to target DB " << DEFAULT_DSN);
    throw DBConnectionException();
  }

  repo_handle = hg_open(REPO_LOCATION.c_str(), NULL);
}

TrackAction::~TrackAction() {
  // destructor handles disconnection
  delete target_db_wrapper;
  hg_close(&repo_handle);
}

int TrackAction::execute(std::shared_ptr<IntermediateMessage> msg) {
  LOG_DEBUG("Executing TrackAction " << this->str());

  std::string src = msg->get_value("path");
  std::string inode = msg->get_value("inode");
  std::string event = msg->get_value("event");

  if (event == "RENAME") {
    if (failed_cp_state.find(inode) != failed_cp_state.end()) {
      // get the new name of the renamed file so we can copy it correctly
      src = msg->get_value("dstPath");
      // clean up the entry in the map
      failed_cp_state.erase(inode);
    } else {
      // if we have a RENAME event without state, don't process it and just exit
      return NO_ERROR;
    }
  } else if (event == "UNLINK") {
    if (failed_cp_state.find(inode) != failed_cp_state.end()) {
      // clean up the entry in the map and exit
      failed_cp_state.erase(inode);
    }
    return NO_ERROR;
  }

  // TODO can we improve this and replace the system() call?
  std::string cmd = "cp " + src + " " + REPO_LOCATION + "/" + inode;

  // FIXME Without stat'ing the src file, it can happen that there's an error during copying:
  // "cp: skipping file ‘/path/to/example’, as it was replaced while being copied"
  // The error indicates a concurrent modification while cp is running (either inode
  // or device changes during copy) and is most likely due to some NFS behavior as
  // we are currently mounting GPFS on the metaocean VM through NFS.
  //
  // It is unclear whether the stat just delays the cp long enough for NFS to get back
  // in sync or actually updates the file handle but it seems to help. The exact
  // reason for this behavior needs to be verified to make this robust.
  struct stat statbuf_pre;
  stat(src.c_str(), &statbuf_pre);
  if (system(cmd.c_str()) != 0) {
    LOG_ERROR("Problems while copying file: " << strerror(errno));
    // store the state of the failed inode
    failed_cp_state[inode] = true;
    return ERROR_NO_RETRY;
  }


  // run 'hg add .'
  std::string std_out;
  std::string std_err;
  const char *add[] = { "add", ".", "--cwd", REPO_LOCATION.c_str(), NULL };
  hg_rawcommand(repo_handle, (char**) add);
  if (parse_hg_return_code(repo_handle, std_out, std_err) != 0) {
    LOG_ERROR("Problems while running hg add: " << std_err
        << ": Not tracking current version of " << src);
    return ERROR_NO_RETRY;
  }

  // run 'hg commit'
  std_out = "";
  std_err = "";
  const char *commit[] = { "commit", "-m", "commit", "--cwd", REPO_LOCATION.c_str(), NULL };
  hg_rawcommand(repo_handle, (char**) commit);
  if (parse_hg_return_code(repo_handle, std_out, std_err) != 0) {
    LOG_ERROR("Problems while running hg commit: " << std_err
        << ": Not tracking current version of " << src);
    return ERROR_NO_RETRY;
  }

  // get the commit ID from the last commit
  std_out = "";
  std_err = "";
  const char *identify[] = { "--debug", "identify", "-i", "--cwd", REPO_LOCATION.c_str(), NULL };
  hg_rawcommand(repo_handle, (char**) identify);
  if (parse_hg_return_code(repo_handle, std_out, std_err) != 0) {
    LOG_ERROR("Problems while running hg identify: " << std_err
        << ": Won't add commit record to database for " << src);
    return ERROR_NO_RETRY;
  }

  // insert commit record into database
  std::string cluster_name = msg->get_value("clusterName");
  std::string node_name = msg->get_value("nodeName");
  std::string fs_name = msg->get_value("fsName");
  std::string event_time = msg->get_value("eventTime");
  std::ostringstream prepared_query;
  prepared_query << "INSERT INTO versions ";
  prepared_query << "(clusterName,nodeName,fsName,path,inode,eventTime,commitId) VALUES (";
  prepared_query << "'" << cluster_name << "',";
  prepared_query << "'" << node_name << "',";
  prepared_query << "'" << fs_name << "',";
  prepared_query << "'" << src << "',";
  prepared_query << "'" << inode << "',";
  prepared_query << "'" << event_time << "',";
  prepared_query << "'" << std_out << "')";
  if (target_db_wrapper->submit_query(prepared_query.str()) != ODBC_SUCCESS) {
    LOG_ERROR("Error while inserting new version " << std_out << " for " << src);
    return ERROR_NO_RETRY;
  }

  return NO_ERROR;
}

std::string TrackAction::str() const {
  return "TRACK " + path_regex_str + " INTO " + target_db.username + "@"
      + target_db.hostname;
}

std::string TrackAction::get_type() const {
  return TRACK_RULE;
}

/*------------------------------
 * TrackAction
 *------------------------------*/

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

/*------------------------------
 * LogLoadField
 *------------------------------*/

LogLoadField::LogLoadField(std::string field) {
  size_t pos = 0;
  size_t slash_pos = std::string::npos;

  // default values
  is_timestamp = false;
  is_event_field_name = false;
  is_range = false;
  is_composite = false;
  timeoffset = 0;
  until_field_id = -1;
  field_id = -1;

  slash_pos = field.find("/");
  if (slash_pos != std::string::npos) {
    // this field represents a timestamp
    is_timestamp = true;
    timeoffset = std::stoi(field.substr(slash_pos + 1));
  }

  try {
    if ((pos = field.find("-")) != std::string::npos) {
      // this is a range field
      field_id = std::stoi(field.substr(0, (pos - 0)));
      std::string until_field_id_str = field.substr(pos + 1, (slash_pos - (pos + 1)));
      if (until_field_id_str == "e") {
        // -1 in a range field indicates that we parse
        // everything until the end of the log line
        until_field_id = -1;
      } else {
        until_field_id = std::stoi(until_field_id_str);
      }
      is_range = true;
    } else if (!field.empty() && !is_timestamp && (pos = field.find("+")) != std::string::npos) {
      // this is a composite field
      std::string plus = "+";
      while ((pos = field.find(plus)) != std::string::npos) {
        field_ids.push_back(std::stoi(field.substr(0, pos)));
        field.erase(0, pos + plus.length());
      }
      field_ids.push_back(std::stoi(field));
      is_composite = true;
    } else if (!field.empty() && !is_timestamp
        && field.find_first_not_of("0123456789") != std::string::npos) {
      // Not a number so we interpret this as an event field name.
      // Note that event field names do currently not support timestamp
      // conversions, i.e. they can't contain a timeoffset.
      field_name = field;
      is_event_field_name = true;
    } else {
      field_id = std::stoi(field.substr(0, (slash_pos - 0)));
    }
  } catch (std::invalid_argument &e) {
    throw;
  }
}
