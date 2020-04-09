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

#include <ctime>
#include <chrono>

#include "action.h"

/*------------------------------
 * Helper functions
 *------------------------------*/

/**
 * Inserts a new record for the state of the specified
 * rule into the database.
 */
int insert_state(OdbcWrapper *db_connection, std::string rule_id,
    std::string state, std::string target) {
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
    std::string state, std::string target) {
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
    std::string rule_id, std::string target) {
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
