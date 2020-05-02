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
#include "db-output-stream.h"

/*------------------------------
 * Helper functions
 *------------------------------*/

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

std::unique_ptr<Action> Action::parse_action(std::string action) {
  try {
    if (action.substr(0, 6) == DB_LOAD_RULE) {
      return std::make_unique<DBLoadAction>(action);
    } else if (action.substr(0, 10) == DB_TRANSFER_RULE) {
      return std::make_unique<DBTransferAction>(action);
    } else if (action.substr(0, 7) == LOG_LOAD_RULE) {
      return std::make_unique<LogLoadAction>(action);
    } else if (action.substr(0, 5) == TRACK_RULE) {
      return std::make_unique<TrackAction>(action);
    } else if (action.substr(0, 11) == CAPTURESOUT_RULE) {
      return std::make_unique<StdoutCaptureAction>(action);
    } else {
      LOGGER_LOG_WARN("No action matched for provided action " << action);
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
    LOGGER_LOG_DEBUG(rule_id << " - waiting for action to consume");
    evt_t msg = action_queue->pop();
    if (msg) {
      LOGGER_LOG_DEBUG(rule_id << " - Received new message, executing action");
#ifdef PERF
      std::string val = msg->get_value("eventTime");
      LOGGER_LOG_DEBUG("Got event time " << val);

      // extract milliseconds from time
      size_t dot_pos = val.find(".", 0);
      std::string time_no_millis = val.substr(0, dot_pos);
      int millis = std::stoi(val.substr(dot_pos + 1, 3));

      // convert to timestamp
      std::tm t = { 0 };
      std::istringstream ss(time_no_millis);
      ss >> std::get_time(&t, "%Y-%m-%d %H:%M:%S");
      time_t timestamp = std::mktime(&t);
      long timestamp_millis = timestamp * 1000 + millis;

      // get current timestamp
      long ms = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count();
      LOGGER_LOG_DEBUG("Got current time " << ms);

      // compute difference and log it
      long lat = ms - timestamp_millis;
      LOGGER_LOG_PERF("Rulelatency: " << lat);
#endif
      execute(std::move(msg));
    }
  }

  LOGGER_LOG_INFO(rule_id << " - finished");
}

int Action::init_output_stream(std::string dst, size_t from) {
  size_t dst_pos;
  if ((dst_pos = dst.find(DB_DST, from)) != std::string::npos) {
    // parse target DB properties and create DB output stream
    // the INTO portion looks like "INTO DB user:password@hostname/tablename USING schema"
    size_t using_pos = dst.find("USING", 0);
    std::string connection_string = dst.substr(from + 4 + 4, using_pos - (from + 4 + 5));
    std::string table_schema = dst.substr(using_pos + 6, dst.length() - (using_pos + 6));
    size_t slash_pos = table_schema.find("/");
    std::string tablename = table_schema.substr(0, slash_pos);
    std::string db_schema = table_schema.substr(slash_pos + 1, table_schema.length());

    // set up output stream
    out = new DBOutputStream(connection_string, db_schema, tablename, false);
    out->open();
    out_dest = DB_DST;
  } else if ((dst_pos = dst.find(FILE_DST, from)) != std::string::npos) {
    // parse file path and create File output stream
    // the INTO portion looks like "INTO FILE path"
    std::string path = dst.substr(from + 4 + 6, dst.length() - (from + 4 + 6));

    // set up output stream
    out = new FileOutputStream(path);
    out->open();
    out_dest = FILE_DST;
  } else {
    // no valid destination specified
    LOGGER_LOG_ERROR("Action " << dst << " does not contain a valid output destination. Valid "
        << "destinations are " << DB_DST << " and " << FILE_DST << ".");
    return ERROR_NO_RETRY;
  }

  return NO_ERROR;
}

int Action::init_state(std::string dst, size_t from) {
  size_t dst_pos;
  if ((dst_pos = dst.find(DB_DST, from)) != std::string::npos) {
    // parse target DB properties and create DB state backend
    // the INTO portion looks like "INTO DB user:password@hostname USING tablename/schema"
    size_t using_pos = dst.find("USING", 0);
    std::string connection_string = dst.substr(from + 4 + 4, using_pos - (from + 4 + 5));

    state_backend = std::make_unique<DBStateBackend>(connection_string);
    state_backend->connect();
  } else if ((dst_pos = dst.find(FILE_DST, from)) != std::string::npos) {
    // create File state backend
    // the INTO portion looks like "INTO FILE path"

    state_backend = std::make_unique<FileStateBackend>();
    state_backend->connect();
  } else {
    // no valid destination specified
    LOGGER_LOG_ERROR("Action " << dst << " does not contain a valid output destination. Valid "
        << "destinations are " << DB_DST << " and " << FILE_DST << ".");
    return ERROR_NO_RETRY;
  }

  return NO_ERROR;
}

void Action::start_action_consumers(int num_threads) {
  LOGGER_LOG_INFO(rule_id << " - starting action consumer");
  for (int i = 0; i < num_threads; i++) {
    consumer_threads.push_back(std::thread(&Action::run_consumer, this));
  }
}

void Action::stop_action_consumers() {
  running = false;
  // push a nullptr for each active thread to unblock pop()
  for (unsigned int i = 0; i < consumer_threads.size(); i++) {
    action_queue->push(std::unique_ptr<Event>(nullptr));
  }
  for (std::thread &t : consumer_threads) {
    t.join();
  }
}

Action::Action() :
    action_queue(new SynchronizedQueue<evt_t>()),
    running(true),
    out() {}

Action::~Action() {
  if (action_queue)
    delete action_queue;
  if (out) {
    out->flush();
    delete out;
  }
  if (state_backend) {
    state_backend->disconnect();
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

std::string LogLoadField::str() {
  if (is_timestamp) {
    return std::to_string(field_id) + "/" + std::to_string(timeoffset);
  } else if (is_event_field_name) {
    return field_name;
  } else if (is_range) {
    return std::to_string(field_id) + "-" + std::to_string(until_field_id);
  } else if (is_composite) {
    std::stringstream ss;
    for (size_t i = 0; i < field_ids.size(); i++) {
      ss << field_ids[i] << (i == field_ids.size() - 1 ? "" : "+");
    }
    return ss.str();
  } else {
    return std::to_string(field_id);
  }
}
