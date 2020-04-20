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
#include <string>
#include <map>

#include <assert.h>
#include <string.h>
#include <errno.h>

#include "event.h"
#include "logger.h"

/*------------------------------
 * Event
 *------------------------------*/

std::string Event::format_as_varchar(const std::string &str, int limit) const {
  std::string escaped_str = "";
  escaped_str.reserve(0 <= limit ? limit : 256);

  // Escape any ' characters. This will double-escape any
  // already-escaped ' chars.
  bool truncated_early = false;
  for (size_t i = 0; i < str.length(); i++) {
    if (0 <= limit && limit <= (int) escaped_str.length()) {
      truncated_early = true;
      // Too long, might as well break now.
      break;
    }

    if (str[i] == '\'') {
      escaped_str += "''";
    } else {
      escaped_str += str[i];
    }
  }

  if (truncated_early || (0 <= limit && limit < (int) escaped_str.length())) {
    // naive truncate
    escaped_str.resize(limit - 3);
    // make sure we don't leave a trailing single-quote at the end of the string
    int escape_parity = 0;
    for (auto pos = escaped_str.length() - 1; 0 < pos; pos--) {
      if (escaped_str.at(pos) == '\'') {
        escape_parity++;
      } else {
        break;
      }
    }
    // If even, any suffix of single-quotes is escaped.
    // Otherwise, remove one additional char.
    if (escape_parity % 2) {
      escaped_str.resize(escaped_str.length() - 1);
    }
    escaped_str += "...";
  }

  return "'" + escaped_str + "'";
}

evt_t Event::deserialize_event(const std::string &event) {
  std::stringstream ss(event);
  std::string evt_type;
  // first field is the event type
  if(!getline(ss, evt_type, ',')) {
    LOG_ERROR("Can't deserialize event " << event << " Dropping event.");
    return nullptr;
  }

  // now deserialize the event specific content
  switch (std::stoi(evt_type)) {
  case TEST_EVENT: return std::make_shared<TestEvent>(event); break;
  default:
    LOG_ERROR("Received invalid event " << event << " Not deserializing.");
    return nullptr;
  }

  return nullptr;
}

/*------------------------------
 * TestEvent
 *------------------------------*/

TestEvent::TestEvent(const std::string &serialized_event) {
  // we expect another three fields containing f1, f2, and f3 (in that order)
  std::stringstream evt_ss(serialized_event);
  std::string evt_type;
  if (!getline(evt_ss, evt_type, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as TestEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a TestEvent.");
  }
  if (!getline(evt_ss, node_name, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as TestEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a TestEvent.");
  }
  if (!getline(evt_ss, send_time, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as TestEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a TestEvent.");
  }
  if (!getline(evt_ss, f1, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as TestEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a TestEvent.");
  }
  if (!getline(evt_ss, f2, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as TestEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a TestEvent.");
  }
  if (!getline(evt_ss, f3, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as TestEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a TestEvent.");
  }
}

TestEvent::TestEvent(std::string f1, std::string f2, std::string f3) :
    f1 { f1 },
    f2 { f2 },
    f3 { f3 } {
}

std::string TestEvent::serialize() const {
  std::stringstream evt;
  evt << get_type() << SER_DELIM
      << node_name << SER_DELIM
      << send_time << SER_DELIM;

  evt << f1 << SER_DELIM
      << f2 << SER_DELIM
      << f3 << SER_DELIM;

  return evt.str();
}

std::string TestEvent::format_for_dst(ConsumerDestination c_dst) const {
  std::string normalized;
  if (c_dst == CD_DB2 || c_dst == CD_POSTGRES) {
    normalized = format_as_varchar(f1, 20)
        + "," + format_as_varchar(f2, 32)
        + "," + format_as_varchar(f3, 128);
  } else {
    assert(!"Error, unsupported cdest\n");
  }

  return normalized;
}

std::string TestEvent::get_value(std::string field) const {
  if (field == "f1")
    return f1;
  else if (field == "f2")
    return f2;
  else if (field == "f3")
    return f3;
  else
    return "";
}
