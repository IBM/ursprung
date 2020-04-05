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

#include <sstream>
#include <vector>

#include "intermediate-message.h"
#include "logger.h"

/*------------------------------
 * IntermediateMessage
 *------------------------------*/

std::string IntermediateMessage::format_as_varchar(const std::string &str, int limit) const {
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

/*------------------------------
 * TestIntermediateMessage
 *------------------------------*/

TestIntermediateMessage::TestIntermediateMessage(ConsumerSource csrc,
    const std::string &msgin) {
  // split on commas
  std::stringstream ss(msgin);
  std::string item;
  std::vector<std::string> tokens;

  while (getline(ss, item, ',')) {
    tokens.push_back(item);
  }
  if (tokens.size() != NUM_FIELDS) {
    LOG_WARN("Got " << tokens.size() << " tokens but expected " <<
        TestIntermediateMessage::NUM_FIELDS << " tokens in message <" <<
        msgin << ">. Ignoring extra tokens.");
  }

  int i = 0;
  f1 = tokens.at(i++);
  f2 = tokens.at(i++);
  f3 = tokens.at(i++);
}

std::string TestIntermediateMessage::normalize(ConsumerDestination cdst,
    std::string delim) const {
  std::string normalized;
  if (cdst == CD_DB2 || cdst == CD_POSTGRES) {
    normalized = format_as_varchar(f1, 20)
        + delim + format_as_varchar(f2, 32)
        + delim + format_as_varchar(f3, 128);
  } else {
    assert(!"Error, unsupported cdest\n");
  }

  return normalized;
}

std::string TestIntermediateMessage::get_value(std::string field) const {
  if (field == "f1") return f1;
  else if (field == "f2") return f2;
  else if (field == "f3") return f3;
  else return "";
}
