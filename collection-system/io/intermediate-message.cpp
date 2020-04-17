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
  if (field == "f1")
    return f1;
  else if (field == "f2")
    return f2;
  else if (field == "f3")
    return f3;
  else
    return "";
}

/*------------------------------
 * ScaleIntermediateMessage
 *------------------------------*/

ScaleIntermediateMessage::ScaleIntermediateMessage(ConsumerSource csrc,
    const std::string &msgin) {
  assert(csrc == CS_PROV_GPFS);
  const int num_fields = 12;

  // split on commas
  std::stringstream ss(msgin);
  std::string item;
  std::vector<std::string> tokens;
  while (getline(ss, item, ',')) {
    tokens.push_back(item);
  }

  if (tokens.size() != num_fields) {
    LOG_ERROR("Got " << tokens.size() << " tokens but expected " << num_fields
        << " tokens in message <" << msgin << ">");
    assert(0 == 1);
  }

  int i = 0;
  event = tokens.at(i++);
  cluster_name = tokens.at(i++);
  node_name = tokens.at(i++);
  fs_name = tokens.at(i++);
  path = tokens.at(i++);
  inode = tokens.at(i++);
  bytes_read = tokens.at(i++);
  bytes_written = tokens.at(i++);
  pid = tokens.at(i++);
  event_time = tokens.at(i++);
  dst_path = tokens.at(i++);
  mode = tokens.at(i++);
}

std::string ScaleIntermediateMessage::normalize(ConsumerDestination cdest,
    std::string delim) const {
  std::string normalized;
  if (cdest == CD_DB2 || CD_POSTGRES) {
    normalized = format_as_varchar(event, 20) + delim
        + format_as_varchar(cluster_name, 32) + delim
        + format_as_varchar(node_name, 128) + delim
        + format_as_varchar(fs_name, 32) + delim + format_as_varchar(path, 256)
        + delim + inode + delim + bytes_read + delim + bytes_written + delim
        + pid + delim + format_as_varchar(event_time) + delim
        + format_as_varchar(dst_path, 256) + delim
        + format_as_varchar(version_hash, 32);
  } else {
    LOG_ERROR("Unsupported destination, can't normalize record.");
  }

  return normalized;
}

std::string ScaleIntermediateMessage::get_value(std::string field) const {
  if (field == "event")
    return event;
  else if (field == "clusterName")
    return cluster_name;
  else if (field == "nodeName")
    return node_name;
  else if (field == "fsName")
    return fs_name;
  else if (field == "path")
    return path;
  else if (field == "inode")
    return inode;
  else if (field == "bytesRead")
    return bytes_read;
  else if (field == "bytesWritten")
    return bytes_written;
  else if (field == "pid")
    return pid;
  else if (field == "eventTime")
    return event_time;
  else if (field == "dstPath")
    return dst_path;
  else if (field == "versionHash")
    return version_hash;
  else
    return "";
}
