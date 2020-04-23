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

#include "scale-event.h"
#include "logger.h"

FSEvent::FSEvent(const std::string &serialized_event) {
  std::stringstream evt_ss(serialized_event);
  // event type
  std::string evt_type;
  if (!getline(evt_ss, evt_type, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // node_name
  if (!getline(evt_ss, node_name, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // send_time
  if (!getline(evt_ss, send_time, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // pid
  std::string pid_str;
  if (!getline(evt_ss, pid_str, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  pid = std::stoul(pid_str);
  // inode
  std::string inode_str;
  if (!getline(evt_ss, inode_str, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  inode = std::stoi(inode_str);
  // bytes_read
  std::string bytes_read_str;
  if (!getline(evt_ss, bytes_read_str, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  bytes_read = std::stoul(bytes_read_str);
  // bytes_written
  std::string bytes_written_str;
  if (!getline(evt_ss, bytes_written_str, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  bytes_written = std::stoi(bytes_written_str);
  // path
  if (!getline(evt_ss, path, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // dst_path
  if (!getline(evt_ss, dst_path, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // mode
  if (!getline(evt_ss, mode, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // event
  if (!getline(evt_ss, event, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // event_time
  if (!getline(evt_ss, event_time, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // cluster_name
  if (!getline(evt_ss, cluster_name, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // fs_name
  if (!getline(evt_ss, fs_name, ',')) {
    LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
}

FSEvent::FSEvent(osm_pid_t pid, int inode, size_t bytes_read,
    size_t bytes_written, std::string event, std::string event_time,
    std::string cluster_name, std::string fs_name, std::string path,
    std::string dst_path, std::string mode, std::string version_hash) :
    pid { pid },
    inode { inode },
    bytes_read { bytes_read },
    bytes_written {bytes_written },
    event { event },
    event_time { event_time },
    cluster_name { cluster_name },
    fs_name { fs_name },
    path { path },
    dst_path { dst_path },
    mode { mode },
    version_hash { version_hash } {
}

std::string FSEvent::serialize() const {
  std::stringstream evt;
  evt << get_type() << SER_DELIM
      << node_name << SER_DELIM
      << send_time << SER_DELIM;

  evt << pid << SER_DELIM
      << inode << SER_DELIM;

  evt << bytes_read << SER_DELIM
      << bytes_written << SER_DELIM
      << path << SER_DELIM
      << dst_path << SER_DELIM
      << mode << SER_DELIM;

  evt << event << SER_DELIM
      << event_time << SER_DELIM
      << cluster_name << SER_DELIM
      << fs_name << SER_DELIM;

  return evt.str();
}

std::string FSEvent::format_for_dst(ConsumerDestination c_dst) const {
  std::stringstream formatted;
  if (c_dst == CD_ODBC || c_dst == CD_FILE) {
    formatted << format_as_varchar(event, 20)
      << "," << format_as_varchar(cluster_name, 32)
      << "," << format_as_varchar(node_name, 128)
      << "," << format_as_varchar(fs_name, 32)
      << "," << format_as_varchar(path, 256)
      << "," << inode
      << "," << bytes_read
      << "," << bytes_written
      << "," << pid
      << "," << format_as_varchar(event_time)
      << "," << format_as_varchar(dst_path, 256)
      << "," << format_as_varchar(version_hash, 32);
  } else {
    assert(!"Error, unsupported cdest\n");
  }
  return formatted.str();
}

std::string FSEvent::get_value(std::string field) const {
  if (field == "event")
    return event;
  else if (field == "cluster_name")
    return cluster_name;
  else if (field == "fs_name")
    return fs_name;
  else if (field == "path")
    return path;
  else if (field == "inode")
    return std::to_string(inode);
  else if (field == "bytes_read")
    return std::to_string(bytes_read);
  else if (field == "bytes_written")
    return std::to_string(bytes_written);
  else if (field == "pid")
    return std::to_string(pid);
  else if (field == "event_time")
    return event_time;
  else if (field == "dst_path")
    return dst_path;
  else if (field == "version_hash")
    return version_hash;
  else
    return "";
}
