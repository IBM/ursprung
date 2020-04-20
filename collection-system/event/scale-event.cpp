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
      << fs_name << SER_DELIM
      << version_hash << SER_DELIM;

  return evt.str();
}

std::string FSEvent::format_for_dst(ConsumerDestination c_dst) const {
  // TODO implement
  return "";
}

std::string FSEvent::get_value(std::string field) const {
  // TODO implement
  return "";
}


