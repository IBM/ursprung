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
#include <assert.h>
#include <rapidjson/document.h>

#include "scale-event.h"
#include "logger.h"

/*------------------------------
 * FSEvent
 *------------------------------*/

FSEvent::FSEvent(const std::string &serialized_event) {
  std::stringstream evt_ss(serialized_event);
  // event type
  std::string evt_type;
  if (!getline(evt_ss, evt_type, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // event
  if (!getline(evt_ss, event, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // cluster_name
  if (!getline(evt_ss, cluster_name, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // node_name
  if (!getline(evt_ss, node_name, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // fs_name
  if (!getline(evt_ss, fs_name, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // path
  if (!getline(evt_ss, path, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // inode
  std::string inode_str;
  if (!getline(evt_ss, inode_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  inode = std::stoi(inode_str);
  // bytes_read
  std::string bytes_read_str;
  if (!getline(evt_ss, bytes_read_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  bytes_read = std::stoul(bytes_read_str);
  // bytes_written
  std::string bytes_written_str;
  if (!getline(evt_ss, bytes_written_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  bytes_written = std::stoi(bytes_written_str);
  // pid
  std::string pid_str;
  if (!getline(evt_ss, pid_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  pid = std::stoul(pid_str);
  // event_time
  if (!getline(evt_ss, event_time, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // dst_path
  if (!getline(evt_ss, dst_path, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
  // mode
  if (!getline(evt_ss, mode, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as FSEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEvent.");
  }
}

FSEvent::FSEvent(osm_pid_t pid, int inode, long bytes_read,
    long bytes_written, std::string event, std::string event_time,
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
  evt << get_type() << SER_DELIM;
  evt << event << SER_DELIM
      << cluster_name << SER_DELIM
      << node_name << SER_DELIM
      << fs_name << SER_DELIM;

  evt << path << SER_DELIM
      << inode << SER_DELIM
      << bytes_read << SER_DELIM
      << bytes_written << SER_DELIM
      << pid << SER_DELIM
      << event_time << SER_DELIM
      << dst_path << SER_DELIM
      << mode << SER_DELIM;

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

/*------------------------------
 * FSEventJson
 *------------------------------*/

std::map<std::string, std::string> FSEventJson::WFEVENT_TO_FSEVENT = {
    { "IN_OPEN", "OPEN" },
    { "IN_CLOSE_WRITE", "CLOSE" },
    { "IN_CLOSE_NOWRITE", "CLOSE" },
    { "IN_CREATE", "CREATE" },
    { "IN_DELETE", "UNLINK" },
    { "IN_DELETE_SELF", "UNLINK" },
    { "IN_MOVED_FROM", "RENAME" },
    { "IN_MOVED_TO", "RENAME" }
};

std::map<long, std::string> FSEventJson::COOKIE_STATE = {};

FSEventJson::FSEventJson(const std::string &serialized_event) {
  rapidjson::Document doc;
  doc.Parse(serialized_event.c_str());

  if (!doc.IsObject()) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as"
        " FSEventJson. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEventJson.");
  }

  // event
  if (!doc.HasMember("event")) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as"
        " FSEventJson. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEventJson.");
  }
  std::string wf_event = doc["event"].GetString();
  event = FSEventJson::WFEVENT_TO_FSEVENT[wf_event];

  // cluster name
  if (!doc.HasMember("clusterName")) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as"
        " FSEventJson. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEventJson.");
  }
  cluster_name = doc["clusterName"].GetString();


  // node_name
  if (!doc.HasMember("nodeName")) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as"
        " FSEventJson. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEventJson.");
  }
  node_name = doc["nodeName"].GetString();

  // fs_name
  if (!doc.HasMember("fsName")) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as"
        " FSEventJson. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEventJson.");
  }
  fs_name = doc["fsName"].GetString();

  // path
  if (!doc.HasMember("path")) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as"
        " FSEventJson. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEventJson.");
  }
  path = doc["path"].GetString();

  // inode
  if (!doc.HasMember("inode")) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as"
        " FSEventJson. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEventJson.");
  }
  inode = std::stoi(doc["inode"].GetString());

  // bytes_read
  bytes_read = 0;
  if (wf_event == "IN_CLOSE_NOWRITE") bytes_read = 1;

  // bytes_written
  bytes_written = 0;
  if (wf_event == "IN_CLOSE_WRITE") bytes_written = 1;

  // pid
  if (!doc.HasMember("processId")) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as"
        " FSEventJson. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEventJson.");
  }
  pid = std::stoi(doc["processId"].GetString());

  // event_time
  if (!doc.HasMember("eventTime")) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as"
        " FSEventJson. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEventJson.");
  }
  // convert to UTC time (scale event times are %YYYY-%mm-%dd_%HH-%MM-%SS%z)
  std::string time_str = doc["eventTime"].GetString();
  struct tm tm1{};
  strptime(time_str.c_str(), "%Y-%m-%d_%H:%M:%S%z", &tm1);
  tm1.tm_isdst = -1;
  time_t t = mktime(&tm1);

  struct tm tm2{};
  gmtime_r(&t, &tm2);
  char string_representation[32];
  size_t len = strftime(string_representation, sizeof(string_representation),
      "%Y-%m-%d %H:%M:%S", &tm2);
  // add '000' as milliseconds as we need it for the database but don't get
  // milliseconds from watch folders
  sprintf(string_representation + len, ".%03d", 0);
  event_time = std::string(string_representation);

  // dst_path
  dst_path = "_NULL_";
  // TODO check that order is always IN_MOVED_FROM first and then IN_MOVED_TO
  if (!doc.HasMember("cookie")) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as"
        " FSEventJson. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEventJson.");
  }
  if (wf_event == "IN_MOVED_FROM") {
    long cookie = std::stoul(doc["cookie"].GetString());
    COOKIE_STATE[cookie] = path;
    // TODO make less hacky: we want the constructor to fail as the actual
    // RENAME event will be created once we receive the IN_MOVED_TO event
    throw std::invalid_argument(serialized_event + ": waiting for IN_MOVE_TO.");
  } else if (wf_event == "IN_MOVED_TO") {
    // make sure that an entry for this cookie exists
    long cookie = std::stoul(doc["cookie"].GetString());
    if (COOKIE_STATE.find(cookie) == COOKIE_STATE.end()) {
      throw std::invalid_argument(serialized_event + ": did not see corresponding "
          "IN_MOVED_FROM. Discarding event");
    }
    std::string src_path = COOKIE_STATE[cookie];
    dst_path = path;
    path = src_path;
    // clean up map entry
    COOKIE_STATE.erase(cookie);
  }

  // mode
  if (!doc.HasMember("permissions")) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as"
        " FSEventJson. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a FSEventJson.");
  }
  mode = doc["permissions"].GetString();
}
