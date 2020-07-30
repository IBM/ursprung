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
#include <cstring>
#include <algorithm>
#include <assert.h>

#include "auditd-event.h"
#include "logger.h"

/*------------------------------
 * SyscallEvent
 *------------------------------*/

#ifdef __linux__
SyscallEvent::SyscallEvent(auparse_state_t *au) :
    pid {-1 },
    ppid { -1 },
    uid { -1 },
    gid { -1 },
    euid { -1 },
    egid { -1 },
    syscall_name { "unknown" },
    event_time { "" },
    rc { RETURNS_VOID },
    data { } {
  // ensure we are parsing an auditd syscall event
  assert(strcmp(auparse_get_type_name(au), "SYSCALL") == 0);
  int pos = auparse_get_record_num(au);

  // extract fields from auditd event
  auparse_goto_record_num(au, pos);
  auparse_first_field(au);
  do {
    const char *field_name = auparse_get_field_name(au);
    if (!field_name)
      continue;

    if (strcmp(field_name, "pid") == 0)
      pid = auparse_get_field_int(au);
    else if (strcmp(field_name, "ppid") == 0)
      ppid = auparse_get_field_int(au);
    else if (strcmp(field_name, "uid") == 0)
      uid = auparse_get_field_int(au);
    else if (strcmp(field_name, "euid") == 0)
      euid = auparse_get_field_int(au);
    else if (strcmp(field_name, "gid") == 0)
      gid = auparse_get_field_int(au);
    else if (strcmp(field_name, "egid") == 0)
      egid = auparse_get_field_int(au);
    // Normalize syscall name: 'clone', etc.
    else if (strcmp(field_name, "syscall") == 0)
      syscall_name = auparse_interpret_field(au);
    else if (strcmp(field_name, "exit") == 0)
      rc = auparse_get_field_int(au);
    // Normalize args: 'O_RDWR|...', etc.
    else if (strcmp(field_name, "a0") == 0)
      arg0 = auparse_interpret_field(au);
    else if (strcmp(field_name, "a1") == 0)
      arg1 = auparse_interpret_field(au);
    else if (strcmp(field_name, "a2") == 0)
      arg2 = auparse_interpret_field(au);
    else if (strcmp(field_name, "a3") == 0)
      arg3 = auparse_interpret_field(au);
    else if (strcmp(field_name, "a4") == 0)
      arg4 = auparse_interpret_field(au);

  } while (0 < auparse_next_field(au));

  // get event ID and timestamp
  const au_event_t *au_event = auparse_get_timestamp(au);
  struct tm tm_;
  gmtime_r(&au_event->sec, &tm_); // Use GMT/UTC because that's what GPFS LWE (policy engine) does.
  char string_representation[32]; // ~20 + ~4 plus some buffer
  size_t len = strftime(string_representation, sizeof(string_representation),
      "%Y-%m-%d %H:%M:%S", &tm_);
  sprintf(string_representation + len, ".%03d", au_event->milli);

  auditd_event_id = au_event->serial;
  event_time = string_representation;

  // set any additional data fields for exec, pipe, and socket-related calls
  if (syscall_name == "execve") {
    data.clear();
    data.push_back(get_cwd(au));
    std::vector<std::string> execve_args = get_execve_args(au);
    data.insert(data.end(), execve_args.begin(), execve_args.end());
  } else if (syscall_name == "pipe") {
    data.clear();
    data = get_pipe_fds(au);
  } else if (syscall_name == "accept" || syscall_name == "connect" || syscall_name == "bind") {
    data.clear();
    data = get_sockaddr(au);
  }

  // reset any modifications to au
  auparse_goto_record_num(au, pos);
}
#endif

SyscallEvent::SyscallEvent(const std::string &serialized_event) {
  std::stringstream evt_ss(serialized_event);
  // event type
  std::string evt_type;
  if (!getline(evt_ss, evt_type, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  // node_name
  if (!getline(evt_ss, node_name, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  // send_time
  if (!getline(evt_ss, send_time, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  // auditd_event_id
  std::string auditd_event_id_str;
  if (!getline(evt_ss, auditd_event_id_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  auditd_event_id = std::stoul(auditd_event_id_str);
  // pid
  std::string pid_str;
  if (!getline(evt_ss, pid_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  pid = std::stoi(pid_str);
  // ppid
  std::string ppid_str;
  if (!getline(evt_ss, ppid_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  ppid = std::stoi(ppid_str);
  // uid
  std::string uid_str;
  if (!getline(evt_ss, uid_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  uid = std::stoi(uid_str);
  // gid
  std::string gid_str;
  if (!getline(evt_ss, gid_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  gid = std::stoi(gid_str);
  // euid
  std::string euid_str;
  if (!getline(evt_ss, euid_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  euid = std::stoi(euid_str);
  // egid
  std::string egid_str;
  if (!getline(evt_ss, egid_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  egid = std::stoi(egid_str);
  // syscall_name
  if (!getline(evt_ss, syscall_name, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  // rc
  std::string rc_str;
  if (!getline(evt_ss, rc_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  rc = std::stoi(rc_str);
  // arg0 - arg4
  if (!getline(evt_ss, arg0, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  if (!getline(evt_ss, arg1, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  if (!getline(evt_ss, arg2, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  if (!getline(evt_ss, arg3, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  if (!getline(evt_ss, arg4, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  // event_time
  if (!getline(evt_ss, event_time, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SyscallEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SyscallEvent.");
  }
  // data
  std::string tok;
  while (getline(evt_ss, tok, ',')) {
    data.push_back(tok);
  }
}

std::string SyscallEvent::serialize() const {
  std::stringstream evt;
  evt << get_type() << SER_DELIM
      << node_name << SER_DELIM
      << send_time << SER_DELIM;

  evt << auditd_event_id << SER_DELIM
      << pid << SER_DELIM
      << ppid << SER_DELIM
      << uid << SER_DELIM
      << gid << SER_DELIM
      << euid << SER_DELIM
      << egid << SER_DELIM;

  evt << syscall_name << SER_DELIM
      << rc << SER_DELIM
      << arg0 << SER_DELIM
      << arg1 << SER_DELIM
      << arg2 << SER_DELIM
      << arg3 << SER_DELIM
      << arg4 << SER_DELIM;

  evt << event_time << SER_DELIM;
  for (std::string tok : data) {
    evt << tok << SER_DELIM;
  }

  return evt.str();
}

std::string SyscallEvent::format_for_dst(ConsumerDestination c_dst) const {
  std::stringstream formatted;
  if (c_dst == CD_ODBC || c_dst == CD_FILE) {
    formatted << "SyscallEvent"
      << "," << format_as_varchar(node_name, 128)
      << "," << std::to_string(auditd_event_id)
      << "," << std::to_string(pid)
      << "," << std::to_string(ppid)
      << "," << std::to_string(uid)
      << "," << std::to_string(gid)
      << "," << std::to_string(euid)
      << "," << std::to_string(egid)
      << "," << format_as_varchar(syscall_name, 10)
      << "," << format_as_varchar(arg0, 200)
      << "," << format_as_varchar(arg1, 200)
      << "," << format_as_varchar(arg2, 200)
      << "," << format_as_varchar(arg3, 200)
      << "," << format_as_varchar(arg4, 200)
      << "," << std::to_string(rc)
      << "," << format_as_varchar(event_time)
      << "," << format_as_varchar(data.size() >= 1 ? data[0] : "", 256)
      << "," << format_as_varchar(data.size() >= 2 ? data[1] : "", 256);
  } else {
    assert(!"Error, unsupported cdest\n");
  }
  return formatted.str();
}

std::string SyscallEvent::get_value(std::string field) const {
  if (field == "auditd_event_id")
    return std::to_string(auditd_event_id);
  else if (field == "pid")
    return std::to_string(pid);
  else if (field == "ppid")
    return std::to_string(ppid);
  else if (field == "uid")
    return std::to_string(uid);
  else if (field == "gid")
    return std::to_string(gid);
  else if (field == "euid")
    return std::to_string(euid);
  else if (field == "egid")
    return std::to_string(egid);
  else if (field == "syscall_name")
    return syscall_name;
  else if (field == "arg0")
    return arg0;
  else if (field == "arg1")
    return arg1;
  else if (field == "arg2")
    return arg2;
  else if (field == "arg3")
    return arg3;
  else if (field == "arg4")
    return arg4;
  else if (field == "rc")
    return std::to_string(rc);
  else if (field == "event_time")
    return event_time;
  else if (field == "type")
    return event_type_to_string[get_type()];
#ifdef __linux__
  else if (field == "data")
    return get_data_as_string();
#endif
  else
    return "";
}

#ifdef __linux__
std::vector<std::string> SyscallEvent::get_execve_args(auparse_state_t *au) {
  int pos = auparse_get_record_num(au);
  char arg_name[16];

  std::vector<std::string> args;

  // find the EXECVE record and get the args
  auparse_first_record(au);
  auparse_first_field(au);
  do {
    if (strcmp(auparse_get_type_name(au), "EXECVE") == 0) {
      auparse_first_field(au);
      // get number of command line arguments
      int argc = atoi(auparse_find_field(au, "argc"));
      auparse_first_field(au);
      do {
        const char *field_name = auparse_get_field_name(au);
        if (!field_name)
          continue;

        for (int i = 0; i < argc; i++) {
          snprintf(arg_name, sizeof(arg_name), "a%d", i);
          if (strcmp(field_name, arg_name) == 0) {
            std::string arg = auparse_interpret_field(au);
            args.push_back(arg);
            break;
          }
        }
      } while (0 < auparse_next_field(au));
      break;
    }
  } while (0 < auparse_next_record(au));

  auparse_goto_record_num(au, pos);
  return args;
}

std::vector<std::string> SyscallEvent::get_pipe_fds(auparse_state_t *au) {
  int pos = auparse_get_record_num(au);

  // find the FD_PAIR record and get the two fds
  std::vector<std::string> fds;
  auparse_first_record(au);
  auparse_first_field(au);
  do {
    if (strcmp(auparse_get_type_name(au), "FD_PAIR") == 0) {
      auparse_first_field(au);
      std::string fd0 = auparse_find_field(au, "fd0");
      std::string fd1 = auparse_find_field(au, "fd1");
      fds.push_back(fd0);
      fds.push_back(fd1);
      break;
    }
  } while (0 < auparse_next_record(au));

  auparse_goto_record_num(au, pos);
  return fds;
}

std::vector<std::string> SyscallEvent::get_sockaddr(auparse_state_t *au) {
  int pos = auparse_get_record_num(au);

  // find the SOCKADDR record and get the sockaddr struct
  std::vector<std::string> socket;
  auparse_first_record(au);
  auparse_first_field(au);
  do {
    if (strcmp(auparse_get_type_name(au), "SOCKADDR") == 0) {
      auparse_first_field(au);
      auparse_find_field(au, "saddr");
      std::string saddr_str = auparse_interpret_field(au);
      // extract ip address and port
      std::string laddr = "laddr=";
      std::string lport = "lport=";
      int laddr_pos = saddr_str.find(laddr);
      int lport_pos = saddr_str.find(lport);
      // we subtract 1 for the space between the laddr and the lport entries
      std::string addr = saddr_str.substr(laddr_pos + laddr.length(),
          lport_pos - (laddr_pos + laddr.length()) - 1);
      // we subtract 2 for the space and the '}' at the end of the SOCKADDR record
      std::string port = saddr_str.substr(lport_pos + lport.length(),
          saddr_str.length() - (lport_pos + lport.length()) - 2);
      socket.push_back(addr);
      socket.push_back(port);
      break;
    }
  } while (0 < auparse_next_record(au));

  auparse_goto_record_num(au, pos);
  return socket;
}

std::string SyscallEvent::get_cwd(auparse_state_t *au) {
  int pos = auparse_get_record_num(au);
  std::string cwd = "unknown";

  // find the CWD record and get the value
  auparse_first_record(au);
  auparse_first_field(au);
  do {
    if (strcmp(auparse_get_type_name(au), "CWD") == 0) {
      cwd = auparse_find_field(au, "cwd");
      break;
    }
  } while (0 < auparse_next_record(au));

  auparse_goto_record_num(au, pos);
  return cwd;
}

std::string SyscallEvent::get_data_as_string() const {
  std::string data_str;
  for (std::string item : data) {
    data_str += item + " ";
  }
  return data_str;
}
#endif

/*------------------------------
 * ProcessEvent
 *------------------------------*/

ProcessEvent::ProcessEvent(const std::string &serialized_event) {
  std::stringstream evt_ss(serialized_event);
  // event type
  std::string evt_type;
  if (!getline(evt_ss, evt_type, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as ProcessEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a ProcessEvent.");
  }
  // node_name
  if (!getline(evt_ss, node_name, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as ProcessEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a ProcessEvent.");
  }
  // send_time
  if (!getline(evt_ss, send_time, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as ProcessEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a ProcessEvent.");
  }
  // pid
  std::string pid_str;
  if (!getline(evt_ss, pid_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as ProcessEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a ProcessEvent.");
  }
  pid = std::stoi(pid_str);
  // ppid
  std::string ppid_str;
  if (!getline(evt_ss, ppid_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as ProcessEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a ProcessEvent.");
  }
  ppid = std::stoi(ppid_str);
  // pgid
  std::string pgid_str;
  if (!getline(evt_ss, pgid_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as ProcessEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a ProcessEvent.");
  }
  pgid = std::stoi(pgid_str);
  // start_time_utc
  if (!getline(evt_ss, start_time_utc, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as ProcessEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a ProcessEvent.");
  }
  // finish_time_utc
  if (!getline(evt_ss, finish_time_utc, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as ProcessEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a ProcessEvent.");
  }
  // exec_cwd
  if (!getline(evt_ss, exec_cwd, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as ProcessEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a ProcessEvent.");
  }
  // exec_cmd_line
  std::string tok;
  while (getline(evt_ss, tok, ',')) {
    exec_cmd_line.push_back(tok);
  }
}

ProcessEvent::ProcessEvent(osm_pid_t pid, osm_pid_t ppid, osm_pgid_t pgid,
    std::string exec_cwd, std::vector<std::string> exec_cmd_line,
    std::string start_time_utc, std::string finish_time_utc) :
    pid { pid },
    ppid { ppid },
    pgid { pgid },
    exec_cwd { exec_cwd },
    exec_cmd_line ( exec_cmd_line ),
    start_time_utc { start_time_utc },
    finish_time_utc { finish_time_utc } {
}

std::string ProcessEvent::serialize() const {
  std::stringstream evt;
  evt << get_type() << SER_DELIM
       << node_name << SER_DELIM
       << send_time << SER_DELIM;

  evt << pid << SER_DELIM
      << ppid << SER_DELIM
      << pgid << SER_DELIM
      << start_time_utc << SER_DELIM
      << finish_time_utc << SER_DELIM;

  evt << exec_cwd << SER_DELIM;
  for (auto tok : exec_cmd_line) {
    evt << tok << SER_DELIM;
  }

  return evt.str();
}

std::string ProcessEvent::format_for_dst(ConsumerDestination c_dst) const {
  std::stringstream formatted;
  if (c_dst == CD_ODBC || c_dst == CD_FILE) {
    formatted << "ProcessEvent"
      << "," << format_as_varchar(node_name, 128)
      << "," << std::to_string(pid)
      << "," << std::to_string(ppid)
      << "," << std::to_string(pgid)
      << "," << format_as_varchar(exec_cwd, 256)
      << "," << format_as_varchar(format_cmd_line(512))
      << "," << format_as_varchar(start_time_utc)
      << "," << format_as_varchar(finish_time_utc);
  } else {
    assert(!"Error, unsupported cdest\n");
  }
  return formatted.str();
}

std::string ProcessEvent::get_value(std::string field) const {
  if (field == "pid")
    return std::to_string(pid);
  else if (field == "ppid")
    return std::to_string(ppid);
  else if (field == "pgid")
    return std::to_string(pgid);
  else if (field == "start_time_utc")
    return start_time_utc;
  else if (field == "finish_time_utc")
    return finish_time_utc;
  else if (field == "exec_cwd")
    return exec_cwd;
  else if (field == "type")
    return event_type_to_string[get_type()];
  else
    return "";
}

bool ProcessEvent::will_cmd_line_fit(const std::vector<std::string> &cmd_line,
    int limit) const {
  int length = 0;
  for (auto tok : cmd_line) {
    length += tok.size() + 1;
  }
  // don't delimit the last one
  if (length) {
    length--;
  }
  return (length <= limit);
}

std::string ProcessEvent::format_cmd_line(int limit) const {
  std::vector<std::string> tmp_cmd_line = exec_cmd_line;

  // Repeatedly trim the longest token until fewer than TRUNCATE_AMOUNT characters remain.
  // Truncating adds 3-4 (...) characters, so step by 10 to make sure this proceeds
  // reasonably quickly.
  const int TRUNCATE_AMOUNT = 10;
  while (!will_cmd_line_fit(tmp_cmd_line, limit)) {
    // get the longest string in tmpCmdLine, trying not to count the first 2
    // (command name, possibly preceded by 'python' or something).
    auto search_start_itr = tmp_cmd_line.begin();
    for (int i = 0; i < 2 && search_start_itr != tmp_cmd_line.end(); i++) {
      search_start_itr++;
    }
    std::vector<std::string>::iterator longest = std::max_element(
        search_start_itr, tmp_cmd_line.end(),
        [](const std::string &lhs, const std::string &rhs) {
          return lhs.size() < rhs.size();
        });

    if (longest == tmp_cmd_line.end()) {
      break;
    }

    // if we can't truncate it anymore, bail
    int old_longest_size = longest->size();
    if (old_longest_size < TRUNCATE_AMOUNT) {
      break;
    }

    // Shrink and add indicator that truncating occurred.
    bool append_quote = (longest->at(0) == '"' && longest->at(old_longest_size - 1) == '"');
    longest->resize(old_longest_size - TRUNCATE_AMOUNT);
    longest->append("...");
    // If this is a "-delimited arg, replace the final quote.
    if (append_quote) {
      longest->append("\"");
    }
  }

  // if it still doesn't fit, start throwing away args
  bool dropped_args = false;
  std::string dropped_args_append = "\"...\"";
  while (!will_cmd_line_fit(tmp_cmd_line, limit - (dropped_args_append.size() + 1))) {
    dropped_args = true;
    tmp_cmd_line.pop_back();
  }
  // if we discarded args, append an indicator
  if (dropped_args) {
    tmp_cmd_line.push_back(dropped_args_append);
  }

  assert(will_cmd_line_fit(tmp_cmd_line, limit));

  // construct the command line, join on " "
  std::string ret = "";
  for (auto tok : tmp_cmd_line) {
    ret += tok + " ";
  }
  // discard the trailing space
  if (ret.size()) {
    ret.resize(ret.size() - 1);
  }

  return ret;
}

/*------------------------------
 * ProcessGroupEvent
 *------------------------------*/

ProcessGroupEvent::ProcessGroupEvent(const std::string &serialized_event) {
  std::stringstream evt_ss(serialized_event);
  // event type
  std::string evt_type;
  if (!getline(evt_ss, evt_type, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as ProcessGroupEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a ProcessGroupEvent.");
  }
  // node_name
  if (!getline(evt_ss, node_name, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as ProcessGroupEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a ProcessGroupEvent.");
  }
  // send_time
  if (!getline(evt_ss, send_time, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as ProcessGroupEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a ProcessGroupEvent.");
  }
  // pgid
  std::string pgid_str;
  if (!getline(evt_ss, pgid_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as ProcessGroupEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a ProcessGroupEvent.");
  }
  pgid = std::stoi(pgid_str);
  // start_time_utc
  if (!getline(evt_ss, start_time_utc, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as ProcessGroupEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a ProcessGroupEvent.");
  }
  // finish_time_utc
  if (!getline(evt_ss, finish_time_utc, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as ProcessGroupEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a ProcessGroupEvent.");
  }
}

ProcessGroupEvent::ProcessGroupEvent(osm_pgid_t pgid, std::string start_time_utc,
    std::string finish_time_utc) :
    pgid { pgid },
    start_time_utc { start_time_utc },
    finish_time_utc { finish_time_utc } {
}

std::string ProcessGroupEvent::serialize() const {
  std::stringstream evt;
  evt << get_type() << SER_DELIM
      << node_name << SER_DELIM
      << send_time << SER_DELIM;

  evt << pgid << SER_DELIM
      << start_time_utc << SER_DELIM
      << finish_time_utc << SER_DELIM;

  return evt.str();
}

std::string ProcessGroupEvent::format_for_dst(ConsumerDestination c_dst) const {
  std::stringstream formatted;
  if (c_dst == CD_ODBC || c_dst == CD_FILE) {
    formatted << "ProcessGroupEvent"
      << "," << format_as_varchar(node_name, 128)
      << "," << std::to_string(pgid)
      << "," << format_as_varchar(start_time_utc)
      << "," << format_as_varchar(finish_time_utc);
  } else {
    assert(!"Error, unsupported cdest\n");
  }
  return formatted.str();
}

std::string ProcessGroupEvent::get_value(std::string field) const {
  if (field == "pgid")
    return std::to_string(pgid);
  else if (field == "start_time_utc")
    return start_time_utc;
  else if (field == "finish_time_utc")
    return finish_time_utc;
  else if (field == "type")
    return event_type_to_string[get_type()];
  else
    return "";
}

/*------------------------------
 * IPCEvent
 *------------------------------*/

IPCEvent::IPCEvent(const std::string &serialized_event) {
  std::stringstream evt_ss(serialized_event);
  // event type
  std::string evt_type;
  if (!getline(evt_ss, evt_type, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as IPCEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a IPCEvent.");
  }
  // node_name
  if (!getline(evt_ss, node_name, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as IPCEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a IPCEvent.");
  }
  // send_time
  if (!getline(evt_ss, send_time, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as IPCEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a IPCEvent.");
  }
  // src_pid
  std::string src_pid_str;
  if (!getline(evt_ss, src_pid_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as IPCEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a IPCEvent.");
  }
  src_pid = std::stoi(src_pid_str);
  // dst_pid
  std::string dst_pid_str;
  if (!getline(evt_ss, dst_pid_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as IPCEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a IPCEvent.");
  }
  dst_pid = std::stoi(dst_pid_str);
  // src_start_time_utc
  if (!getline(evt_ss, src_start_time_utc, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as IPCEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a IPCEvent.");
  }
  // dst_start_time_utc
  if (!getline(evt_ss, dst_start_time_utc, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as IPCEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a IPCEvent.");
  }
}

IPCEvent::IPCEvent(osm_pid_t src_pid, osm_pid_t dst_pid, std::string src_start_time_utc,
    std::string dst_start_time_utc) :
    src_pid { src_pid },
    dst_pid { dst_pid },
    src_start_time_utc { src_start_time_utc },
    dst_start_time_utc { dst_start_time_utc } {
}

std::string IPCEvent::serialize() const {
  std::stringstream evt;
  evt << get_type() << SER_DELIM
      << node_name << SER_DELIM
      << send_time << SER_DELIM;

  evt << src_pid << SER_DELIM
      << dst_pid << SER_DELIM
      << src_start_time_utc << SER_DELIM
      << dst_start_time_utc << SER_DELIM;

  return evt.str();
}

std::string IPCEvent::format_for_dst(ConsumerDestination c_dst) const {
  std::stringstream formatted;
  if (c_dst == CD_ODBC || c_dst == CD_FILE) {
    formatted << "IPCEvent"
      << "," << format_as_varchar(node_name, 128)
      << "," << std::to_string(src_pid)
      << "," << std::to_string(dst_pid)
      << "," << format_as_varchar(src_start_time_utc)
      << "," << format_as_varchar(dst_start_time_utc);
  } else {
    assert(!"Error, unsupported cdest\n");
  }
  return formatted.str();
}

std::string IPCEvent::get_value(std::string field) const {
  if (field == "src_pid")
    return std::to_string(src_pid);
  else if (field == "dst_pid")
    return std::to_string(dst_pid);
  else if (field == "src_start_time_utc")
    return src_start_time_utc;
  else if (field == "dst_start_time_utc")
    return dst_start_time_utc;
  else if (field == "type")
    return event_type_to_string[get_type()];
  else
    return "";
}

/*------------------------------
 * SocketEvent
 *------------------------------*/

SocketEvent::SocketEvent(const std::string &serialized_event) {
  std::stringstream evt_ss(serialized_event);
  // event type
  std::string evt_type;
  if (!getline(evt_ss, evt_type, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SocketEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SocketEvent.");
  }
  // node_name
  if (!getline(evt_ss, node_name, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SocketEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SocketEvent.");
  }
  // send_time
  if (!getline(evt_ss, send_time, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SocketEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SocketEvent.");
  }
  // pid
  std::string pid_str;
  if (!getline(evt_ss, pid_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SocketEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SocketEvent.");
  }
  pid = std::stoi(pid_str);
  // open_time
  if (!getline(evt_ss, open_time, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SocketEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SocketEvent.");
  }
  // close_time
  if (!getline(evt_ss, close_time, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SocketEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SocketEvent.");
  }
  // port
  std::string port_str;
  if (!getline(evt_ss, port_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SocketEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SocketEvent.");
  }
  port = std::stoi(port_str);
}

SocketEvent::SocketEvent(osm_pid_t pid, std::string open_time,
    std::string close_time, uint16_t port) :
    pid { pid },
    open_time { open_time },
    close_time { close_time },
    port { port } {
}

std::string SocketEvent::serialize() const {
  std::stringstream evt;
  evt << get_type() << SER_DELIM
      << node_name << SER_DELIM
      << send_time << SER_DELIM;

  evt << pid << SER_DELIM
      << open_time << SER_DELIM
      << close_time << SER_DELIM
      << port << SER_DELIM;

  return evt.str();
}

std::string SocketEvent::format_for_dst(ConsumerDestination c_dst) const {
  std::stringstream formatted;
  if (c_dst == CD_ODBC || c_dst == CD_FILE) {
    formatted << "SocketEvent"
      << "," << format_as_varchar(node_name, 128)
      << "," << std::to_string(pid)
      << "," << std::to_string(port)
      << "," << format_as_varchar(open_time)
      << "," << format_as_varchar(close_time);
  } else {
    assert(!"Error, unsupported cdest\n");
  }
  return formatted.str();
}

std::string SocketEvent::get_value(std::string field) const {
  if (field == "pid")
    return std::to_string(pid);
  else if (field == "port")
    return std::to_string(port);
  else if (field == "open_time")
    return open_time;
  else if (field == "close_time")
    return close_time;
  else if (field == "type")
    return event_type_to_string[get_type()];
  else
    return "";
}

/*------------------------------
 * SocketConnectEvent
 *------------------------------*/

SocketConnectEvent::SocketConnectEvent(const std::string &serialized_event) {
  std::stringstream evt_ss(serialized_event);
  // event type
  std::string evt_type;
  if (!getline(evt_ss, evt_type, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SocketConnectEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SocketConnectEvent.");
  }
  // node_name
  if (!getline(evt_ss, node_name, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SocketConnectEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SocketConnectEvent.");
  }
  // send_time
  if (!getline(evt_ss, send_time, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SocketConnectEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SocketConnectEvent.");
  }
  // pid
  std::string pid_str;
  if (!getline(evt_ss, pid_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SocketConnectEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SocketConnectEvent.");
  }
  pid = std::stoi(pid_str);
  // connect_time
  if (!getline(evt_ss, connect_time, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SocketConnectEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SocketConnectEvent.");
  }
  // dst_node
  if (!getline(evt_ss, dst_node, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SocketConnectEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SocketConnectEvent.");
  }
  // dst_port
  std::string dst_port_str;
  if (!getline(evt_ss, dst_port_str, ',')) {
    LOGGER_LOG_ERROR("Can't deserialize event " << serialized_event << " as SocketConnectEvent. Wrong format!");
    throw std::invalid_argument(serialized_event + " is not a SocketConnectEvent.");
  }
  dst_port = std::stoi(dst_port_str);
}

SocketConnectEvent::SocketConnectEvent(osm_pid_t pid, std::string connect_time,
    std::string dst_node, uint16_t dst_port) :
    pid { pid },
    connect_time { connect_time },
    dst_node { dst_node },
    dst_port { dst_port } {
}

std::string SocketConnectEvent::serialize() const {
  std::stringstream evt;
  evt << get_type() << SER_DELIM
      << node_name << SER_DELIM
      << send_time << SER_DELIM;

  evt << pid << SER_DELIM
      << connect_time << SER_DELIM
      << dst_node << SER_DELIM
      << dst_port << SER_DELIM;

  return evt.str();
}

std::string SocketConnectEvent::format_for_dst(ConsumerDestination c_dst) const {
  std::stringstream formatted;
  if (c_dst == CD_ODBC || c_dst == CD_FILE) {
    formatted << "SocketConnectEvent"
      << "," << format_as_varchar(node_name, 128)
      << "," << std::to_string(pid)
      << "," << std::to_string(dst_port)
      << "," << format_as_varchar(connect_time)
      << "," << format_as_varchar(dst_node);
  } else {
    assert(!"Error, unsupported cdest\n");
  }
  return formatted.str();
}

std::string SocketConnectEvent::get_value(std::string field) const {
  if (field == "pid")
    return std::to_string(pid);
  else if (field == "dst_port")
    return std::to_string(dst_port);
  else if (field == "connect_time")
    return connect_time;
  else if (field == "dst_node")
    return dst_node;
  else if (field == "type")
    return event_type_to_string[get_type()];
  else
    return "";
}
