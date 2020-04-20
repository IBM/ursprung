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

const std::string SER_DELIM = ",";

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

Event* Event::deserialize(const std::string &event) {
  Event *e = nullptr;
  // TODO implement deserialization
  return e;
}

/*------------------------------
 * TestEvent
 *------------------------------*/

TestEvent::TestEvent(std::string f1, std::string f2, std::string f3) :
    f1 { f1 },
    f2 { f2 },
    f3 { f3 } {
}

std::string TestEvent::serialize() const {
  std::stringstream evt;
  evt << get_type() << SER_DELIM
      << f1 << SER_DELIM
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

/*------------------------------
 * FSEvent
 *------------------------------*/

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

/*------------------------------
 * SyscallEvent
 *------------------------------*/

#ifdef __linux
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

std::string SyscallEvent::serialize() const {
  std::stringstream evt;
  evt << get_type() << SER_DELIM
      << node_name << SER_DELIM
      << send_time << SER_DELIM;

  evt << auditd_event_id << SER_DELIM
      << pid << SER_DELIM
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
  // TODO implement
  return "";
}

std::string SyscallEvent::get_value(std::string field) const {
  // TODO implement
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

std::string SyscallEvent::get_data_as_string() {
  std::string data;
  for (std::string item : data) {
    data += item + " ";
  }
  return data;
}
#endif

/*------------------------------
 * ProcessEvent
 *------------------------------*/

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
  // TODO implement
  return "";
}

std::string ProcessEvent::get_value(std::string field) const {
  // TODO implement
  return "";
}

/*------------------------------
 * ProcessGroupEvent
 *------------------------------*/

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
  // TODO implement
  return "";
}

std::string ProcessGroupEvent::get_value(std::string field) const {
  // TODO implement
  return "";
}

/*------------------------------
 * IPCEvent
 *------------------------------*/

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
  // TODO implement
  return "";
}

std::string IPCEvent::get_value(std::string field) const {
  // TODO implement
  return "";
}

/*------------------------------
 * SocketEvent
 *------------------------------*/

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
  // TODO implement
  return "";
}

std::string SocketEvent::get_value(std::string field) const {
  // TODO implement
  return "";
}

/*------------------------------
 * SocketEvent
 *------------------------------*/

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
  // TODO implement
  return "";
}

std::string SocketConnectEvent::get_value(std::string field) const {
  // TODO implement
  return "";
}
