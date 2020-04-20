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

#ifndef OS_EVENT_H
#define OS_EVENT_H

#include <string>
#include <vector>
#ifdef __linux__
#include <libaudit.h>
#include <auparse.h>
#endif

#include "intermediate-message.h"
#include "event.h"

class Event;

typedef uint32_t osm_pid_t;
typedef uint32_t osm_pgid_t;
typedef std::shared_ptr<Event> evt_t;

// different event types
enum EventType : int {
  FS_EVENT =              0x1,
  PROCESS_EVENT =         0x2,
  PROCESS_GROUP_EVENT =   0x3,
  SYSCALL_EVENT =         0x4,
  IPC_EVENT =             0x5,
  SOCKET_EVENT =          0x6,
  SOCKET_CONNECT_EVENT =  0x7,
  TEST_EVENT =            0x8
};

enum ConsumerSource {
  CS_PROV_GPFS = 0,
  CS_PROV_AUDITD = 1
};

enum ConsumerDestination {
  CD_DB2 = 0,
  CD_POSTGRES = 1,
  CD_ODBC
};

/**
 * Base class for any operating system related event.
 * Implementations need to provide conversion from/to
 * CSV format for (de)serialization.
 */
class Event {
protected:
  std::string node_name;
  std::string send_time;
  /**
   * Converts the specified string into a varchar formatted string
   * for insertion into a DB table.
   */
  std::string format_as_varchar(const std::string &str, int limit = -1) const;

public:
  Event() {}
  virtual ~Event() {}

  static Event* deserialize(const std::string &event);

  virtual std::string serialize() const =0;
  virtual std::string format_for_dst(ConsumerDestination c_dst) const =0;
  /**
   * Get the value for the specified message field.
   * If the field doesn't exist, this function returns
   * an empty string.
   */
  virtual std::string get_value(std::string field) const = 0;
  virtual EventType get_type() const =0;

};

class TestEvent: public Event {
private:
  static const int NUM_FIELDS = 3;
  std::string f1;
  std::string f2;
  std::string f3;

public:
  TestEvent(std::string f1, std::string f2, std::string f3);
  ~TestEvent() {}

  virtual std::string serialize() const override;
  virtual std::string format_for_dst(ConsumerDestination c_dst) const override;
  virtual std::string get_value(std::string field) const override;
  virtual EventType get_type() const override {
    return TEST_EVENT;
  }
};

class FSEvent: public Event {
private:
  osm_pid_t pid;
  int inode;
  size_t bytes_read;
  size_t bytes_written;

  std::string event;
  std::string event_time;
  std::string cluster_name;
  std::string fs_name;
  std::string path;
  std::string dst_path;
  std::string mode;
  std::string version_hash;

public:
  FSEvent(osm_pid_t pid, int inode, size_t bytes_read, size_t bytes_written,
      std::string event, std::string event_time, std::string cluster_name,
      std::string fs_name, std::string path, std::string dst_path,
      std::string mode, std::string version_hash);
  ~FSEvent() {}

  virtual std::string serialize() const override;
  virtual std::string format_for_dst(ConsumerDestination c_dst) const override;
  virtual std::string get_value(std::string field) const override;
  virtual EventType get_type() const override {
    return FS_EVENT;
  }
};

class SyscallEvent: public Event {
private:
  static const int RETURNS_VOID = -2;

  unsigned long auditd_event_id;
  int pid;
  int ppid;
  int uid;
  int gid;
  int euid;
  int egid;
  // might be RETURNS_VOID
  int rc;

  std::string syscall_name;
  std::string event_time;
  std::string arg0;
  std::string arg1;
  std::string arg2;
  std::string arg3;
  std::string arg4;

  /*
   * Field to hold additional data that was obtained from additional
   * records to the main SYSCALL record.
   *
   * For execve, data_[0] is cmdLine, and data_[1...N] are a0, a1, etc.
   * For pipe, data_[0] is fd0 and data_[1] is fd1.
   * For accept, connect, and bind, data_[0] is the socket address and data_[1] is the port
   */
  std::vector<std::string> data;

  /*
   * Private helper methods for processing auditd events using
   * libauparse.
   */
#ifdef __linux__
  std::vector<std::string> get_execve_args(auparse_state_t *au);
  std::vector<std::string> get_pipe_fds(auparse_state_t *au);
  std::vector<std::string> get_sockaddr(auparse_state_t *au);
  std::string get_data_as_string();
  std::string get_cwd(auparse_state_t *au);
#endif

public:
  /*
   * Creates a Syscall event from a raw auditd event using libauparse.
   * The au must be at a SYSCALL record before calling this constructor.
   * On return, the au is at the same record.
   */
#ifdef __linux__
  SyscallEvent(auparse_state_t *au);
#endif
  ~SyscallEvent() {};

  virtual std::string serialize() const override;
  virtual std::string format_for_dst(ConsumerDestination c_dst) const override;
  virtual std::string get_value(std::string field) const override;
  virtual EventType get_type() const override {
    return SYSCALL_EVENT;
  }
};

class ProcessEvent: public Event {
private:
  osm_pid_t pid;
  osm_pid_t ppid;
  // TODO for completeness, this should be a list of <pgid, timestamp> pairs
  osm_pgid_t pgid;

  // TODO for completeness, this should be a list of <X, timestamp> pairs
  // since exec calls can be nested
  std::string exec_cwd;
  std::vector<std::string> exec_cmd_line;
  std::string start_time_utc;
  std::string finish_time_utc;

public:
  ProcessEvent(osm_pid_t pid, osm_pid_t ppid, osm_pgid_t pgid,
      std::string exec_cwd, std::vector<std::string> exec_cmd_line,
      std::string start_time_utc, std::string finish_time_utc);
  ~ProcessEvent() {}

  virtual std::string serialize() const override;
  virtual std::string format_for_dst(ConsumerDestination c_dst) const override;
  virtual std::string get_value(std::string field) const override;
  virtual EventType get_type() const override {
    return PROCESS_EVENT;
  }
};

class ProcessGroupEvent: public Event {
private:
  osm_pgid_t pgid;
  std::string start_time_utc;
  std::string finish_time_utc;

public:
  ProcessGroupEvent(osm_pgid_t pgid, std::string start_time_utc, std::string finish_time_utc);
  ~ProcessGroupEvent() {}

  virtual std::string serialize() const override;
  virtual std::string format_for_dst(ConsumerDestination c_dst) const override;
  virtual std::string get_value(std::string field) const override;
  virtual EventType get_type() const override {
    return PROCESS_GROUP_EVENT;
  }
};

class IPCEvent: public Event {
private:
  osm_pid_t src_pid;
  osm_pid_t dst_pid;
  std::string src_start_time_utc;
  std::string dst_start_time_utc;

public:
  IPCEvent(const std::string &serialized_event);
  IPCEvent(osm_pid_t src_pid, osm_pid_t dst_pid, std::string src_start_time_utc,
      std::string dst_start_time_utc);
  ~IPCEvent() {}

  virtual std::string serialize() const override;
  virtual std::string format_for_dst(ConsumerDestination c_dst) const override;
  virtual std::string get_value(std::string field) const override;
  virtual EventType get_type() const override {
    return IPC_EVENT;
  }
};

class SocketEvent: public Event {
private:
  uint16_t port;
  osm_pid_t pid;
  std::string open_time;
  std::string close_time;

public:
  SocketEvent(const std::string &serialized_event);
  SocketEvent(osm_pid_t pid, std::string open_time, std::string close_time, uint16_t port);
  ~SocketEvent() {}

  virtual std::string serialize() const override;
  virtual std::string format_for_dst(ConsumerDestination c_dst) const override;
  virtual std::string get_value(std::string field) const override;
  virtual EventType get_type() const override {
    return SOCKET_EVENT;
  }
};

class SocketConnectEvent: public Event {
private:
  osm_pid_t pid;
  std::string connect_time;
  std::string dst_node;
  uint16_t dst_port;

public:
  SocketConnectEvent(const std::string &serialized_event);
  SocketConnectEvent(osm_pid_t pid, std::string connect_time,
      std::string dst_node, uint16_t dst_port);
  ~SocketConnectEvent() {}

  virtual std::string serialize() const override;
  virtual std::string format_for_dst(ConsumerDestination c_dst) const override;
  virtual std::string get_value(std::string field) const override;
  virtual EventType get_type() const override {
    return SOCKET_EVENT;
  }
};

#endif // OS_EVENT_H
