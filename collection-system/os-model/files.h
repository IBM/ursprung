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

#ifndef PROV_AUDITD_FILES_H_
#define PROV_AUDITD_FILES_H_

#include <map>
#include <iostream>
#include <memory>

#include "auditd-event.h"

class OpenFile;

typedef enum osm_fd {
  /* Needed for empty file descriptors. */
  osm_fd_none,
  osm_fd_file,
  /*
   * We need to distinugish between the read and write end of a
   * pipe as a pipe only supports unidirectional communication.
   */
  osm_fd_pipe_read,
  osm_fd_pipe_write,
  osm_fd_socket
} osm_fd_t;

class OpenFile {
public:
  enum OSFileType {
    OS_FILE_TYPE_FILE,
    OS_FILE_TYPE_SOCKET,
    OS_FILE_TYPE_PIPE
  };
  OpenFile() {}
  virtual ~OpenFile() {}

  virtual OSFileType get_type() const =0;
  virtual std::string str() const =0;
};

class Pipe: public OpenFile {
private:
  osm_pid_t reader;
  osm_pid_t writer;
  std::string reader_birth;
  std::string writer_birth;

public:
  Pipe() :
    reader { -1 },
    writer { -1 },
    reader_birth { "" },
    writer_birth { "" } {}
  ~Pipe() {}

  void set_reader_process(osm_pid_t pid, std::string birth_time) {
    reader = pid;
    reader_birth = birth_time;
  }
  void set_writer_process(osm_pid_t pid, std::string birthTime) {
    writer = pid;
    writer_birth = birthTime;
  }
  /*
   * Converts the pipe to an IPC event. If the pipe is not complete, i.e.
   * it does not have both a correct reader and writer set (either or both
   * are -1), this function will return NULL.
   */
  IPCEvent* to_ipc_event();
  OSFileType get_type() const override { return OSFileType::OS_FILE_TYPE_PIPE; }
  std::string str() const override;
};

class Socket: public OpenFile {
private:
  osm_pid_t local_pid;
  std::string open_time;
  std::string connect_time;
  std::string close_time;
  std::string local_addr;
  std::string remote_addr;
  uint16_t local_port;
  uint16_t remote_port;
  bool connected;
  bool bound;

public:
  /* Store mapping between IP Addresses and hostnames to speed up socket connects. */
  static std::map<std::string, std::string> reverse_dns_cache;

  Socket() :
      local_pid { -1 },
      open_time { "" },
      connect_time { "" },
      close_time { "" },
      local_addr { "" },
      remote_addr { "" },
      local_port { 0 },
      remote_port { 0 },
      connected { false },
      bound { false } {}
  ~Socket() {}

  void open(osm_pid_t pid, std::string time) {
    local_pid = pid;
    open_time = time;
  }
  void bind(uint16_t port) {
    local_port = port;
    bound = true;
  }
  void connect(std::string addr, uint16_t port, std::string time);
  void close(std::string time) { close_time = time; }
  osm_pid_t get_local_pid() const { return local_pid; }
  uint16_t get_local_port() const { return local_port; }
  uint16_t get_remote_port() const { return remote_port; }
  std::string get_local_addr() const { return local_addr; }
  std::string get_remote_addr() const { return remote_addr; }
  std::string get_open_time() const { return open_time; }
  std::string get_close_time() const { return close_time; }
  bool has_connected() const { return connected; }
  bool is_bound() const { return bound; }

  /*
   * Converts the socket to a Socket event. If the socket has not
   * been bound to a specific address, this function returns null.
   */
  SocketEvent* to_socket_event();
  /*
   * Converts the socket to a Socket event. If the socket didn't make
   * any connection, this function returns null.
   */
  SocketConnectEvent* to_socket_connect_event();
  OSFileType getType() const override { return OSFileType::OS_FILE_TYPE_SOCKET; }
  std::string str() const override;
};

class FileDescriptor {
private:
  osm_fd_t type;
  int fd;
  std::shared_ptr<OpenFile> target_file;

public:
  /*
   * We need a default constructor so we can access FileDescriptors
   * in a map using the [] operator (as is done, e.g., in ProcessTable::pipe).
   */
  FileDescriptor() :
      type { osm_fd_none },
      fd { -1 } {}
  FileDescriptor(osm_fd_t type, int fd, std::shared_ptr<OpenFile> target_file) :
      type { type },
      fd { fd },
      target_file { target_file } {}

  osm_fd_t get_type() const { return type; }
  int get_fd() const { return fd; }
  /*
   * As we use the use_count of the shared_ptr as a reference count for how many
   * times the target_file is referenced by a file descriptor, we only ever create
   * a copy of target_file if the entire file descriptor is copied (mainly through
   * inheritance during a fork). This means that we can't return the shared_ptr
   * here as otherwise, this would create a copy and change the use_count, and instead,
   * we return the actual OpenFile pointer. However, users of this function should not
   * assume that the returned pointer will remain valid as it is managed through a shared_ptr.
   */
  OpenFile* get_target_file() const { return target_file.get(); }
  long get_target_file_references() const { return target_file.use_count(); }
  long get_target_file_type() const { return target_file.get()->get_type(); }
  std::string str() const;
};

#endif /* PROV_AUDITD_FILES_H_ */
