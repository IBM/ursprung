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

#include <cstring>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "files.h"
#include "logger.h"

/*------------------------------
 * Pipe
 *------------------------------*/

std::string Pipe::str() const {
  return "[" + std::to_string(writer) + "->" + std::to_string(reader) + "]";
}

IPCEvent* Pipe::to_ipc_event() {
  if (writer != -1 && reader != -1) {
    return new IPCEvent(writer, reader, writer_birth, reader_birth);
  } else {
    return nullptr;
  }
}

/*------------------------------
 * Socket
 *------------------------------*/

std::map<std::string, std::string> Socket::reverse_dns_cache;

void Socket::connect(std::string addressStr, uint16_t port, std::string time) {
  remote_port = port;
  connect_time = time;
  connected = true;

  // check if we have resolved this address before
  if (Socket::reverse_dns_cache.find(addressStr) != Socket::reverse_dns_cache.end()) {
    remote_addr = Socket::reverse_dns_cache[addressStr];
    return;
  }
  remote_addr = addressStr;

  // resolve IP address to hostname
  int bufferLen = 256;
  char hostname[bufferLen];
  char service[bufferLen];
  sockaddr_in address;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = inet_addr(remote_addr.c_str());
  int rc = getnameinfo((sockaddr*) &address, sizeof(address), hostname, 260,
      service, 260, 0);
  // only set hostname if we successfully resolve IP address
  if (rc != 0) {
    LOG_ERROR("Couldn't resolve " << remote_addr << " due to " << gai_strerror(rc));
  } else {
    // use only name portion not FQDN
    char *firstDot = strchr(hostname, '.');
    if (firstDot) {
        *firstDot = '\0';
    }
    remote_addr = std::string(hostname);
    // cache entry
    Socket::reverse_dns_cache[addressStr] = remote_addr;
  }
}

std::string Socket::str() const {
  int bufferLen = 1024;
  char buffer[bufferLen];
  snprintf(buffer, bufferLen, "Local: pid %d open %s close %s addr %s:%d -- Remote: %s:%d",
      local_pid, open_time.c_str(), close_time.c_str(), local_addr.c_str(), local_port,
      remote_addr.c_str(), remote_port);
  return std::string(buffer);
}

SocketEvent* Socket::to_socket_event() {
  if (is_bound()) {
    return new SocketEvent(local_pid, open_time, close_time, local_port);
  } else {
    return nullptr;
  }
}

SocketConnectEvent* Socket::to_socket_connect_event() {
  if (has_connected()) {
    return new SocketConnectEvent(local_pid, connect_time, remote_addr, remote_port);
  } else {
    return nullptr;
  }
}

/*------------------------------
 * FileDescriptor
 *------------------------------*/

std::string FileDescriptor::str() const {
  return std::to_string(fd) + "/" + target_file.get()->str();
}
