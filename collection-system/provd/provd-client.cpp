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
#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <netdb.h>

#include "provd-client.h"
#include "provd.h"
#include "logger.h"

/*------------------------------
 * ProvdClient
 *------------------------------*/

int ProvdClient::connect_to_server(std::string node) {
  int rc;
  struct sockaddr_in serv_addr;

  if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    LOGGER_LOG_ERROR("Problems while creating socket: " << strerror(errno));
    return socket_fd;
  }

  serv_addr.sin_family = AF_INET;
  // TODO support custom provd port by adding a provd-consumer config
  serv_addr.sin_port = htons(DEFAULT_PORT);

  // convert hostname to IP address
  char ip[100];
  struct hostent *he;
  struct in_addr **addr_list;
  if ((he = gethostbyname(node.c_str())) == NULL) {
    // get the host info
    LOGGER_LOG_ERROR("Couldn't lookup " << node)
    return -1;
  }
  addr_list = (struct in_addr**) he->h_addr_list;
  strcpy(ip, inet_ntoa(*addr_list[0]));

  if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0) {
    LOGGER_LOG_ERROR("Invalid address provided: " << node);
    return -1;
  }

  if ((rc = connect(socket_fd, (struct sockaddr*) &serv_addr, sizeof(serv_addr))) < 0) {
    LOGGER_LOG_ERROR("Can't connect to server: " << strerror(errno));
    return rc;
  }

  return 0;
}

int ProvdClient::disconnect_from_server() {
  return close(socket_fd);
}

int ProvdClient::submit_trace_proc_request(int32_t pid, std::string regex_str) {
  int rc = 0;
  int16_t opcode = REQ_TRACE_PROCESS;

  // send opcode
  int16_t opcode_to_send = htons(opcode);
  char *bytes = (char*) &opcode_to_send;
  if ((rc = NetworkHelper::write_to_socket(socket_fd, bytes, sizeof(opcode_to_send))) < 0) {
    return rc;
  }

  // send first operand - pid
  int32_t operand_to_send = htonl(pid);
  bytes = (char*) &operand_to_send;
  if ((rc = NetworkHelper::write_to_socket(socket_fd, bytes, sizeof(operand_to_send))) < 0) {
    return rc;
  }

  // send second operand - regex
  char *regex = (char*) regex_str.c_str();
  // add 1 for the '\0' character
  int32_t len = regex_str.size() + 1;
  int32_t len_to_send = htonl(len);
  bytes = (char*) &len_to_send;
  if ((rc = NetworkHelper::write_to_socket(socket_fd, bytes, sizeof(len_to_send))) < 0) {
    return rc;
  }
  if ((rc = NetworkHelper::write_to_socket(socket_fd, regex, len)) < 0) {
    return rc;
  }

  return rc;
}

int ProvdClient::submit_stop_trace_proc_request(int32_t pid) {
  int rc = 0;
  uint16_t opcode = REQ_TRACE_PROCESS_STOP;

  // send opcode
  int16_t opcode_to_send = htons(opcode);
  char *bytes = (char*) &opcode_to_send;
  if ((rc = NetworkHelper::write_to_socket(socket_fd, bytes, sizeof(opcode_to_send))) < 0) {
    return rc;
  }

  // send first operand - pid
  int32_t operand_to_send = htonl(pid);
  bytes = (char*) &operand_to_send;
  if ((rc = NetworkHelper::write_to_socket(socket_fd, bytes, sizeof(operand_to_send))) < 0) {
    return rc;
  }

  return rc;
}

int ProvdClient::receive_line(std::string *line) {
  int32_t line_len;
  int bytes_read = NetworkHelper::read_from_socket(socket_fd, (char*) &line_len, sizeof(line_len));
  if (bytes_read == 0) {
    return 0;
  }

  char buffer[ntohl(line_len)];
  bytes_read = NetworkHelper::read_from_socket(socket_fd, buffer, ntohl(line_len));
  if (bytes_read == 0) {
    return 0;
  }

  *line = std::string(buffer);
  return bytes_read;
}

/*------------------------------
 * NetworkHelper
 *------------------------------*/

int NetworkHelper::write_to_socket(int fd, char *buffer, int len) {
  int rc = 0;
  int bytes_written = 0;

  while (len > 0) {
    if ((rc = write(fd, buffer, len)) < 0) {
      LOGGER_LOG_ERROR("Problems while writing: " << strerror(errno));
      return bytes_written;
    }
    bytes_written += rc;
    buffer += rc;
    len -= rc;
  }
  return bytes_written;
}

int NetworkHelper::read_from_socket(int fd, char *buffer, int len) {
  int rc = 0;
  int bytes_read = 0;

  while (len > 0) {
    if ((rc = read(fd, buffer, len)) < 0) {
      LOGGER_LOG_ERROR("Problems while writing: " << strerror(errno));
      return bytes_read;
    }
    if (rc == 0) {
      // server closed the connection
      return bytes_read;
    }
    bytes_read += rc;
    buffer += rc;
    len -= rc;
  }

  return bytes_read;
}
