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
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h>
#include <sstream>
#include <string>

#include "provd.h"
#include "provd-client.h"
#include "logger.h"
#include "config.h"
#include "signal-handling.h"

extern "C" {
#include <libptrace_do.h>
}

#define ESC 27

/*------------------------------
 * ProvdServer
 *------------------------------*/

ProvdServer::ProvdServer() {
  int enable = 1;

  if ((socketfd = socket(AF_INET, SOCK_STREAM, 0)) <= 0) {
    LOG_ERROR("Can't create server socket: " << strerror(errno));
  }
  if (setsockopt(socketfd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &enable, sizeof(enable))) {
    LOG_ERROR("Can't set SO_REUSEADDR on server socket: " << strerror(errno));
  }

  address.sin_family = AF_INET;
  address.sin_port = htons((uint16_t ) std::stoi(Config::config[Config::CKEY_PROVD_PORT]));
  address.sin_addr.s_addr = INADDR_ANY;

  if (bind(socketfd, (const sockaddr*) &address, sizeof(address)) < 0) {
    LOG_ERROR("Can't bind server socket: " << strerror(errno));
  }
}

ProvdServer::~ProvdServer() {
  for (std::pair<int32_t, ReqHandler*> worker : workers) {
    LOG_INFO("Waiting for worker " << worker.second->getThreadId() << " to finish...");
    if (worker.second->is_running()) {
      // signal the thread to stop in case it is still running
      worker.second->stop();
    }
    // wait for thread to finish
    worker.second->join();
    delete worker.second;
    LOG_INFO("Worker finished. provd shutting down.");
  }
}

void ProvdServer::start() {
  int clientfd;
  int address_len = sizeof(address);
  ssize_t bytes_read;
  int16_t opcode;

  listen(socketfd, 1000);
  LOG_INFO("Server Listening on " << ntohs(address.sin_port));

  // Server can be stopped from the outside by receiving a SIGINT/SIGTERM
  // or by the owner of the ProvdServer object by calling stop() and setting
  // running to false.
  while (running && signal_handling::running) {
    if ((clientfd = accept(socketfd, (struct sockaddr*) &address,
        (socklen_t*) &address_len)) < 0) {
      if (errno == EINTR && signal_handling::running) {
        LOG_INFO("Received shutdown signal, shutting down...");
        // any clean up should go here
        break;
      } else {
        LOG_ERROR("Problems while accepting new incoming connection: " << strerror(errno));
      }
    }

    // read the request type
    bytes_read = NetworkHelper::read_from_socket(clientfd, (char*) &opcode, sizeof(opcode));
    if (bytes_read != 2) {
      LOG_ERROR("Wrong opcode received (" << bytes_read << " "
          << ntohs(opcode) << "). Not processing request.");
      continue;
    }

    // read operands for different requests
    switch (ntohs(opcode)) {
    case REQ_TRACE_PROCESS:
      LOG_INFO("Received trace process request.");
      dispatch_trace_process_req(clientfd);
      break;
    case REQ_TRACE_PROCESS_STOP:
      LOG_INFO("Received trace process STOP request.");
      dispatch_trace_process_stop_req(clientfd);
      break;
    default:
      LOG_INFO("Received unknown request " << ntohs(opcode) << ".");
    }
  }
  LOG_INFO("Server finished, shutting down.");
}

void ProvdServer::dispatch_trace_process_req(int sock) {
  // retrieve missing arguments
  int32_t pid;
  int32_t msg_len;
  NetworkHelper::read_from_socket(sock, (char*) &pid, sizeof(pid));
  NetworkHelper::read_from_socket(sock, (char*) &msg_len, sizeof(msg_len));
  char buffer[ntohl(msg_len)];
  NetworkHelper::read_from_socket(sock, buffer, ntohl(msg_len));
  std::string regex_str(buffer);

  // check if we have previously registered a handler
  if (workers.find(pid) != workers.end()) {
    // clean up the previous entry
    delete workers[pid];
  }

  // create handler thread and run it
  TraceProcessReqHandler *handler = new TraceProcessReqHandler(sock, ntohl(pid), regex_str);
  handler->run();
  workers[pid] = handler;
}

void ProvdServer::dispatch_trace_process_stop_req(int sock) {
  int32_t pid;
  NetworkHelper::read_from_socket(sock, (char*) &pid, sizeof(pid));
  if (workers[pid]) {
    workers[pid]->stop();
  }
  close(sock);
}

/*------------------------------
 * TraceProcessReqHandler
 *------------------------------*/

void TraceProcessReqHandler::handle() {
  LOG_INFO("Read pid " << tracee_pid << " and regex " << regexStr);
  uint buffer_size = 1024;
  char *buffer;
  char buffer_cpy[buffer_size];

  /*
   * Here, we attach to the tracee and then allocate some memory in
   * the tracee's address space. We copy the file name to which the
   * tracee's stdout should be redirected to that allocated memory
   * and then open that file, dup stdout to that new file, and then close
   * the file again.
   */
  // check if the process still exists
  if (kill(tracee_pid, 0) < 0) {
    LOG_WARN("Process " << tracee_pid << " doesn't exist anymore. Not tracing.");
    close(sock);
    return;
  }

  // TODO add error handling for all ptrace calls
  struct ptrace_do *target = ptrace_do_init(tracee_pid);
  buffer = (char*) ptrace_do_malloc(target, buffer_size);
  memset(buffer, 0, buffer_size);
  snprintf(buffer, buffer_size, "%s-%d", tracee_out_base_path, tracee_pid);
  snprintf(buffer_cpy, buffer_size, "$s-%d", tracee_out_base_path, tracee_pid);
  void *remote_addr = ptrace_do_push_mem(target, buffer);

  int remote_fd = ptrace_do_syscall(target, __NR_open, (unsigned long) remote_addr,
      O_CREAT | O_WRONLY | O_TRUNC, 0644, 0, 0, 0);
  ptrace_do_syscall(target, __NR_dup2, remote_fd, 1, 0, 0, 0, 0);
  ptrace_do_syscall(target, __NR_dup2, remote_fd, 2, 0, 0, 0, 0);
  ptrace_do_syscall(target, __NR_close, remote_fd, 0, 0, 0, 0, 0);
  ptrace_do_cleanup(target);

  // Now we have to read the file and look for provenance records
  int local_fd = open(buffer_cpy, O_RDONLY);
  if (local_fd <= 0) {
    LOG_ERROR("Problems while opening file: " << local_fd << " " << strerror(errno));
  }

  LOG_INFO("Start reading file " << buffer_cpy);
  int rc;
  uint offset = 0;
  off_t size = 0;
  struct stat statbuf;
  bool control_seq = false;

  while (running) {
    sleep(1);
    // stat the file to get the current size
    if ((rc = fstat(local_fd, &statbuf)) != 0) {
      LOG_ERROR("Problems while stating " << buffer_cpy << ": " << strerror(errno));
    }
    // if size didn't change since last stat, go back to sleep
    if (statbuf.st_size == size) {
      continue;
    }

    // if size changed, read the new content
    size = statbuf.st_size;
    ssize_t bytes_read;
    uint file_buffer_size = 8192;
    // we assume a line does not contain more than 4K characters
    uint line_buffer_size = 4096;
    char file_buffer[file_buffer_size];
    char line_buffer[line_buffer_size];

    while ((bytes_read = read(local_fd, file_buffer, file_buffer_size)) > 0) {
      for (size_t i = 0; i < bytes_read; i++) {
        // bounds check
        if (offset >= line_buffer_size) {
          LOG_ERROR("Found line longer than 4096, resetting buffer. "
              << "Some provenance may be lost.");
          offset = 0;
        }

        // deal with control characters
        if (iscntrl(file_buffer[i]) && file_buffer[i] != '\n') {
          if (file_buffer[i] == ESC) {
            // set state to check, whether we have a control sequence
            control_seq = true;
          }
          continue;
        }
        if (control_seq) {
          control_seq = false;
          if (file_buffer[i] == '[') {
            // We're entering a control sequence, fast forward to the end of it.
            // A control sequence ends with one of the following letters:
            // A - H (65 - 72), J, K, S, or T.
            while ((file_buffer[i] < 65 || file_buffer[i] > 72)
                && file_buffer[i] != 'J' && file_buffer[i] != 'K'
                && file_buffer[i] != 'S' && file_buffer[i] != 'T') {
              i++;
            }
            continue;
          }
        }

        // copy current line content to lineBuffer
        line_buffer[offset] = file_buffer[i];
        offset++;
        // once we find line feed, finish the current line
        if (file_buffer[i] == '\n') {
          line_buffer[offset - 1] = '\0';
          std::string line(line_buffer);
          LOG_DEBUG("Read line " << line << " with size " << line.size());

          // check if the line matches
          if (std::regex_match(line, matching_str)) {
            LOG_DEBUG("Found match in " << line);
            // send matching line to client
            int32_t len = line.size();
            int32_t len_to_send = htonl(len);
            char *bytes = (char*) &len_to_send;
            if ((rc = NetworkHelper::write_to_socket(sock, bytes, sizeof(len_to_send))) < 0) {
              LOG_ERROR("Problems while writing: " << strerror(errno));
            }
            if ((rc = NetworkHelper::write_to_socket(sock, (char*) line.c_str(),
                line.size())) < 0) {
              LOG_ERROR("Problems while writing: " << strerror(errno));
            }
          }
          offset = 0;
        }
      }
    }
    if (bytes_read < 0) {
      LOG_ERROR("Problems while reading file: " << strerror(errno));
    }
  }

  LOG_INFO("Stop reading file " << buffer_cpy);
  if (remove(buffer_cpy) != 0) {
    LOG_ERROR("Problems while deleting file " << buffer_cpy << ": " << strerror(errno));
  }
  close(sock);
}

/*------------------------------
 * main
 *------------------------------*/

int main(int argc, char **argv) {
  std::cout << "-----------------------------------------------------------" << std::endl <<
               "              Ursprung Provenance Daemon                   " << std::endl <<
               "-----------------------------------------------------------" << std::endl;

  // parse and check command line args
  if (argc != 2) {
    fprintf(stderr, "Error, usage: %s configFile\n", argv[0]);
    return -1;
  }
  struct stat statBuf;
  if (stat(argv[1], &statBuf)) {
    fprintf(stderr, "Error, no such configFile %s", argv[1]);
    return -1;
  }

  // initialize config and logger
  Config::parse_config(argv[1]);
  Logger::set_log_file_name(Config::config[Config::CKEY_LOG_FILE]);
  signal_handling::setup_handlers();

  // start the main loop
  ProvdServer server = ProvdServer();
  server.start();

  delete server;
  return 0;
}
