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

#ifndef PROVD_PROVD_H_
#define PROVD_PROVD_H_

#include <sys/socket.h>
#include <netinet/in.h>
#include <thread>
#include <map>
#include <regex>
#include <string>
#include <sstream>

#define DEFAULT_PORT 7531
#define BACKLOG 1000

// request Types the provd server can process
#define REQ_TRACE_PROCESS       0x0001
#define REQ_TRACE_PROCESS_STOP  0x0002

class ReqHandler;

class ProvdServer {
private:
  bool running = true;
  int socketfd;
  struct sockaddr_in address;
  std::map<int32_t, ReqHandler*> workers;

  /*
   * Receives and dispatches a REQ_TRACE_PROCESS request according to
   * its protocol. The protocol is:
   *
   * 4 bytes containing pid
   * 4 bytes containing msg length (N)
   *   (we assume that the length includes the \0 character at the end of the regex)
   * N bytes containing the regex to search for in the process' stdout
   */
  void dispatch_trace_process_req(int sock);
  /*
   * Receives and handles a REQ_TRACE_PROCESS_STOP request according
   * to its protocol. The protocol is:
   *
   * 4 bytes containing pid
   *
   * This function does not dispatch a thread itself but rather just send a
   * signal to the corresponding worker thread to signal it to stop.
   */
  void dispatch_trace_process_stop_req(int sock);

public:
  ProvdServer();
  ~ProvdServer();
  ProvdServer(const ProvdServer&) = delete;
  ProvdServer& operator=(const ProvdServer &x) = delete;

  /*
   * Main request server loop which waits for incoming client connections,
   * reads the request and its corresponding arguments, and dispatches a thread
   * to handle the request.
   */
  void start();
  void stop() { running = false; }
};

class ReqHandler {
private:
  std::thread thr;

protected:
  bool running = true;
  int sock;

public:
  /*
   * A request handler receives a socket descriptor as a constructor
   * argument to be able to communicate with the corresponding client.
   * The request handler is responsible for closing the socket once
   * no more communication is required.
   */
  ReqHandler(int sock) : sock { sock } {}
  virtual ~ReqHandler() {}

  virtual void handle() =0;

  void run() { thr = std::thread(&ReqHandler::handle, this); }
  void stop() { running = false; }
  bool is_running() { return running; }
  std::string get_thread_id() const {
    auto id = thr.get_id();
    std::stringstream ss;
    ss << id;
    return ss.str();
  }
  void join() { thr.join(); }
};

class TraceProcessReqHandler: public ReqHandler {
private:
  const std::string tracee_out_base_path = "/tmp/stdout";
  int32_t tracee_pid;
  std::string regex_str;
  std::regex matching_str;

public:
  TraceProcessReqHandler(int sock, int32_t pid, std::string regexStr) :
      ReqHandler(sock), tracee_pid { pid }, regex_str { regexStr } {
    matching_str = std::regex(regexStr);
  }
  virtual ~TraceProcessReqHandler() {}

  virtual void handle() override;
};

#endif /* PROVD_PROVD_H_ */
