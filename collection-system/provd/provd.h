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

class ProvdConfig {
private:
    std::string logFileName;
    uint16_t port;
public:
    ProvdConfig(std::string configFile);
    ~ProvdConfig() {};
    std::string getLogFileName() const { return logFileName; };
    uint16_t getPort() const { return port; };
};

class ProvdServer {
private:
    bool running = true;
    int socketfd;
    struct sockaddr_in address;
    std::map<int32_t, ReqHandler*> workers;

    void dispatchTraceProcessReq(int sock);
    void dispatchTraceProcessStopReq(int sock);
public:
    ProvdServer(const ProvdConfig& conf);
    ~ProvdServer();
    ProvdServer(const ProvdServer&) = delete;
    ProvdServer& operator=(const ProvdServer& x) = delete;
    void start();
    void stop() { running = false; };
};

class ReqHandler {
private:
    std::thread thr;
protected:
    bool running = true;
    int sock;
public:
    /**
     * A request handler receives a socket descriptor as a constructor
     * argument to be able to communicate with the corresponding client.
     * The request handler is responsible for closing the socket once
     * no more communication is required.
     */
    ReqHandler(int sock) : sock {sock} {};
    void run() {
        thr = std::thread(&ReqHandler::handle, this);
    }
    void stop() {
        running = false;
    }
    bool isRunning() {
        return running;
    }
    std::string getThreadId() const {
        auto id = thr.get_id();
        std::stringstream ss;
        ss << id;
        return ss.str();
    }
    void join() { thr.join(); }
    virtual void handle()=0;
    virtual ~ReqHandler() {};
};

class TraceProcessReqHandler : public ReqHandler {
private:
    int32_t traceePid;
    std::string regexStr;
    std::regex matchingStr;
public:
    TraceProcessReqHandler(int sock, int32_t pid, std::string regexStr) :
            ReqHandler(sock),
            traceePid {pid},
            regexStr {regexStr} {
        matchingStr = std::regex(regexStr);
    }
    virtual ~TraceProcessReqHandler() {};
    virtual void handle() override;
};

#endif /* PROVD_PROVD_H_ */
