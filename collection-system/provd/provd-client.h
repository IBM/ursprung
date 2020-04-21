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

#ifndef PROVD_PROVD_CLIENT_H_
#define PROVD_PROVD_CLIENT_H_

#include <string>

class ProvdClient {
private:
  int socket_fd;

public:
  int connect_to_server(std::string node);
  int disconnect_from_server();
  int submit_trace_proc_request(int32_t pid, std::string regex_str);
  int submit_stop_trace_proc_request(int32_t pid);
  int receive_line(std::string *line);
};

class NetworkHelper {
public:
  static int write_to_socket(int fd, char *buffer, int len);
  static int read_from_socket(int fd, char *buffer, int len);
};

#endif /* PROVD_PROVD_CLIENT_H_ */
