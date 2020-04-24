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

#ifndef PROV_AUDITD_OS_COMMON_H_
#define PROV_AUDITD_OS_COMMON_H_

#include <map>
#include <string>

typedef enum osm_syscall {
  osm_syscall_clone,
  osm_syscall_execve,
  osm_syscall_setpgid,
  osm_syscall_exit,
  osm_syscall_exit_group,
  osm_syscall_vfork,
  osm_syscall_pipe,
  osm_syscall_close,
  osm_syscall_dup2,
  osm_syscall_socket,
  osm_syscall_connect,
  osm_syscall_bind
} osm_syscall_t;

// global map (defined in os-model.cpp) to map syscall strings to osm_syscall_t
extern std::map<std::string, osm_syscall_t> string_to_syscall;

// return codes
typedef enum osm_rc {
  osm_rc_ok,
  osm_rc_slowdown
} osm_rc_t;

#endif /* PROV_AUDITD_OS_COMMON_H_ */
