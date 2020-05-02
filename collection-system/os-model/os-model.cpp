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

#include "../os-model/os-model.h"

#include <assert.h>

#include "logger.h"

std::map<std::string, osm_syscall_t> string_to_syscall {
  { "clone", osm_syscall_clone },
  { "execve", osm_syscall_execve },
  { "setpgid", osm_syscall_setpgid },
  { "exit", osm_syscall_exit },
  { "exit_group", osm_syscall_exit_group },
  { "vfork", osm_syscall_vfork },
  { "pipe", osm_syscall_pipe },
  { "close", osm_syscall_close },
  { "dup2", osm_syscall_dup2 },
  { "socket", osm_syscall_socket },
  { "connect", osm_syscall_connect },
  { "bind", osm_syscall_bind },
};

OSModel::OSModel() : pt { } {}

osm_rc_t OSModel::apply_syscall(SyscallEvent *se) {
  assert(se);
  LOGGER_LOG_DEBUG("OSModel::applySyscall: Applying syscall: " << se->serialize());

  // ignore failed syscalls
  if (se->rc != SyscallEvent::RETURNS_VOID && se->rc < 0 && se->rc != -115) {
    LOGGER_LOG_ERROR("OSModel::applySyscall: Ignoring failed syscall for pid "
        << se->pid << ": " << se->syscall_name << " rc " << se->rc);
    delete se;
    return osm_rc_ok;
  }

  // save se, we own it and must delete it later
  appliedSyscalls.push_back(se);

  if (string_to_syscall.find(se->syscall_name) != string_to_syscall.end()) {
    switch (string_to_syscall[se->syscall_name]) {
    // process-related syscalls
    case osm_syscall_clone:
    case osm_syscall_execve:
    case osm_syscall_setpgid:
    case osm_syscall_exit:
    case osm_syscall_exit_group:
    case osm_syscall_vfork:
    case osm_syscall_pipe:
    case osm_syscall_close:
    case osm_syscall_dup2:
    case osm_syscall_socket:
    case osm_syscall_connect:
    case osm_syscall_bind:
      pt.apply_syscall(se);
      break;
    default:
      // ignore unmodeled syscalls
      LOGGER_LOG_DEBUG("OSModel::applySyscall: Unmodeled syscall: " << se->serialize());
      break;
    }
  }

  return osm_rc_ok;
}

std::vector<Event*> OSModel::reap_os_events() {
  std::vector<Event*> ret;

  // get all raw syscalls first
  ret.reserve(ret.size() + appliedSyscalls.size());
  ret.insert(ret.end(), appliedSyscalls.begin(), appliedSyscalls.end());
  appliedSyscalls.clear();

  // then get all aggregated events (process, process group, etc.)
  std::vector<Event*> processEvents = pt.reap_os_events();
  ret.reserve(ret.size() + processEvents.size());
  ret.insert(ret.end(), processEvents.begin(), processEvents.end());

  for (auto e : ret) {
    assert(e);
  }

  return ret;
}
