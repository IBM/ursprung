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

#ifndef OS_MODEL_H
#define OS_MODEL_H

#include <map>
#include <list>
#include <vector>
#include <set>
#include <string>

#include "auditd-event.h"
#include "os-common.h"
#include "processes.h"

/**
 * OSModel models an OS. It replays a syscall trace to track higher-level objects
 * (e.g. Processes). It consists of a ProcessTable, which tracks the set of live
 * processes.
 */
class OSModel {
private:
  ProcessTable pt;
  std::vector<SyscallEvent*> appliedSyscalls;

public:
  OSModel();

  /* Apply this syscall to the existing model. This OSModel is now the owner of se. */
  osm_rc_t apply_syscall(SyscallEvent *se);
  /* Return completed OS events. Caller is responsible for cleaning them up. */
  std::vector<Event*> reap_os_events();
};

#endif // OS_MODEL_H
