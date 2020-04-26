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

#include "gtest/gtest.h"
#include "os-model.h"

TEST(os_model_test, test1) {
  OSModel os;

  // try the different syscalls and make sure we get the corresponding events
  // Syscall event structure
  // 1. Event Type (4 for Syscall event)
  // 2. Hostname
  // 3. Send time
  // 4. auditd event ID
  // 5. pid
  // 6. ppid
  // 7. uid
  // 8. gid
  // 9. euid
  // 10. egid
  // 11. syscall name
  // 12. return code
  // 13. arg0
  // 14. arg1
  // 15. arg2
  // 16. arg3
  // 17. arg4
  // 18. event time
  // 19. data (event specific data as comma separated list)
  std::string clone_event_str = "4,node1,2020/04/26-14:24:00.000,1,121,120,1010,2,1010,2,"
      "clone,122,,,,,,2020/04/26-14:24:00.000,";
  std::string exec_event_str = "4,node1,2020/04/26-14:24:01.000,2,122,121,1010,2,1010,2,"
      "execve,0,,,,,,2020/04/26-14:24:01.000,python,train.py,-i,input";
  std::string exit_group_event_str = "4,node1,2020/04/26-14:24:02.000,2,122,121,1010,2,1010,2,"
      "exit_group,0,,,,,,2020/04/26-14:24:02.000,";
  std::shared_ptr<Event> clone_event = Event::deserialize_event(clone_event_str);
  std::shared_ptr<Event> exec_event = Event::deserialize_event(exec_event_str);
  std::shared_ptr<Event> exit_group_event = Event::deserialize_event(exit_group_event_str);

  os.apply_syscall((SyscallEvent*) clone_event.get());
  os.apply_syscall((SyscallEvent*) exec_event.get());
  os.apply_syscall((SyscallEvent*) exit_group_event.get());


}
