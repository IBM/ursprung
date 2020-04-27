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

TEST(os_model_test, test_fork_exec_exit) {
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
  std::string setpgid_event_str = "4,node1,2020/04/26-14:24:00.500,2,122,121,1010,2,1010,2,"
      "setpgid,0,0,0,,,,2020/04/26-14:24:00.500,";
  std::string exec_event_str = "4,node1,2020/04/26-14:24:01.000,3,122,121,1010,2,1010,2,"
      "execve,0,,,,,,2020/04/26-14:24:01.000,python,train.py,-i,input";
  std::string exit_group_event_str = "4,node1,2020/04/26-14:24:02.000,4,122,121,1010,2,1010,2,"
      "exit_group,0,,,,,,2020/04/26-14:24:02.000,";

  std::shared_ptr<Event> clone_event = Event::deserialize_event(clone_event_str);
  std::shared_ptr<Event> setpgid_event = Event::deserialize_event(setpgid_event_str);
  std::shared_ptr<Event> exec_event = Event::deserialize_event(exec_event_str);
  std::shared_ptr<Event> exit_group_event = Event::deserialize_event(exit_group_event_str);

  os.apply_syscall((SyscallEvent*) clone_event.get());
  os.apply_syscall((SyscallEvent*) setpgid_event.get());
  os.apply_syscall((SyscallEvent*) exec_event.get());
  os.apply_syscall((SyscallEvent*) exit_group_event.get());

  std::vector<Event*> events = os.reap_os_events();
  EXPECT_EQ(6, events.size());
  EXPECT_EQ("2,,,122,121,122,2020/04/26-14:24:00.000,2020/04/26-14:24:02.000,python,"
      "train.py,-i,input,", events[4]->serialize());
  EXPECT_EQ("3,,,122,2020/04/26-14:24:00.500,2020/04/26-14:24:02.000,", events[5]->serialize());
}

TEST(os_model_test, test_pipe_ipc) {
  OSModel os;

  std::string setpgid_event1_str = "4,node1,2020/04/26-14:24:00.000,1,121,120,1010,2,1010,2,"
      "setpgid,0,0,0,,,,2020/04/26-14:24:00.000,";
  std::string pipe_event_str = "4,node1,2020/04/26-14:24:00.500,2,121,120,1010,2,1010,2,"
      "pipe,0,0,0,,,,2020/04/26-14:24:00.500,3,4";
  std::string clone_event1_str = "4,node1,2020/04/26-14:24:01.100,3,121,120,1010,2,1010,2,"
      "clone,122,,,,,,2020/04/26-14:24:01.100,";
  std::string clone_event2_str = "4,node1,2020/04/26-14:24:01.200,4,121,120,1010,2,1010,2,"
      "clone,123,,,,,,2020/04/26-14:24:01.200,";
  std::string setpgid_event2_str = "4,node1,2020/04/26-14:24:01.310,5,121,120,1010,2,1010,2,"
      "setpgid,0,7A,79,,,,2020/04/26-14:24:01.310,";
  std::string setpgid_event3_str = "4,node1,2020/04/26-14:24:01.320,6,121,120,1010,2,1010,2,"
      "setpgid,0,7B,79,,,,2020/04/26-14:24:01.320,";
  std::string dup_event1_str = "4,node1,2020/04/26-14:24:02.000,7,122,121,1010,2,1010,2,"
      "dup2,0,3,0,,,,2020/04/26-14:24:02.000,";
  std::string dup_event2_str = "4,node1,2020/04/26-14:24:02.500,8,123,121,1010,2,1010,2,"
      "dup2,0,4,1,,,,2020/04/26-14:24:02.500,";
  std::string close_event1_str = "4,node1,2020/04/26-14:24:03.000,9,122,121,1010,2,1010,2,"
      "close,0,3,,,,,2020/04/26-14:24:03.000,";
  std::string close_event2_str = "4,node1,2020/04/26-14:24:03.100,10,122,121,1010,2,1010,2,"
      "close,0,4,,,,,2020/04/26-14:24:03.100,";
  std::string close_event3_str = "4,node1,2020/04/26-14:24:03.200,11,123,121,1010,2,1010,2,"
      "close,0,3,,,,,2020/04/26-14:24:03.200,";
  std::string close_event4_str = "4,node1,2020/04/26-14:24:03.300,12,123,121,1010,2,1010,2,"
      "close,0,4,,,,,2020/04/26-14:24:03.300,";
  std::string close_event5_str = "4,node1,2020/04/26-14:24:03.400,13,121,120,1010,2,1010,2,"
      "close,0,3,,,,,2020/04/26-14:24:03.400,";
  std::string close_event6_str = "4,node1,2020/04/26-14:24:03.500,14,121,120,1010,2,1010,2,"
      "close,0,4,,,,,2020/04/26-14:24:03.500,";
  std::string exit_group_event1_str = "4,node1,2020/04/26-14:24:04.000,15,122,121,1010,2,1010,2,"
      "exit_group,0,,,,,,2020/04/26-14:24:04.000,";
  std::string exit_group_event2_str = "4,node1,2020/04/26-14:24:05.000,16,123,121,1010,2,1010,2,"
      "exit_group,0,,,,,,2020/04/26-14:24:05.000,";
  std::string exit_group_event3_str = "4,node1,2020/04/26-14:24:06.000,17,121,120,1010,2,1010,2,"
      "exit_group,0,,,,,,2020/04/26-14:24:06.000,";

  std::shared_ptr<Event> setpgid_event1 = Event::deserialize_event(setpgid_event1_str);
  std::shared_ptr<Event> pipe_event = Event::deserialize_event(pipe_event_str);
  std::shared_ptr<Event> clone_event1 = Event::deserialize_event(clone_event1_str);
  std::shared_ptr<Event> clone_event2 = Event::deserialize_event(clone_event2_str);
  std::shared_ptr<Event> setpgid_event2 = Event::deserialize_event(setpgid_event2_str);
  std::shared_ptr<Event> setpgid_event3 = Event::deserialize_event(setpgid_event3_str);
  std::shared_ptr<Event> dup_event1 = Event::deserialize_event(dup_event1_str);
  std::shared_ptr<Event> dup_event2 = Event::deserialize_event(dup_event2_str);
  std::shared_ptr<Event> close_event1 = Event::deserialize_event(close_event1_str);
  std::shared_ptr<Event> close_event2 = Event::deserialize_event(close_event2_str);
  std::shared_ptr<Event> close_event3 = Event::deserialize_event(close_event3_str);
  std::shared_ptr<Event> close_event4 = Event::deserialize_event(close_event4_str);
  std::shared_ptr<Event> close_event5 = Event::deserialize_event(close_event5_str);
  std::shared_ptr<Event> close_event6 = Event::deserialize_event(close_event6_str);
  std::shared_ptr<Event> exit_group_event1 = Event::deserialize_event(exit_group_event1_str);
  std::shared_ptr<Event> exit_group_event2 = Event::deserialize_event(exit_group_event2_str);
  std::shared_ptr<Event> exit_group_event3 = Event::deserialize_event(exit_group_event3_str);

  os.apply_syscall((SyscallEvent*) setpgid_event1.get());
  os.apply_syscall((SyscallEvent*) pipe_event.get());
  os.apply_syscall((SyscallEvent*) clone_event1.get());
  os.apply_syscall((SyscallEvent*) clone_event2.get());
  os.apply_syscall((SyscallEvent*) setpgid_event2.get());
  os.apply_syscall((SyscallEvent*) setpgid_event3.get());
  os.apply_syscall((SyscallEvent*) dup_event1.get());
  os.apply_syscall((SyscallEvent*) dup_event2.get());
  os.apply_syscall((SyscallEvent*) close_event1.get());
  os.apply_syscall((SyscallEvent*) close_event2.get());
  os.apply_syscall((SyscallEvent*) close_event3.get());
  os.apply_syscall((SyscallEvent*) close_event4.get());
  os.apply_syscall((SyscallEvent*) close_event5.get());
  os.apply_syscall((SyscallEvent*) close_event6.get());
  os.apply_syscall((SyscallEvent*) exit_group_event1.get());
  os.apply_syscall((SyscallEvent*) exit_group_event2.get());
  os.apply_syscall((SyscallEvent*) exit_group_event3.get());

  std::vector<Event*> events = os.reap_os_events();
  EXPECT_EQ(22, events.size());
  EXPECT_EQ("2,,,122,121,121,2020/04/26-14:24:01.100,2020/04/26-14:24:04.000,UNKNOWN,UNKNOWN,", events[17]->serialize());
  EXPECT_EQ("2,,,123,121,121,2020/04/26-14:24:01.200,2020/04/26-14:24:05.000,UNKNOWN,UNKNOWN,", events[18]->serialize());
  EXPECT_EQ("2,,,121,120,121,1970-01-01 00:00:00.000,2020/04/26-14:24:06.000,UNKNOWN,UNKNOWN,", events[19]->serialize());
  EXPECT_EQ("3,,,121,2020/04/26-14:24:00.000,2020/04/26-14:24:06.000,", events[20]->serialize());
  EXPECT_EQ("5,,,123,122,2020/04/26-14:24:01.200,2020/04/26-14:24:01.100,", events[21]->serialize());
}

TEST(os_model_test, test_socket_ipc) {
  OSModel os;

  Socket::reverse_dns_cache["192.168.0.1"] = "some-host";

  std::string clone_event1_str = "4,node1,2020/04/26-14:24:01.100,1,121,120,1010,2,1010,2,"
      "clone,122,,,,,,2020/04/26-14:24:01.100,";
  std::string clone_event2_str = "4,node1,2020/04/26-14:24:01.200,2,121,120,1010,2,1010,2,"
      "clone,123,,,,,,2020/04/26-14:24:01.200,";
  std::string socket_event1_str = "4,node1,2020/04/26-14:24:02.000,3,122,121,1010,2,1010,2,"
      "socket,3,,,,,,2020/04/26-14:24:02.000,";
  std::string bind_event_str = "4,node1,2020/04/26-14:24:02.100,4,122,121,1010,2,1010,2,"
      "bind,0,3,,,,,2020/04/26-14:24:02.100,192.168.0.1,12345,";
  std::string socket_event2_str = "4,node1,2020/04/26-14:24:03.000,5,123,121,1010,2,1010,2,"
      "socket,3,,,,,,2020/04/26-14:24:03.000,";
  std::string connect_event_str = "4,node1,2020/04/26-14:24:04.000,6,123,121,1010,2,1010,2,"
      "connect,0,3,,,,,2020/04/26-14:24:04.000,192.168.0.1,12345,";
  std::string close_event1_str = "4,node1,2020/04/26-14:24:05.000,7,123,121,1010,2,1010,2,"
      "close,0,3,,,,,2020/04/26-14:24:05.000,";
  std::string close_event2_str = "4,node1,2020/04/26-14:24:06.100,8,122,121,1010,2,1010,2,"
      "close,0,3,,,,,2020/04/26-14:24:06.100,";
  std::string exit_group_event1_str = "4,node1,2020/04/26-14:24:07.000,9,123,121,1010,2,1010,2,"
      "exit_group,0,,,,,,2020/04/26-14:24:07.000,";
  std::string exit_group_event2_str = "4,node1,2020/04/26-14:24:08.000,10,122,121,1010,2,1010,2,"
      "exit_group,0,,,,,,2020/04/26-14:24:08.000,";

  std::shared_ptr<Event> clone_event1 = Event::deserialize_event(clone_event1_str);
  std::shared_ptr<Event> clone_event2 = Event::deserialize_event(clone_event2_str);
  std::shared_ptr<Event> socket_event1 = Event::deserialize_event(socket_event1_str);
  std::shared_ptr<Event> bind_event = Event::deserialize_event(bind_event_str);
  std::shared_ptr<Event> socket_event2 = Event::deserialize_event(socket_event2_str);
  std::shared_ptr<Event> connect_event = Event::deserialize_event(connect_event_str);
  std::shared_ptr<Event> close_event1 = Event::deserialize_event(close_event1_str);
  std::shared_ptr<Event> close_event2 = Event::deserialize_event(close_event2_str);
  std::shared_ptr<Event> exit_group_event1 = Event::deserialize_event(exit_group_event1_str);
  std::shared_ptr<Event> exit_group_event2 = Event::deserialize_event(exit_group_event2_str);

  os.apply_syscall((SyscallEvent*) clone_event1.get());
  os.apply_syscall((SyscallEvent*) clone_event2.get());
  os.apply_syscall((SyscallEvent*) socket_event1.get());
  os.apply_syscall((SyscallEvent*) bind_event.get());
  os.apply_syscall((SyscallEvent*) socket_event2.get());
  os.apply_syscall((SyscallEvent*) connect_event.get());
  os.apply_syscall((SyscallEvent*) close_event1.get());
  os.apply_syscall((SyscallEvent*) close_event2.get());
  os.apply_syscall((SyscallEvent*) exit_group_event1.get());
  os.apply_syscall((SyscallEvent*) exit_group_event2.get());

  std::vector<Event*> events = os.reap_os_events();
  EXPECT_EQ(14, events.size());
  EXPECT_EQ("2,,,123,121,-1,2020/04/26-14:24:01.200,2020/04/26-14:24:07.000,UNKNOWN,UNKNOWN,", events[10]->serialize());
  EXPECT_EQ("2,,,122,121,-1,2020/04/26-14:24:01.100,2020/04/26-14:24:08.000,UNKNOWN,UNKNOWN,", events[11]->serialize());
  EXPECT_EQ("6,,,122,2020/04/26-14:24:02.000,2020/04/26-14:24:06.100,12345,", events[12]->serialize());
  EXPECT_EQ("7,,,123,2020/04/26-14:24:04.000,some-host,12345,", events[13]->serialize());
}
