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

#include <memory>

#include "gtest/gtest.h"
#include "event.h"
#include "scale-event.h"
#include "auditd-event.h"

TEST(event_test, test_event_test1) {
  TestEvent e("1","abc","hello world");
  e.set_node_name("node1");
  e.set_send_time("time1");

  // test serialization
  std::string e_serialized = e.serialize();
  EXPECT_EQ("8,node1,time1,1,abc,hello world,", e_serialized);

  // test deserialization
  std::shared_ptr<Event> e_deserialized = Event::deserialize_event(e_serialized);
  EXPECT_EQ(TEST_EVENT, e_deserialized->get_type());
  EXPECT_EQ("node1", e_deserialized->get_node_name());
  EXPECT_EQ("time1", e_deserialized->get_send_time());
  EXPECT_EQ("1",  e_deserialized->get_value("f1"));
  EXPECT_EQ("abc", e_deserialized->get_value("f2"));
  EXPECT_EQ("hello world", e_deserialized->get_value("f3"));
}

TEST(event_test, test_event_test2) {
  EXPECT_TRUE(Event::deserialize_event("8,1,2") == nullptr);
}

TEST(event_test, fs_event_test1) {
  FSEvent e(1, 2, 3, 4, "event", "event_time", "cluster_name", "fs_name",
      "path", "dst_path", "mode", "hash");
  e.set_node_name("node1");
  e.set_send_time("time1");

  // test serialization
  std::string e_serialized = e.serialize();
  EXPECT_EQ("1,node1,time1,1,2,3,4,path,dst_path,mode,event,event_time,cluster_name,fs_name,",
      e_serialized);

  // test deserialization
  std::shared_ptr<Event> e_deserialized = Event::deserialize_event(e_serialized);
  EXPECT_EQ(FS_EVENT, e_deserialized->get_type());
  EXPECT_EQ("node1", e_deserialized->get_node_name());
  EXPECT_EQ("time1", e_deserialized->get_send_time());
  EXPECT_EQ("event",  e_deserialized->get_value("event"));
  EXPECT_EQ("cluster_name",  e_deserialized->get_value("cluster_name"));
  EXPECT_EQ("fs_name",  e_deserialized->get_value("fs_name"));
  EXPECT_EQ("path",  e_deserialized->get_value("path"));
  EXPECT_EQ("2",  e_deserialized->get_value("inode"));
  EXPECT_EQ("3",  e_deserialized->get_value("bytes_read"));
  EXPECT_EQ("4",  e_deserialized->get_value("bytes_written"));
  EXPECT_EQ("1",  e_deserialized->get_value("pid"));
  EXPECT_EQ("event_time",  e_deserialized->get_value("event_time"));
  EXPECT_EQ("dst_path",  e_deserialized->get_value("dst_path"));
  EXPECT_EQ("",  e_deserialized->get_value("version_hash"));
}

TEST(event_test, syscall_event_test1) {
  std::string evt = "4,node1,time1,12345,1,2,3,4,5,6,clone,"
      "0,a0,a1,a2,a3,and another very long argument to the syscall,"
      "time2,data0,data1";
  // test deserialization
  std::shared_ptr<Event> e = Event::deserialize_event(evt);
  EXPECT_EQ(SYSCALL_EVENT, e->get_type());
  EXPECT_EQ("node1", e->get_node_name());
  EXPECT_EQ("time1", e->get_send_time());
  EXPECT_EQ("12345", e->get_value("auditd_event_id"));
  EXPECT_EQ("1", e->get_value("pid"));
  EXPECT_EQ("2", e->get_value("ppid"));
  EXPECT_EQ("3", e->get_value("uid"));
  EXPECT_EQ("4", e->get_value("gid"));
  EXPECT_EQ("5", e->get_value("euid"));
  EXPECT_EQ("6", e->get_value("egid"));
  EXPECT_EQ("clone", e->get_value("syscall_name"));
  EXPECT_EQ("a0", e->get_value("arg0"));
  EXPECT_EQ("a1", e->get_value("arg1"));
  EXPECT_EQ("a2", e->get_value("arg2"));
  EXPECT_EQ("a3", e->get_value("arg3"));
  EXPECT_EQ("and another very long argument to the syscall", e->get_value("arg4"));
  EXPECT_EQ("0", e->get_value("rc"));
  EXPECT_EQ("time2", e->get_value("event_time"));

  // test serialization
  std::string serialized_event = e->serialize();
  EXPECT_EQ("4,node1,time1,12345,1,2,3,4,5,6,clone,"
      "0,a0,a1,a2,a3,and another very long argument to the syscall,"
      "time2,data0,data1,", serialized_event);
}

TEST(event_test, process_event_test1) {
  std::vector<std::string> cmd_line = { "python", "train.py", "-i input", "-o output" };
  ProcessEvent e(1, 2, 3, "/this/is/the/cwd", cmd_line,
      "start_time1", "finish_time2");
  e.set_node_name("node1");
  e.set_send_time("time1");

  // test serialization
  std::string e_serialized = e.serialize();
  EXPECT_EQ("2,node1,time1,1,2,3,start_time1,finish_time2,"
      "/this/is/the/cwd,python,train.py,-i input,-o output,", e_serialized);

  // test deserialization
  std::shared_ptr<Event> e_deserialized = Event::deserialize_event(e_serialized);
  EXPECT_EQ(PROCESS_EVENT, e_deserialized->get_type());
  EXPECT_EQ("node1", e_deserialized->get_node_name());
  EXPECT_EQ("time1", e_deserialized->get_send_time());
  EXPECT_EQ("1",  e_deserialized->get_value("pid"));
  EXPECT_EQ("2",  e_deserialized->get_value("ppid"));
  EXPECT_EQ("3",  e_deserialized->get_value("pgid"));
  EXPECT_EQ("start_time1",  e_deserialized->get_value("start_time_utc"));
  EXPECT_EQ("finish_time2",  e_deserialized->get_value("finish_time_utc"));
  EXPECT_EQ("/this/is/the/cwd",  e_deserialized->get_value("exec_cwd"));
}

TEST(event_test, process_group_event_test1) {
  ProcessGroupEvent e(1, "start_time", "finish_time");
  e.set_node_name("node1");
  e.set_send_time("time1");

  // test serialization
  std::string e_serialized = e.serialize();
  EXPECT_EQ("3,node1,time1,1,start_time,finish_time,", e_serialized);

  // test deserialization
  std::shared_ptr<Event> e_deserialized = Event::deserialize_event(e_serialized);
  EXPECT_EQ(PROCESS_GROUP_EVENT, e_deserialized->get_type());
  EXPECT_EQ("node1", e_deserialized->get_node_name());
  EXPECT_EQ("time1", e_deserialized->get_send_time());
  EXPECT_EQ("1",  e_deserialized->get_value("pgid"));
  EXPECT_EQ("start_time",  e_deserialized->get_value("start_time_utc"));
  EXPECT_EQ("finish_time",  e_deserialized->get_value("finish_time_utc"));
}

TEST(event_test, ipc_event_test1) {
  IPCEvent e(1, 2, "src_start_time", "dst_start_time");
  e.set_node_name("node1");
  e.set_send_time("time1");

  // test serialization
  std::string e_serialized = e.serialize();
  EXPECT_EQ("5,node1,time1,1,2,src_start_time,dst_start_time,", e_serialized);

  // test deserialization
  std::shared_ptr<Event> e_deserialized = Event::deserialize_event(e_serialized);
  EXPECT_EQ(IPC_EVENT, e_deserialized->get_type());
  EXPECT_EQ("node1", e_deserialized->get_node_name());
  EXPECT_EQ("time1", e_deserialized->get_send_time());
  EXPECT_EQ("1",  e_deserialized->get_value("src_pid"));
  EXPECT_EQ("2",  e_deserialized->get_value("dst_pid"));
  EXPECT_EQ("src_start_time",  e_deserialized->get_value("src_start_time_utc"));
  EXPECT_EQ("dst_start_time",  e_deserialized->get_value("dst_start_time_utc"));
}

TEST(event_test, socket_event_test1) {
  SocketEvent e(1, "open_time", "close_time", 54321);
  e.set_node_name("node1");
  e.set_send_time("time1");

  // test serialization
  std::string e_serialized = e.serialize();
  EXPECT_EQ("6,node1,time1,1,open_time,close_time,54321,", e_serialized);

  // test deserialization
  std::shared_ptr<Event> e_deserialized = Event::deserialize_event(e_serialized);
  EXPECT_EQ(SOCKET_EVENT, e_deserialized->get_type());
  EXPECT_EQ("node1", e_deserialized->get_node_name());
  EXPECT_EQ("time1", e_deserialized->get_send_time());
  EXPECT_EQ("1",  e_deserialized->get_value("pid"));
  EXPECT_EQ("54321",  e_deserialized->get_value("port"));
  EXPECT_EQ("open_time",  e_deserialized->get_value("open_time"));
  EXPECT_EQ("close_time",  e_deserialized->get_value("close_time"));
}

TEST(event_test, socket_connect_event_test1) {
  SocketConnectEvent e(12345, "connect_time", "node2", 54321);
  e.set_node_name("node1");
  e.set_send_time("time1");

  // test serialization
  std::string e_serialized = e.serialize();
  EXPECT_EQ("7,node1,time1,12345,connect_time,node2,54321,", e_serialized);

  // test deserialization
  std::shared_ptr<Event> e_deserialized = Event::deserialize_event(e_serialized);
  EXPECT_EQ(SOCKET_CONNECT_EVENT, e_deserialized->get_type());
  EXPECT_EQ("node1", e_deserialized->get_node_name());
  EXPECT_EQ("time1", e_deserialized->get_send_time());
  EXPECT_EQ("12345",  e_deserialized->get_value("pid"));
  EXPECT_EQ("54321",  e_deserialized->get_value("dst_port"));
  EXPECT_EQ("connect_time",  e_deserialized->get_value("connect_time"));
  EXPECT_EQ("node2",  e_deserialized->get_value("dst_node"));
}
