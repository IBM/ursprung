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
  std::string event = "1,OPEN,gpfs-test-cluster,node,fs0,"
      "/gpfs/fs0/testfile,405523,0,0,29279,"
      "2020-05-29 23:28:02.409261,_NULL_,-rw-r--r--";

  std::shared_ptr<Event> e_deserialized = Event::deserialize_event(event);

  // test deserialization
  EXPECT_EQ(FS_EVENT, e_deserialized->get_type());
  EXPECT_EQ("node", e_deserialized->get_node_name());
  EXPECT_EQ("OPEN",  e_deserialized->get_value("event"));
  EXPECT_EQ("gpfs-test-cluster",  e_deserialized->get_value("cluster_name"));
  EXPECT_EQ("fs0",  e_deserialized->get_value("fs_name"));
  EXPECT_EQ("/gpfs/fs0/testfile",  e_deserialized->get_value("path"));
  EXPECT_EQ("405523",  e_deserialized->get_value("inode"));
  EXPECT_EQ("0",  e_deserialized->get_value("bytes_read"));
  EXPECT_EQ("0",  e_deserialized->get_value("bytes_written"));
  EXPECT_EQ("29279",  e_deserialized->get_value("pid"));
  EXPECT_EQ("2020-05-29 23:28:02.409261",  e_deserialized->get_value("event_time"));
  EXPECT_EQ("_NULL_",  e_deserialized->get_value("dst_path"));
  EXPECT_EQ("",  e_deserialized->get_value("version_hash"));

  std::string e_serialized = e_deserialized->serialize();
  EXPECT_EQ(event + ",", e_serialized);
}

TEST(event_test, fs_event_test2) {
  std::string event = "1,OPEN,gpfs-test-cluster,node,fs0,"
      "/gpfs/fs0/testfile,405523,-1,-1,29279,"
      "2020-05-29 23:28:02.409261,_NULL_,-rw-r--r--";

  std::shared_ptr<Event> e_deserialized = Event::deserialize_event(event);

  // test deserialization
  EXPECT_EQ(FS_EVENT, e_deserialized->get_type());
  EXPECT_EQ("node", e_deserialized->get_node_name());
  EXPECT_EQ("OPEN",  e_deserialized->get_value("event"));
  EXPECT_EQ("gpfs-test-cluster",  e_deserialized->get_value("cluster_name"));
  EXPECT_EQ("fs0",  e_deserialized->get_value("fs_name"));
  EXPECT_EQ("/gpfs/fs0/testfile",  e_deserialized->get_value("path"));
  EXPECT_EQ("405523",  e_deserialized->get_value("inode"));
  EXPECT_EQ("-1",  e_deserialized->get_value("bytes_read"));
  EXPECT_EQ("-1",  e_deserialized->get_value("bytes_written"));
  EXPECT_EQ("29279",  e_deserialized->get_value("pid"));
  EXPECT_EQ("2020-05-29 23:28:02.409261",  e_deserialized->get_value("event_time"));
  EXPECT_EQ("_NULL_",  e_deserialized->get_value("dst_path"));
  EXPECT_EQ("",  e_deserialized->get_value("version_hash"));

  std::string e_serialized = e_deserialized->serialize();
  EXPECT_EQ(event + ",", e_serialized);
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

TEST(event_test, fs_event_json_test1) {
  std::string event = "{\"WF_JSON\": \"0.0.1\", \"wd\":\"1\",\"cookie\":\"0\",\"event\": "
      "\"IN_CLOSE_NOWRITE\",\"path\": \"/gpfs/fs0/some-folder/some-file\", \"clusterName\": "
      "\"gpfs-test-cluster\", \"nodeName\": \"node1\", \"nfsClientIp\": \"\", "
      "\"fsName\": \"fs0\", \"inode\": \"82380\", \"fileSetID\": \"0\", \"linkCount\": \"1\", "
      "\"openFlags\": \"32769\", \"poolName\": \"system\", \"fileSize\": \"13\", \"ownerUserId\": \"100\", "
      "\"ownerGroupId\": \"100\", \"atime\": \"2020-08-03_09:06:26-0700\", \"ctime\": "
      "\"2020-08-03_09:06:26-0700\", \"mtime\": \"2020-08-03_09:06:26-0700\", \"eventTime\": "
      "\"2020-08-03_09:06:36-0700\", \"clientUserId\": \"100\", \"clientGroupId\": \"100\", "
      "\"processId\": \"10391\", \"permissions\": \"200100644\", \"acls\": null, \"xattrs\": null, "
      "\"subEvent\": \"NONE\" }";

  std::shared_ptr<Event> e_deserialized = Event::deserialize_event(event);

  // test deserialization
  EXPECT_EQ(FS_EVENT, e_deserialized->get_type());
  EXPECT_EQ("node1", e_deserialized->get_node_name());
  EXPECT_EQ("CLOSE",  e_deserialized->get_value("event"));
  EXPECT_EQ("gpfs-test-cluster",  e_deserialized->get_value("cluster_name"));
  EXPECT_EQ("fs0",  e_deserialized->get_value("fs_name"));
  EXPECT_EQ("/gpfs/fs0/some-folder/some-file",  e_deserialized->get_value("path"));
  EXPECT_EQ("82380",  e_deserialized->get_value("inode"));
  EXPECT_EQ("1",  e_deserialized->get_value("bytes_read"));
  EXPECT_EQ("0",  e_deserialized->get_value("bytes_written"));
  EXPECT_EQ("10391",  e_deserialized->get_value("pid"));
  EXPECT_EQ("2020-08-03_09:06:36-0700",  e_deserialized->get_value("event_time"));
  EXPECT_EQ("_NULL_",  e_deserialized->get_value("dst_path"));
  EXPECT_EQ("",  e_deserialized->get_value("version_hash"));
}

TEST(event_test, fs_event_json_test2) {
  std::string event = "{\"WF_JSON\": \"0.0.1\", \"wd\":\"1\",\"cookie\":\"0\",\"event\": "
      "\"IN_CLOSE_WRITE\",\"path\": \"/gpfs/fs0/some-folder/some-file\", \"clusterName\": "
      "\"gpfs-test-cluster\", \"nodeName\": \"node1\", \"nfsClientIp\": \"\", "
      "\"fsName\": \"fs0\", \"inode\": \"82380\", \"fileSetID\": \"0\", \"linkCount\": \"1\", "
      "\"openFlags\": \"32769\", \"poolName\": \"system\", \"fileSize\": \"13\", \"ownerUserId\": \"100\", "
      "\"ownerGroupId\": \"100\", \"atime\": \"2020-08-03_09:06:26-0700\", \"ctime\": "
      "\"2020-08-03_09:06:26-0700\", \"mtime\": \"2020-08-03_09:06:26-0700\", \"eventTime\": "
      "\"2020-08-03_09:06:36-0700\", \"clientUserId\": \"100\", \"clientGroupId\": \"100\", "
      "\"processId\": \"10391\", \"permissions\": \"200100644\", \"acls\": null, \"xattrs\": null, "
      "\"subEvent\": \"NONE\" }";

  std::shared_ptr<Event> e_deserialized = Event::deserialize_event(event);

  // test deserialization
  EXPECT_EQ(FS_EVENT, e_deserialized->get_type());
  EXPECT_EQ("node1", e_deserialized->get_node_name());
  EXPECT_EQ("CLOSE",  e_deserialized->get_value("event"));
  EXPECT_EQ("gpfs-test-cluster",  e_deserialized->get_value("cluster_name"));
  EXPECT_EQ("fs0",  e_deserialized->get_value("fs_name"));
  EXPECT_EQ("/gpfs/fs0/some-folder/some-file",  e_deserialized->get_value("path"));
  EXPECT_EQ("82380",  e_deserialized->get_value("inode"));
  EXPECT_EQ("0",  e_deserialized->get_value("bytes_read"));
  EXPECT_EQ("1",  e_deserialized->get_value("bytes_written"));
  EXPECT_EQ("10391",  e_deserialized->get_value("pid"));
  EXPECT_EQ("2020-08-03_09:06:36-0700",  e_deserialized->get_value("event_time"));
  EXPECT_EQ("_NULL_",  e_deserialized->get_value("dst_path"));
  EXPECT_EQ("",  e_deserialized->get_value("version_hash"));
}

TEST(event_test, fs_event_json_test3) {
  std::string event1 = "{\"WF_JSON\": \"0.0.1\", \"wd\":\"1\",\"cookie\":\"12345\",\"event\": "
      "\"IN_MOVED_FROM\",\"path\": \"/gpfs/fs0/some-folder/some-file\", \"clusterName\": "
      "\"gpfs-test-cluster\", \"nodeName\": \"node1\", \"nfsClientIp\": \"\", "
      "\"fsName\": \"fs0\", \"inode\": \"82380\", \"fileSetID\": \"0\", \"linkCount\": \"1\", "
      "\"openFlags\": \"32769\", \"poolName\": \"system\", \"fileSize\": \"13\", \"ownerUserId\": \"100\", "
      "\"ownerGroupId\": \"100\", \"atime\": \"2020-08-03_09:06:26-0700\", \"ctime\": "
      "\"2020-08-03_09:06:26-0700\", \"mtime\": \"2020-08-03_09:06:26-0700\", \"eventTime\": "
      "\"2020-08-03_09:06:36-0700\", \"clientUserId\": \"100\", \"clientGroupId\": \"100\", "
      "\"processId\": \"10391\", \"permissions\": \"200100644\", \"acls\": null, \"xattrs\": null, "
      "\"subEvent\": \"NONE\" }";
  std::string event2 = "{\"WF_JSON\": \"0.0.1\", \"wd\":\"1\",\"cookie\":\"12345\",\"event\": "
      "\"IN_MOVED_TO\",\"path\": \"/gpfs/fs0/some-folder/some-dst-file\", \"clusterName\": "
      "\"gpfs-test-cluster\", \"nodeName\": \"node1\", \"nfsClientIp\": \"\", "
      "\"fsName\": \"fs0\", \"inode\": \"82380\", \"fileSetID\": \"0\", \"linkCount\": \"1\", "
      "\"openFlags\": \"32769\", \"poolName\": \"system\", \"fileSize\": \"13\", \"ownerUserId\": \"100\", "
      "\"ownerGroupId\": \"100\", \"atime\": \"2020-08-03_09:06:26-0700\", \"ctime\": "
      "\"2020-08-03_09:06:26-0700\", \"mtime\": \"2020-08-03_09:06:26-0700\", \"eventTime\": "
      "\"2020-08-03_09:06:36-0700\", \"clientUserId\": \"100\", \"clientGroupId\": \"100\", "
      "\"processId\": \"10391\", \"permissions\": \"200100644\", \"acls\": null, \"xattrs\": null, "
      "\"subEvent\": \"NONE\" }";

  std::shared_ptr<Event> e_deserialized1 = Event::deserialize_event(event1);
  std::shared_ptr<Event> e_deserialized2 = Event::deserialize_event(event2);

  // test deserialization
  EXPECT_TRUE(!e_deserialized1);
  EXPECT_EQ(FS_EVENT, e_deserialized2->get_type());
  EXPECT_EQ("node1", e_deserialized2->get_node_name());
  EXPECT_EQ("RENAME",  e_deserialized2->get_value("event"));
  EXPECT_EQ("gpfs-test-cluster",  e_deserialized2->get_value("cluster_name"));
  EXPECT_EQ("fs0",  e_deserialized2->get_value("fs_name"));
  EXPECT_EQ("/gpfs/fs0/some-folder/some-file",  e_deserialized2->get_value("path"));
  EXPECT_EQ("82380",  e_deserialized2->get_value("inode"));
  EXPECT_EQ("0",  e_deserialized2->get_value("bytes_read"));
  EXPECT_EQ("0",  e_deserialized2->get_value("bytes_written"));
  EXPECT_EQ("10391",  e_deserialized2->get_value("pid"));
  EXPECT_EQ("2020-08-03_09:06:36-0700",  e_deserialized2->get_value("event_time"));
  EXPECT_EQ("/gpfs/fs0/some-folder/some-dst-file",  e_deserialized2->get_value("dst_path"));
  EXPECT_EQ("",  e_deserialized2->get_value("version_hash"));
}
