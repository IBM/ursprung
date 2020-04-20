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
  EXPECT_THROW(Event::deserialize_event("8,1,2"), std::invalid_argument);
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
