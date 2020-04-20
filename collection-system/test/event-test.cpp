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

TEST(event_test, test_event_test) {
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
