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
#include "config.h"

TEST(parse_config_test, test1) {
  Config::parse_config("test.cfg");
  EXPECT_TRUE((Config::config.find("key1") != Config::config.end()));
  EXPECT_TRUE((Config::config.find("key2") != Config::config.end()));
  EXPECT_TRUE((Config::config.find("key3") != Config::config.end()));
  EXPECT_TRUE((Config::config.find("key4") != Config::config.end()));
  EXPECT_EQ(4, Config::config.size());
  EXPECT_EQ("val1", Config::config["key1"]);
  EXPECT_EQ("val2", Config::config["key2"]);
  EXPECT_EQ("val3", Config::config["key3"]);
  EXPECT_EQ("val4", Config::config["key4"]);
}
