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
#include "error.h"

TEST(parse_config_test, test1) {
  int rc = Config::parse_config("test.cfg");
  EXPECT_EQ(NO_ERROR, rc);
  EXPECT_TRUE((Config::config.find("odbc-dsn") != Config::config.end()));
  EXPECT_TRUE((Config::config.find("odbc-user") != Config::config.end()));
  EXPECT_TRUE((Config::config.find("odbc-pass") != Config::config.end()));
  EXPECT_EQ(3, Config::config.size());
  EXPECT_EQ("val1", Config::config["odbc-dsn"]);
  EXPECT_EQ("val3", Config::config["odbc-user"]);
  EXPECT_EQ("val4", Config::config["odbc-pass"]);
}

TEST(parse_config_test, test2) {
  int rc = Config::parse_config("test-invalid-path.cfg");
  EXPECT_EQ(ERROR_NO_RETRY, rc);
}
