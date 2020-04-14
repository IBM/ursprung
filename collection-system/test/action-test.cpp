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
#include "action.h"
#include "error.h"

TEST(db_load_action_test, test1) {
  EXPECT_THROW(DBLoadAction a(""), std::invalid_argument);
}

TEST(db_load_action_test, test2) {
  DBLoadAction a("DBLOAD path INTO FILE dbload-out");
  EXPECT_EQ("path", a.get_event_field());
}

TEST(db_load_action_test, test3) {
  DBLoadAction a("DBLOAD f1 INTO FILE dbload-out");
  std::shared_ptr<IntermediateMessage> msg =
      std::make_shared<TestIntermediateMessage>(CS_PROV_AUDITD, "test-db-load-action,f2,f3");
  a.execute(msg);

  std::ifstream in_file("dbload-out");
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(in_file, line)) {
    lines.push_back(line);
  }

  EXPECT_EQ(5, lines.size());
  EXPECT_EQ("a1,b1,c1", lines[0]);
  EXPECT_EQ("a2,b2,c2", lines[1]);
  EXPECT_EQ("a3,b3,c3", lines[2]);
  EXPECT_EQ("a4,b4,c4", lines[3]);
  EXPECT_EQ("a5,b5,c5", lines[4]);
}

TEST(db_load_action_test, test4) {
  DBLoadAction a("DBLOAD f1 INTO DB user1:password2@dsn3 USING table4/schema5");
  EXPECT_EQ("DBLOAD f1 INTO user1:password2@dsn3 USING table4/schema5", a.str());
}
