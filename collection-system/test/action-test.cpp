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

#include <cstdio>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "action.h"
#include "error.h"
#include "db-connector.h"

/*------------------------------
 * DBLoadAction
 *------------------------------*/

TEST(db_load_action_test, test_invalid_creation) {
  EXPECT_THROW(DBLoadAction a(""), std::invalid_argument);
}

TEST(db_load_action_test, test_valid_creation) {
  DBLoadAction a("DBLOAD path INTO FILE dbload-out");
  EXPECT_EQ("path", a.get_event_field());
}

TEST(db_load_action_test, test_execute) {
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

TEST(db_load_action_test, test_str) {
  DBLoadAction a("DBLOAD f1 INTO DB ODBC user1:password2@dsn3 USING table4/schema5");
  EXPECT_EQ("DBLOAD f1 INTO ODBC user1:password2@dsn3 USING table4/schema5", a.str());
}

/*------------------------------
 * DBTransferAction
 *------------------------------*/

TEST(db_transfer_action_test, test_invalid_creation) {
  EXPECT_THROW(DBTransferAction a(""), std::invalid_argument);
}

TEST(db_transfer_action_test, test_valid_creation) {
  DBTransferAction a("DBTRANSFER select * from table/attr FROM MOCK user:password@db INTO "
      "FILE dbtransfer-out");
  EXPECT_EQ("select * from table", a.get_query());
  EXPECT_EQ("attr", a.get_state_attribute_name());
  EXPECT_EQ("MOCK user:password@db", a.get_connection_string());
}

TEST(db_transfer_action_test, test_execute) {
  DBTransferAction a("DBTRANSFER select * from table/attr FROM MOCK user:password@db INTO "
      "FILE dbtransfer-out");
  // this message is not used by the action execution
  std::shared_ptr<IntermediateMessage> msg =
      std::make_shared<TestIntermediateMessage>(CS_PROV_AUDITD, "test-db-transfer-action,f2,f3");
  // execute three times to get three new rows
  a.execute(msg);
  a.execute(msg);
  a.execute(msg);
  std::ifstream in_file("dbtransfer-out");
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(in_file, line)) {
    lines.push_back(line);
  }
  EXPECT_EQ(3, lines.size());
  EXPECT_EQ("a0,b0,c0", lines[0]);
  EXPECT_EQ("a1,b1,c1", lines[1]);
  EXPECT_EQ("a2,b2,c2", lines[2]);
}

TEST(db_transfer_action_test, test_str) {
  DBTransferAction a("DBTRANSFER select a,b,c from table/attr FROM MOCK user:password@db INTO "
      "DB MOCK user1:password2@dsn3 USING table4/schema5");
  EXPECT_EQ("DBTRANSFER select a,b,c from table/attr FROM MOCK user:password@db INTO "
      "MOCK user1:password2@dsn3 USING table4/schema5", a.str());
}

/*------------------------------
 * LogLoadAction
 *------------------------------*/

std::vector<std::string> read_file(std::string path) {
  std::ifstream in_file(path);
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(in_file, line)) {
    lines.push_back(line);
  }
  return lines;
}

TEST(log_load_action_test, test_invalid_creation) {
  EXPECT_THROW(LogLoadAction a(""), std::invalid_argument);
}

TEST(log_load_action_test, test_valid_creation) {
  LogLoadAction a("LOGLOAD path MATCH some-entry FIELDS 0,1,3-5 DELIM , INTO "
      "FILE logload-out");
  EXPECT_EQ("path", a.get_event_field());
  EXPECT_EQ("(.*)some-entry(.*)", a.get_matching_string());
  EXPECT_EQ(",", a.get_delimiter());
}

TEST(log_load_action_test, test_execute) {
  LogLoadAction a("LOGLOAD f1 MATCH some-entry FIELDS 0,1,3-5 DELIM , INTO "
      "FILE logload-out");
  std::ofstream out_file("test-log-load-action");
  std::shared_ptr<IntermediateMessage> msg =
      std::make_shared<TestIntermediateMessage>(CS_PROV_AUDITD, "test-log-load-action,f2,f3");

  // first line
  out_file << "first log line, no match" << std::endl;
  a.execute(msg);
  std::vector<std::string> lines = read_file("logload-out");
  EXPECT_TRUE(lines.empty());

  // second line
  out_file << "second line,some-entry,matches,3,4,5" << std::endl;
  a.execute(msg);
  lines = read_file("logload-out");
  EXPECT_EQ(1, lines.size());
  EXPECT_EQ("second line,some-entry,3 4 5", lines[0]);

  // third line, partial
  out_file << "third line,some-";
  a.execute(msg);
  lines = read_file("logload-out");
  EXPECT_EQ(1, lines.size());
  EXPECT_EQ("second line,some-entry,3 4 5", lines[0]);
  out_file << "entry,matches again,6,7,8" << std::endl;
  a.execute(msg);
  lines = read_file("logload-out");
  EXPECT_EQ(2, lines.size());
  EXPECT_EQ("second line,some-entry,3 4 5", lines[0]);
  EXPECT_EQ("third line,some-entry,6 7 8", lines[1]);

  // log rotation
  out_file.close();
  std::rename("test-log-load-action", "test-log-load-action.1");
  std::ofstream out_file2("test-log-load-action");
  out_file2 << "first log line, no match" << std::endl;
  out_file2 << "second line,some-entry,matches,3,4,5" << std::endl;
  a.execute(msg);
  lines = read_file("logload-out");
  EXPECT_EQ(3, lines.size());
  EXPECT_EQ("second line,some-entry,3 4 5", lines[0]);
  EXPECT_EQ("third line,some-entry,6 7 8", lines[1]);
  EXPECT_EQ("second line,some-entry,3 4 5", lines[2]);
}

TEST(log_load_action_test, test_str) {
  LogLoadAction a("LOGLOAD path MATCH some-entry FIELDS 0,1,3-5 DELIM , INTO "
      "DB MOCK user1:password2@dsn3 USING table4/schema5");
  EXPECT_EQ("LOGLOAD path MATCH (.*)some-entry(.*) FIELDS 0,1,3-5 DELIM , INTO MOCK "
      "user1:password2@dsn3 USING table4/schema5", a.str());
}

/*------------------------------
 * TrackAction
 *------------------------------*/

TEST(track_action_test, test_invalid_creation) {
  EXPECT_THROW(TrackAction a(""), std::invalid_argument);
}

TEST(track_action_test, test_valid_creation) {
  TrackAction a("TRACK /tmp/(.*).py AT /my/repo INTO FILE track-out");
  EXPECT_EQ("/tmp/(.*).py", a.get_path_regex());
  EXPECT_EQ("/my/repo", a.get_repo_path());
}

// TODO add test for TrackAction.execute()

TEST(track_action_test, test_str) {
  TrackAction a("TRACK /tmp/(.*).py AT /my/repo INTO DB MOCK user1:password2@dsn3 "
      "USING table4/schema5");
  EXPECT_EQ("TRACK /tmp/(.*).py AT /my/repo INTO MOCK user1:password2@dsn3 "
      "USING table4/schema5", a.str());
}

/*------------------------------
 * StdoutCaptureAction
 *------------------------------*/

TEST(stdoutcapture_action_test, test_invalid_creation) {
  EXPECT_THROW(StdoutCaptureAction a(""), std::invalid_argument);
}

TEST(stdoutcapture_action_test, test_valid_creation) {
  StdoutCaptureAction a("CAPTURESOUT MATCH (.*)train.py FIELDS 0,1,2 DELIM , INTO "
      "FILE stdout-capture-out");
  EXPECT_EQ("(.*)train.py", a.get_matching_string());
  EXPECT_EQ(",", a.get_delimiter());
}

// TODO add test for StdoutCaptureAction.execute()

TEST(stdoutcapture_action_test, test_str) {
  StdoutCaptureAction a("CAPTURESOUT MATCH (.*)train.py FIELDS 0,1,2 DELIM , INTO "
      "DB MOCK user1:password2@dsn3 USING table4/schema5");
  EXPECT_EQ("CAPTURESOUT MATCH (.*)train.py FIELDS 0,1,2 DELIM , INTO "
      "MOCK user1:password2@dsn3 USING table4/schema5", a.str());
}
