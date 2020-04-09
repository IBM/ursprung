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

#include <fstream>
#include <thread>
#include <chrono>

#include "gtest/gtest.h"
#include "msg-input-stream.h"
#include "msg-output-stream.h"
#include "db-output-stream.h"
#include "error.h"

/*------------------------------
 * FileInputStream tests
 *------------------------------*/

TEST(file_input_stream_test, test1) {
  std::string file = "test-file-in-stream";
  FileInputStream s(file);
  int rc = s.open();
  EXPECT_EQ(NO_ERROR, rc);

  std::string msg;
  rc = s.recv(msg);
  EXPECT_EQ(NO_ERROR, rc);
  EXPECT_EQ(std::string("testline"), msg);
  rc = s.recv(msg);
  EXPECT_EQ(ERROR_EOF, rc);

  s.close();
}

/*------------------------------
 * FileOutputStream tests
 *------------------------------*/

TEST(file_output_stream_test, test1) {
  std::string file = "test-file-out-stream";
  FileOutputStream s(file);
  int rc = s.open();
  EXPECT_EQ(NO_ERROR, rc);

  std::string msg = "test message";
  rc = s.send(msg, 0);
  s.flush();
  s.close();

  std::ifstream inFile(file);
  std::string line;
  std::getline(inFile, line);
  EXPECT_EQ(msg, line);
}

/*----------------------------------------------------------------------------------
 * DBOutputStream tests
 *
 * All tests, which test sending functionality of the stream are currently disabled
 * as with the current design, there's not really a good way of mocking/faking
 * the ODBC connection that is established in DBOutputStream::send_to_db. As
 * a quick and dirty way to semi-manually check whether the stream works as
 * expected is to comment out the ODBC related part in DBOutputStream::send_to_db,
 * enable the tests,  and then print and inspect the generated queries (that would
 * be submitted to the database) manually. This is not ideal but good enough for now.
 *
 * Maybe we should decouple query generation fom query submission in the stream
 * and then somehow only test up to query generation but skip submission?
 *-----------------------------------------------------------------------------------*/

TEST(db_output_stream_test, test_dummies) {
  std::string dsn = "dsn";
  std::string user = "user";
  std::string pw = "pw";
  std::string schema = "schema";
  std::string table = "table";
  DBOutputStream s(dsn, user, pw, schema, table, false);
  s.set_batch_size(5);

  // does nothing
  s.open();
  s.close();
  s.flush();
  EXPECT_EQ(NO_ERROR, s.send("msg", 0));
}

TEST(db_output_stream_test, DISABLED_test_send_batch_sync) {
  std::string dsn = "dsn";
  std::string user = "user";
  std::string pw = "pw";
  std::string schema = "col";
  std::string table = "testtable";
  DBOutputStream s(dsn, user, pw, schema, table, false);
  s.set_batch_size(5);

  // create first batch
  std::vector<std::string> msgs;
  for (size_t i = 0; i < 10; i++) {
    msgs.push_back("msg " + std::to_string(i));
  }
  s.send_batch(msgs);
}

TEST(db_output_stream_test, DISABLED_test_send_batch_sync_multiplexed) {
  std::string dsn = "dsn";
  std::string user = "user";
  std::string pw = "pw";
  std::string schema = "col";
  std::string table = "testtable";
  DBOutputStream s(dsn, user, pw, schema, table, false, true, 0);
  s.set_batch_size(5);

  s.set_multiplex_group("tableA", "keyA,val", "A");
  s.set_multiplex_group("tableB", "keyB,val", "B");
  s.set_multiplex_group("tableC", "keyC,val", "C");

  // create first batch
  std::vector<std::string> msgs;
  for (size_t i = 0; i < 6; i++) {
    msgs.push_back("A,key A,msg " + std::to_string(i));
    msgs.push_back("B,key B,msg " + std::to_string(i));
    msgs.push_back("C,key C,msg " + std::to_string(i));
  }
  s.send_batch(msgs);
}

TEST(db_output_stream_test, DISABLED_test_send_batch_async) {
  std::string dsn = "dsn";
  std::string user = "user";
  std::string pw = "pw";
  std::string schema = "col";
  std::string table = "testtable";
  DBOutputStream s(dsn, user, pw, schema, table, true);
  s.set_batch_size(5);

  // create first batch
  std::vector<std::string> msgs;
  for (size_t i = 0; i < 12; i++) {
    msgs.push_back("msg " + std::to_string(i));
  }
  s.send_batch(msgs);

  // wait for threads to finish inserting
  std::this_thread::sleep_for (std::chrono::seconds(1));
}
