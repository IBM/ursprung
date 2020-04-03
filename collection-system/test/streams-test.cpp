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
#include "msg-input-stream.h"
#include "msg-output-stream.h"
#include "error.h"

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
