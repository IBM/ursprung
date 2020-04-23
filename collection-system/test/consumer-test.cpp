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

#include "gtest/gtest.h"
#include "config.h"
#include "logger.h"
#include "msg-input-stream.h"
#include "msg-output-stream.h"
#include "abstract-consumer.h"
#include "auditd-consumer.h"
#include "scale-consumer.h"

TEST(scale_consumer_test, test1) {
  // create in and output streams
  std::string in_file_name = "scale-consumer-test.in";
  std::unique_ptr<MsgInputStream> in = std::make_unique<FileInputStream>(in_file_name);
  std::string out_file_name = "scale-consumer-test.out";
  std::unique_ptr<MsgOutputStream> out = std::make_unique<FileOutputStream>(out_file_name);

  ScaleConsumer c(CS_PROV_GPFS, std::move(in), CD_FILE, std::move(out), false);
  c.run();

  std::ifstream in_file("scale-consumer-test.out");
  std::string line;
  std::getline(in_file, line);
  EXPECT_EQ("'CLOSE','scale-cluster','some-node','fs0',"
      "'/tmp/some-file',4321,12,0,1234,'2020/04/22 - 01:01:00.123','',''", line);
}
