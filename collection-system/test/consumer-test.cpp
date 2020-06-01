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
  EXPECT_EQ("'OPEN','gpfs-test-cluster','node','fs0',"
      "'/gpfs/fs0/testfile',405523,0,0,29279,'2020-05-29 23:28:02.409261','_NULL_',''", line);
}

TEST(auditd_consumer_test, test1) {
  // create in and output streams
  std::string in_file_name = "auditd-consumer-test.in";
  std::unique_ptr<MsgInputStream> in = std::make_unique<FileInputStream>(in_file_name);
  std::string out_file_name = "auditd-consumer-test.out";
  std::unique_ptr<MsgOutputStream> out = std::make_unique<FileOutputStream>(out_file_name);

  AuditdConsumer c(CS_PROV_AUDITD, std::move(in), CD_FILE, std::move(out));
  c.run();

  std::ifstream in_file("auditd-consumer-test.out");
  std::string line;
  std::vector<std::string> lines;
  while(std::getline(in_file, line)) {
    lines.push_back(line);
  }
  std::getline(in_file, line);
  EXPECT_EQ(6, lines.size());
  std::string syscall_event = "SyscallEvent,'some-node',123,1234,1233,1010,2,1010,2,"
      "'exit_group','some arg','another arg','third arg','','',"
      "0,'2020/04/22 - 01:01:00:123','data1','data2'";
  std::string process_event = "ProcessEvent,'some-node',1234,1233,1234,'/tmp/cwd',"
      "'python train.py -d data','2020/04/22 - 01:01:00:123','2020/04/22 - 01:01:01:123'";
  std::string process_group_event = "ProcessGroupEvent,'some-node',1234,'2020/04/22 - 01:01:00:123',"
      "'2020/04/22 - 01:01:01:123'";
  std::string ipc_event = "IPCEvent,'some-node',1234,1235,'2020/04/22 - 01:01:00:123',"
      "'2020/04/22 - 01:01:00:123'";
  std::string socket_event = "SocketEvent,'some-node',1234,12345,'2020/04/22 - 01:01:00:123',"
      "'2020/04/22 - 01:01:01:123'";
  std::string socket_connect_event = "SocketConnectEvent,'some-other-node',4321,12345,"
      "'2020/04/22 - 01:01:00:124','some-node'";
  EXPECT_EQ(lines[0], syscall_event);
  EXPECT_EQ(lines[1], process_event);
  EXPECT_EQ(lines[2], process_group_event);
  EXPECT_EQ(lines[3], ipc_event);
  EXPECT_EQ(lines[4], socket_event);
  EXPECT_EQ(lines[5], socket_connect_event);
}
