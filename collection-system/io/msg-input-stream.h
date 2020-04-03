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

#ifndef MSG_INPUT_STREAM_H_
#define MSG_INPUT_STREAM_H_

#include <fstream>
#include <string>

/**
 * An input stream handles setting up, reading from, and tearing down
 * a connection to an input source, which emits provenance events.
 */
class MsgInputStream {
public:
  virtual ~MsgInputStream() {};
  virtual int open() = 0;
  virtual int close() = 0;
  virtual int recv(std::string &msg_str) = 0;
};

/**
 * This class is only needed for testing purposes.
 */
class FileInputStream: public MsgInputStream {
private:
  std::string filename;
  std::unique_ptr<std::ifstream> in_file;

public:
  FileInputStream(std::string &filename);
  virtual ~FileInputStream() {};

  virtual int open() override;
  virtual void close() override;
  virtual int recv(std::string &next_msg) override;
};

#endif
