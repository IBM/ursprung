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

#ifndef MSG_OUTPUT_STREAM_H_
#define MSG_OUTPUT_STREAM_H_

#include <fstream>
#include <string>
#include <memory>
#include <vector>

/**
 * An output stream handles setting up, writing to, and tearing down
 * a connection to an provenance store, which stores provenance events.
 */
class MsgOutputStream {
public:
  virtual ~MsgOutputStream() {}
  virtual int open() = 0;
  virtual void close() = 0;
  virtual int send(const std::string &msg_str, int partition,
      const std::string *key = nullptr) = 0;
  virtual int send_batch(const std::vector<std::string> &msg_batch) = 0;
  virtual void flush() const = 0;
  virtual std::string str() const = 0;
};

/**
 * This class is only needed for testing purposes.
 */
class FileOutputStream: public MsgOutputStream {
private:
  std::string filename;
  std::unique_ptr<std::ofstream> out_file;

public:
  FileOutputStream(std::string filename);
  virtual ~FileOutputStream() {};

  virtual int open() override;
  virtual void close() override;
  virtual int send(const std::string &msg_str, int partition,
      const std::string *key = nullptr) override;
  virtual int send_batch(const std::vector<std::string> &msg_batch) override;
  virtual void flush() const override;
  virtual std::string str() const override;
};

#endif // MESSAGE_OUT_STREAM_H
