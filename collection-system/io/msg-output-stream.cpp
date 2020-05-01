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

#include <string.h>
#include <errno.h>
#include <assert.h>

#include "logger.h"
#include "msg-output-stream.h"
#include "error.h"

/*------------------------------
 * FileOutputStream
 *------------------------------*/

FileOutputStream::FileOutputStream(std::string filename) :
    filename(filename) {
  LOG_DEBUG("Constructing FileOutputStream for " << filename);
  out_file = std::make_unique<std::ofstream>();
}

int FileOutputStream::open() {
  out_file->open(filename, std::ofstream::trunc);

  if (!out_file->good()) {
    LOG_ERROR("Problems while trying to open output file " << filename << ": " << strerror(errno));
    return ERROR_NO_RETRY;
  }
  return NO_ERROR;
}

void FileOutputStream::close() {
  out_file->close();
}

int FileOutputStream::send(const std::string &msg_str, int partition,
    const std::string *key) {
  assert(out_file->is_open());
  *out_file << msg_str << std::endl;
  return NO_ERROR;
}

int FileOutputStream::send_batch(const std::vector<std::string> &msg_batch) {
  assert(out_file->is_open());
  for (std::string msg : msg_batch) {
    *out_file << msg << std::endl;
  }
  return NO_ERROR;
}

void FileOutputStream::flush() const {
  out_file->flush();
}

std::string FileOutputStream::str() const {
  return filename;
}
