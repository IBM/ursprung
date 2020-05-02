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

#include "logger.h"
#include "error.h"
#include "msg-input-stream.h"

/*------------------------------
 * FileInputStream
 *------------------------------*/

FileInputStream::FileInputStream(std::string &filename) :
    filename { filename } {
  LOGGER_LOG_DEBUG("Constructing FileInputStream from " << filename);
  in_file = std::make_unique<std::ifstream>();
}

int FileInputStream::open() {
  in_file->open(filename);

  if (!in_file->good()) {
    LOGGER_LOG_ERROR("Problems while trying to open input file " << filename << ": " << strerror(errno));
    return ERROR_NO_RETRY;
  }
  return NO_ERROR;
}

void FileInputStream::close() {
  in_file->close();
}

int FileInputStream::recv(std::string &next_msg) {
  std::string line;
  if (std::getline(*in_file, line)) {
    next_msg = line;
    return NO_ERROR;
  }
  if (in_file->eof()) {
    return ERROR_EOF;
  }

  return ERROR_NO_RETRY;
}
