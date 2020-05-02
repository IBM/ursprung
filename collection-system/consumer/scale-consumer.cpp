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

#include <sys/stat.h>
#include <fcntl.h>
#include <openssl/md5.h>
#include <unistd.h>
#include <sys/mman.h>
#include <iomanip>
#include <assert.h>

#include "scale-consumer.h"
#include "scale-event.h"

int ScaleConsumer::receive_event(ConsumerSource csrc, evt_t event) {
  // TODO add processing of directory renames here
  // if assert passes, we can safely cast event and call get_value() with FSEvent attributes
  assert(event->get_type() == FS_EVENT);

  if (event->get_value("event") == "CLOSE"
      && std::stoi(event->get_value("bytes_written")) > 0 && track_versions) {
    // if the event updated a file and version tracking is enabled, compute version hash
    // open file, check that it exists and compute file size
    int fd = open(event->get_value("path").c_str(), O_RDONLY);
    if (fd < 0) {
      LOGGER_LOG_WARN("Couldn't open file " << event->get_value("path") << " for version tracking. "
          << "No hash computed.");
      return NO_ERROR;
    }
    struct stat statbuf;
    if (fstat(fd, &statbuf) < 0) {
      LOGGER_LOG_WARN("Couldn't open file " << event->get_value("path") << " for version tracking. "
          << "No hash computed.");
      return NO_ERROR;
    }
    unsigned long file_size = statbuf.st_size;

    // compute hash over the content
    unsigned char file_hash[MD5_DIGEST_LENGTH];
    char *file_buffer = (char*) mmap(0, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (file_buffer == MAP_FAILED) {
      LOGGER_LOG_WARN("Couldn't mmap file for version tracking due to " << strerror(errno));
      close(fd);
      return NO_ERROR;
    }
    MD5((unsigned char*) file_buffer, file_size, file_hash);
    if (munmap(file_buffer, file_size) != 0) {
      LOGGER_LOG_WARN("Problems while unmapping " << event->get_value("path") << ".");
    }
    close(fd);

    // convert hash to hex
    std::ostringstream sout;
    sout << std::hex << std::setfill('0');
    for (long long c : file_hash) {
      sout << std::setw(2) << (long long) c;
    }
    ((FSEvent*) event.get())->set_version_hash(sout.str());
    LOGGER_LOG_DEBUG("Computed hash for " << event->get_value("path"));
  }

  return NO_ERROR;
}
