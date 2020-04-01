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

#include "config.h"
#include "logger.h"
#include "error.h"

#include <fstream>

#include <string.h>
#include <errno.h>

config_opts_t Config::config;

/**
 * Parses the config file at the provided path. The config
 * format is expected to be a simple 'key = value' format.
 * Comments are prefixed by '#'.
 */
int Config::parse_config(std::string path) {
  std::ifstream config_file(path);

  if (config_file.fail()) {
    LOG_ERROR("Couldn't read config file: " << strerror(errno));
    return ERROR_NO_RETRY;
  }

  std::string line;

  while (std::getline(config_file, line)) {
    // ignore comments and empty lines
    if (line.empty())
      continue;
    if (line.at(0) == '#')
      continue;

    size_t pos = line.find("=");
    // ignore invalid lines
    if (pos == std::string::npos) {
      LOG_ERROR("\"" << line << "\" is an invalid config entry, ignoring it.");
      continue;
    }

    // get and trim key and value
    std::string key = line.substr(0, pos);
    key.erase(key.begin(), std::find_if(key.begin(), key.end(), [](int c) {
      return !std::isspace(c);
    }));
    key.erase(std::find_if(key.rbegin(), key.rend(), [](int c) {
      return !std::isspace(c);
    }).base(), key.end());

    std::string val = line.substr(pos + 1);
    val.erase(val.begin(), std::find_if(val.begin(), val.end(), [](int c) {
      return !std::isspace(c);
    }));
    val.erase(std::find_if(val.rbegin(), val.rend(), [](int c) {
      return !std::isspace(c);
    }).base(), val.end());

    Config::config[key] = val;
  }

  return NO_ERROR;
}
