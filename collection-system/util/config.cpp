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
#include "error.h"

#include <fstream>
#include <iostream>

#include <string.h>
#include <errno.h>

// define config keys
const std::string Config::CKEY_ODBC_DSN = "odbc-dsn";
const std::string Config::CKEY_ODBC_USER = "odbc-user";
const std::string Config::CKEY_ODBC_PASS = "odbc-pass";
const std::string Config::CKEY_BROKER_HOST = "broker-host";
const std::string Config::CKEY_BROKER_PORT = "broker-port";
const std::string Config::CKEY_KAFKA_TOPIC = "kafka-topic";
const std::string Config::CKEY_KAFKA_SASL_USER = "sasl-user";
const std::string Config::CKEY_KAFKA_SASL_PASS = "sasl-password";
const std::string Config::CKEY_LOG_FILE = "log-file";
const std::string Config::CKEY_RULES_FILE = "rules-file";
const std::string Config::CKEY_TRACK_VERSIONS = "enable-versioning";
const std::string Config::CKEY_PROVD_PORT = "port";
const std::string Config::CKEY_PROV_SRC = "prov-src";
const std::string Config::CKEY_INPUT_SRC = "in-src";
const std::string Config::CKEY_OUTPUT_DST = "out-dst";

config_opts_t Config::config;

int Config::parse_config(std::string path) {
  std::ifstream config_file(path);

  if (config_file.fail()) {
    std::cerr << "Couldn't read config file: " << strerror(errno) << std::endl;
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
      std::cout << "\"" << line << "\" is an invalid config entry, ignoring it." << std::endl;
      continue;
    }

    // get and trim key
    std::string key = line.substr(0, pos);
    key.erase(key.begin(), std::find_if(key.begin(), key.end(), [](int c) {
      return !std::isspace(c);
    }));
    key.erase(std::find_if(key.rbegin(), key.rend(), [](int c) {
      return !std::isspace(c);
    }).base(), key.end());

    if (!Config::is_conf_key_valid(key)) {
      std::cout << "Key \"" << key << "\" is not a valid key, ignoring config entry." << std::endl;
      continue;
    }

    // get and trim value
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

void Config::print_config() {
  std::cout << "Config values:" << std::endl
      << Config::CKEY_ODBC_DSN << " = "  << Config::config[Config::CKEY_ODBC_DSN] << std::endl
      << Config::CKEY_ODBC_USER << " = "  << Config::config[Config::CKEY_ODBC_USER] << std::endl
      << Config::CKEY_ODBC_PASS << " = "  << Config::config[Config::CKEY_ODBC_PASS] << std::endl
      << Config::CKEY_BROKER_HOST << " = "  << Config::config[Config::CKEY_BROKER_HOST] << std::endl
      << Config::CKEY_BROKER_PORT << " = "  << Config::config[Config::CKEY_BROKER_PORT] << std::endl
      << Config::CKEY_KAFKA_TOPIC << " = "  << Config::config[Config::CKEY_KAFKA_TOPIC] << std::endl
      << Config::CKEY_KAFKA_SASL_USER << " = "  << Config::config[Config::CKEY_KAFKA_SASL_USER] << std::endl
      << Config::CKEY_KAFKA_SASL_PASS << " = "  << Config::config[Config::CKEY_KAFKA_SASL_PASS] << std::endl
      << Config::CKEY_LOG_FILE << " = "  << Config::config[Config::CKEY_LOG_FILE] << std::endl
      << Config::CKEY_RULES_FILE << " = "  << Config::config[Config::CKEY_RULES_FILE] << std::endl
      << Config::CKEY_TRACK_VERSIONS << " = "  << Config::config[Config::CKEY_TRACK_VERSIONS] << std::endl
      << Config::CKEY_PROVD_PORT << " = "  << Config::config[Config::CKEY_PROVD_PORT] << std::endl
      << Config::CKEY_PROV_SRC << " = "  << Config::config[Config::CKEY_PROV_SRC] << std::endl
      << Config::CKEY_INPUT_SRC << " = "  << Config::config[Config::CKEY_INPUT_SRC] << std::endl
      << Config::CKEY_OUTPUT_DST << " = "  << Config::config[Config::CKEY_OUTPUT_DST]
      << std::endl;
}

bool Config::has_conf_key(std::string key) {
  if (Config::config.find(key) != Config::config.end()
      && Config::config[key] != "") {
    return true;
  }
  return false;
}

bool Config::is_conf_key_valid(std::string key) {
  if (key == Config::CKEY_ODBC_DSN)
    return true;
  if (key == Config::CKEY_ODBC_USER)
    return true;
  if (key == Config::CKEY_ODBC_PASS)
    return true;
  if (key == Config::CKEY_BROKER_HOST)
    return true;
  if (key == Config::CKEY_BROKER_PORT)
    return true;
  if (key == Config::CKEY_KAFKA_TOPIC)
    return true;
  if (key == Config::CKEY_KAFKA_SASL_USER)
    return true;
  if (key == Config::CKEY_KAFKA_SASL_PASS)
    return true;
  if (key == Config::CKEY_LOG_FILE)
    return true;
  if (key == Config::CKEY_RULES_FILE)
    return true;
  if (key == Config::CKEY_TRACK_VERSIONS)
    return true;
  if (key == Config::CKEY_PROVD_PORT)
    return true;
  if (key == Config::CKEY_PROV_SRC)
    return true;
  if (key == Config::CKEY_INPUT_SRC)
    return true;
  if (key == Config::CKEY_OUTPUT_DST)
    return true;

  return false;
}
