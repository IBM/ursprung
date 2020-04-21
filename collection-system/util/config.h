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

#ifndef UTIL_CONFIG_H_
#define UTIL_CONFIG_H_

#include <map>
#include <string>

typedef std::map<std::string, std::string> config_opts_t;

/**
 * Parse and store 'key = value' based configs into
 * a static map to be available to the entire program.
 */
class Config {
private:
  static bool is_conf_key_valid(std::string key);

public:
  // valid configuration keys
  static const std::string CKEY_DB_HOST;
  static const std::string CKEY_DB_PORT;
  static const std::string CKEY_DB_NAME;
  static const std::string CKEY_DB_USER;
  static const std::string CKEY_DB_PASS;
  static const std::string CKEY_BROKER_HOST;
  static const std::string CKEY_BROKER_PORT;
  static const std::string CKEY_KAFKA_TOPIC;
  static const std::string CKEY_KAFKA_SASL_USER;
  static const std::string CKEY_KAFKA_SASL_PASS;
  static const std::string CKEY_LOG_FILE;
  static const std::string CKEY_RULES_FILE;
  static const std::string CKEY_TRACK_VERSIONS;
  static const std::string CKEY_PROVD_PORT;

  static config_opts_t config;
  static int parse_config(std::string path);
  static void print_config();
};

#endif /* UTIL_CONFIG_H_ */
