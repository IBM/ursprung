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
  /*
   * Checks whether the provided configuration key is valid, i.e.
   * if it equals one of the above defined CKEY_ constants.
   */
  static bool is_conf_key_valid(std::string key);

public:
  // valid configuration keys
  static const std::string CKEY_ODBC_DSN;
  static const std::string CKEY_ODBC_USER;
  static const std::string CKEY_ODBC_PASS;
  static const std::string CKEY_BROKER_HOST;
  static const std::string CKEY_BROKER_PORT;
  static const std::string CKEY_KAFKA_TOPIC;
  static const std::string CKEY_KAFKA_SASL_USER;
  static const std::string CKEY_KAFKA_SASL_PASS;
  static const std::string CKEY_LOG_FILE;
  static const std::string CKEY_RULES_FILE;
  static const std::string CKEY_TRACK_VERSIONS;
  static const std::string CKEY_PROVD_PORT;
  static const std::string CKEY_PROV_SRC;
  static const std::string CKEY_INPUT_SRC;
  static const std::string CKEY_OUTPUT_DST;
  static const std::string CKEY_IN_FILE;
  static const std::string CKEY_OUT_FILE;

  static config_opts_t config;
  /*
   * Parses the config file at the provided path. The config
   * format is expected to be a simple 'key = value' format.
   * Comments are prefixed by '#'.
   */
  static int parse_config(std::string path);
  static bool has_conf_key(std::string key);
  static bool get_bool(std::string key);
  static long get_long(std::string key) { return std::stol(config[key]); }
  static void print_config();
};

#endif /* UTIL_CONFIG_H_ */
