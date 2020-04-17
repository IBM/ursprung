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

#include <iostream>
#include <getopt.h>

#include "error.h"
#include "config.h"
#include "logger.h"

void print_usage() {
  std::cout << "Usage :\n"
      " -c, --config       path to config file      (required)\n"
      " -l, --log-file     path to log file         (optional)\n"
      "";
}

/**
 * Parse command line arguments. Arugments specified
 * on the command line overwrite any arguments defined
 * in the config file.
 */
int parse_args(int argc, char **argv) {
  int rc = NO_ERROR;

  const char *const shortops = "hc:l:";
  const option longops[] = {
      { "help", no_argument, nullptr, 'h' },
      { "config", required_argument, nullptr, 'c' },
      { "log-file", required_argument,nullptr, 'l' }
  };

  if (argc <= 1) {
    print_usage();
    return ERROR_NO_RETRY;
  }

  int opt;
  bool config_provided = false;
  while ((opt = getopt_long(argc, argv, shortops, longops, nullptr)) != -1) {
    switch (opt) {
    case 'c':
      if ((rc = Config::parse_config(std::string(optarg))) != NO_ERROR) {
        return rc;
      }
      config_provided = true;
      break;
    case 'l':
      Config::config[Config::CKEY_LOG_FILE] = std::string(optarg);
      break;
    case 'h':
      print_usage();
      exit(0);
    }
  }

  if (!config_provided) {
    print_usage();
    return ERROR_NO_RETRY;
  }
  Config::print_config();

  return rc;
}

int main(int argc, char **argv) {
  std::cout << "-----------------------------------------------------------" << std::endl <<
               "              Ursprung Provenance Consumer                 " << std::endl <<
               "-----------------------------------------------------------" << std::endl;

  // parse command line arguments and config file
  if (parse_args(argc, argv)) {
    exit(-1);
  }
  // configure Logger
  Logger::set_log_file_name(Config::config[Config::CKEY_LOG_FILE]);
}
