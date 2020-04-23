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
#include "constants.h"
#include "msg-input-stream.h"
#include "msg-output-stream.h"
#include "kafka-input-stream.h"
#include "db-output-stream.h"
#include "abstract-consumer.h"
#include "auditd-consumer.h"
#include "scale-consumer.h"

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

std::unique_ptr<AbstractConsumer> create_configured_consumer() {
  // create the input stream for the consumer
  std::unique_ptr<MsgInputStream> in;
  std::string in_src = Config::config[Config::CKEY_INPUT_SRC];
  if (in_src == constants::KAFKA_STREAM) {
    // make sure all Kafka relevant properties are set in config
    if (Config::has_conf_key(Config::CKEY_BROKER_HOST)
        && Config::has_conf_key(Config::CKEY_BROKER_PORT)
        && Config::has_conf_key(Config::CKEY_KAFKA_TOPIC)) {
      in = std::make_unique<KafkaInputStream>(
          Config::config[Config::CKEY_BROKER_HOST],
          Config::config[Config::CKEY_BROKER_PORT],
          Config::config[Config::CKEY_KAFKA_TOPIC]);
    } else {
      LOG_ERROR("Kafka input source needs to specify "
          << Config::CKEY_BROKER_HOST << ", "
          << Config::CKEY_BROKER_PORT << ", and "
          << Config::CKEY_KAFKA_TOPIC << ".");
      return nullptr;
    }
  } else if (in_src == constants::FILE_STREAM) {
    // make sure all File relevant properties are set in config
    if (Config::has_conf_key(Config::CKEY_IN_FILE)) {
      in = std::make_unique<FileInputStream>(Config::config[Config::CKEY_IN_FILE]);
    } else {
      LOG_ERROR("File input source needs to specify " << Config::CKEY_IN_FILE << ".");
      return nullptr;
    }
  } else {
    LOG_ERROR("Unknown input source " << in_src);
    return nullptr;
  }

  // create the output stream for the consumer
  std::unique_ptr<MsgOutputStream> out;
  std::string out_dst = Config::config[Config::CKEY_OUTPUT_DST];
  if (out_dst == constants::ODBC_STREAM) {
    // make sure all DB relevant properties are set in config
    if (Config::has_conf_key(Config::CKEY_ODBC_DSN)) {
      // construct the connection string for an ODBC connection
      std::string conn = "ODBC " + Config::config[Config::CKEY_ODBC_USER] + ":" +
          Config::config[Config::CKEY_ODBC_PASS] + "@" +
          Config::config[Config::CKEY_ODBC_DSN];

      // construct the db output stream based on the provenance source
      if (Config::config[Config::CKEY_PROV_SRC] == constants::AUDITD_SRC) {
        out = std::make_unique<DBOutputStream>(conn, "", "", true, true, 0);
        ((DBOutputStream*) out.get())->set_multiplex_group(
            constants::AUDIT_SYSCALL_EVENTS_TABLENAME,
            constants::AUDIT_SYSCALL_EVENTS_SCHEMA,
            constants::AUDIT_SYSCALL_EVENTS_KEY);
        ((DBOutputStream*) out.get())->set_multiplex_group(
            constants::AUDIT_PROCESS_EVENTS_TABLENAME,
            constants::AUDIT_PROCESS_EVENTS_SCHEMA,
            constants::AUDIT_PROCESS_EVENTS_KEY);
        ((DBOutputStream*) out.get())->set_multiplex_group(
            constants::AUDIT_PROCESSGROUP_EVENTS_TABLENAME,
            constants::AUDIT_PROCESSGROUP_EVENTS_SCHEMA,
            constants::AUDIT_PROCESSGROUP_EVENTS_KEY);
        ((DBOutputStream*) out.get())->set_multiplex_group(
            constants::AUDIT_IPC_EVENTS_TABLENAME,
            constants::AUDIT_IPC_EVENTS_SCHEMA,
            constants::AUDIT_IPC_EVENTS_KEY);
        ((DBOutputStream*) out.get())->set_multiplex_group(
            constants::AUDIT_SOCKET_EVENTS_TABLENAME,
            constants::AUDIT_SOCKET_EVENTS_SCHEMA,
            constants::AUDIT_SOCKET_EVENTS_KEY);
        ((DBOutputStream*) out.get())->set_multiplex_group(
            constants::AUDIT_SOCKETCONNECT_EVENTS_TABLENAME,
            constants::AUDIT_SOCKETCONNECT_EVENTS_SCHEMA,
            constants::AUDIT_SOCKETCONNECT_EVENTS_KEY);
      } else if (Config::config[Config::CKEY_PROV_SRC] == constants::SCALE_SRC) {
        out = std::make_unique<DBOutputStream>(conn, constants::SCALE_EVENTS_TABLENAME,
            constants::SCALE_EVENTS_TABLENAME, true, false);
      } else {
        LOG_ERROR("Unsupported provenance source "
                  << Config::config[Config::CKEY_PROV_SRC] << ".");
        return nullptr;
      }
    } else {
      LOG_ERROR("ODBC output destination source needs to specify "
          << Config::CKEY_ODBC_DSN << ".");
      return nullptr;
    }
  } else if (out_dst == constants::FILE_STREAM) {
    // make sure all File relevant properties are set in config
    if (Config::has_conf_key(Config::CKEY_OUT_FILE)) {
      out = std::make_unique<FileOutputStream>(Config::config[Config::CKEY_OUT_FILE]);
    } else {
      LOG_ERROR("File ouput destination needs to specify " << Config::CKEY_OUT_FILE << ".");
      return nullptr;
    }
  } else {
    LOG_ERROR("Unknown output destination " << out_dst);
    return nullptr;
  }

  // create the consumer
  std::unique_ptr<AbstractConsumer> consumer;
  ConsumerDestination c_dst;
  if (out_dst == "ODBC") c_dst = CD_ODBC;
  else if (out_dst == "File") c_dst = CD_FILE;

  if (Config::config[Config::CKEY_PROV_SRC] == constants::AUDITD_SRC) {
    consumer = std::make_unique<AuditdConsumer>(CS_PROV_AUDITD, std::move(in),
        c_dst, std::move(out));
  } else if (Config::config[Config::CKEY_PROV_SRC] == constants::SCALE_SRC) {
    // TODO correctly interpret config option for tracking versions
    consumer = std::make_unique<ScaleConsumer>(CS_PROV_GPFS, std::move(in),
        c_dst, std::move(out), false);
  }

  return consumer;
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

  // create the consumer
  std::unique_ptr<AbstractConsumer> consumer = create_configured_consumer();
  consumer->run();
}
