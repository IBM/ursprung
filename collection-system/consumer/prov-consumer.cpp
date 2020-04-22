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
#include "abstract-consumer.h"
#include "scale-consumer.h"
#include "auditd-consumer.h"
#include "msg-input-stream.h"
#include "msg-output-stream.h"
#include "kafka-input-stream.h"
#include "db-output-stream.h"

// supported provenance input sources
const std::string AUDITD_SRC = "auditd";
const std::string SCALE_SRC = "scale";

// stream types
const std::string ODBC_STREAM = "ODBC";
const std::string KAFKA_STREAM = "Kafka";
const std::string FILE_STREAM = "File";

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

std::unique_ptr<MsgInputStream> get_input_stream(std::string in_src) {
  if (in_src == KAFKA_STREAM) {
    // make sure all Kafka relevant properties are set in config
    if (Config::has_conf_key(Config::CKEY_BROKER_HOST)
        && Config::has_conf_key(Config::CKEY_BROKER_PORT)
        && Config::has_conf_key(Config::CKEY_KAFKA_TOPIC)) {
      return std::make_unique<KafkaInputStream>(
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
  } else {
    LOG_ERROR("Unknown input source " << in_src);
    return nullptr;
  }
}

std::unique_ptr<MsgOutputStream> get_output_stream(std::string out_dst) {
  if (out_dst == ODBC_STREAM) {
    // make sure all DB relevant properties are set in config
    if (Config::has_conf_key(Config::CKEY_ODBC_DSN)) {
      // construct the connection string for an ODBC connection
      std::string conn = "ODBC" + Config::config[Config::CKEY_ODBC_USER] + ":" +
          Config::config[Config::CKEY_ODBC_PASS] + "@" +
          Config::config[Config::CKEY_ODBC_DSN];

      // construct the db output stream based on the provenance source
      if (Config::config[Config::CKEY_PROV_SRC] == AUDITD_SRC) {
        std::unique_ptr<DBOutputStream> db_out = std::make_unique<DBOutputStream>(
            conn, "", "", true, true, 0);
        db_out->set_multiplex_group(DBOutputStream::AUDIT_SYSCALL_EVENTS_TABLENAME,
            DBOutputStream::AUDIT_SYSCALL_EVENTS_SCHEMA,
            DBOutputStream::AUDIT_SYSCALL_EVENTS_KEY);
        db_out->set_multiplex_group(DBOutputStream::AUDIT_PROCESS_EVENTS_TABLENAME,
            DBOutputStream::AUDIT_PROCESS_EVENTS_SCHEMA,
            DBOutputStream::AUDIT_PROCESS_EVENTS_KEY);
        db_out->set_multiplex_group(DBOutputStream::AUDIT_PROCESSGROUP_EVENTS_TABLENAME,
            DBOutputStream::AUDIT_PROCESSGROUP_EVENTS_SCHEMA,
            DBOutputStream::AUDIT_PROCESSGROUP_EVENTS_KEY);
        db_out->set_multiplex_group(DBOutputStream::AUDIT_IPC_EVENTS_TABLENAME,
            DBOutputStream::AUDIT_IPC_EVENTS_SCHEMA,
            DBOutputStream::AUDIT_IPC_EVENTS_KEY);
        db_out->set_multiplex_group(DBOutputStream::AUDIT_SOCKET_EVENTS_TABLENAME,
            DBOutputStream::AUDIT_SOCKET_EVENTS_SCHEMA,
            DBOutputStream::AUDIT_SOCKET_EVENTS_KEY);
        db_out->set_multiplex_group(DBOutputStream::AUDIT_SOCKETCONNECT_EVENTS_TABLENAME,
            DBOutputStream::AUDIT_SOCKETCONNECT_EVENTS_SCHEMA,
            DBOutputStream::AUDIT_SOCKETCONNECT_EVENTS_KEY);
        return db_out;
      } else if (Config::config[Config::CKEY_PROV_SRC] == SCALE_SRC) {
        std::unique_ptr<DBOutputStream> db_out = std::make_unique<DBOutputStream>(
            conn, DBOutputStream::SCALE_EVENTS_TABLENAME,
            DBOutputStream::SCALE_EVENTS_TABLENAME, true, false);
        return db_out;
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
  } else {
    LOG_ERROR("Unknown output destination " << out_dst);
    return nullptr;
  }
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

  // determine input and output types
  std::unique_ptr<MsgInputStream> in = get_input_stream(
      Config::config[Config::CKEY_INPUT_SRC]);
  std::unique_ptr<MsgOutputStream> out = get_output_stream(
      Config::config[Config::CKEY_OUTPUT_DST]);
  if (!in || !out) {
    LOG_ERROR("Couldn't set up input/output connections.");
    exit(-1);
  }

  // create the consumer
  AbstractConsumer* consumer;
  if (Config::config[Config::CKEY_PROV_SRC] == AUDITD_SRC) {
    // TODO somehow combine consumer src/destination and input/output streams in on object
    consumer = new AuditdConsumer(CS_PROV_AUDITD, std::move(in),
        CD_ODBC, std::move(out));
  } else if (Config::config[Config::CKEY_PROV_SRC] == SCALE_SRC) {
    // TODO somehow combine consumer src/destination and input/output streams in on object
    // TODO correctly interpret config option for tracking versions
    consumer = new ScaleConsumer(CS_PROV_GPFS, std::move(in),
        CD_ODBC, std::move(out), false);
  }
}
