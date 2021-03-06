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

#include <string>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

#include "msg-output-stream.h"
#include "auditd-event.h"
#include "logger.h"
#include "plugin-pipeline.h"
#include "config.h"
#include "signal-handling.h"
#include "constants.h"
#include "error.h"
#include "kafka-output-stream.h"

std::unique_ptr<MsgOutputStream> create_configured_output_stream() {
  // create the output stream for the plugin
  std::unique_ptr<MsgOutputStream> out;
  std::string out_dst = Config::config[Config::CKEY_OUTPUT_DST];
  if (out_dst == constants::FILE_STREAM) {
    // make sure all File relevant properties are set in config
    if (Config::has_conf_key(Config::CKEY_OUT_FILE)) {
      out = std::make_unique<FileOutputStream>(Config::config[Config::CKEY_OUT_FILE]);
    } else {
      LOGGER_LOG_ERROR("File ouput destination needs to specify " << Config::CKEY_OUT_FILE << ".");
      return nullptr;
    }
  } else if (out_dst == constants::KAFKA_STREAM) {
    if (Config::has_conf_key(Config::CKEY_KAFKA_BROKERS)
        && Config::has_conf_key(Config::CKEY_KAFKA_TOPIC)) {
      out = std::make_unique<KafkaOutputStream>(
          Config::config[Config::CKEY_KAFKA_TOPIC],
          Config::config[Config::CKEY_KAFKA_BROKERS]);
    } else {
      LOGGER_LOG_ERROR("Kafka input source needs to specify "
          << Config::CKEY_KAFKA_BROKERS << ", "
          << Config::CKEY_KAFKA_TOPIC << ", and "
          << Config::CKEY_KAFKA_GROUP_ID << ".");
      return nullptr;
    }
  } else {
    LOGGER_LOG_ERROR("Unknown output destination " << out_dst);
    return nullptr;
  }

  return out;
}

int main(int argc, char *argv[]) {
  std::cout << "-----------------------------------------------------------" << std::endl <<
               "                    Ursprung auditd plugin                 " << std::endl <<
               "-----------------------------------------------------------" << std::endl;

  if (argc != 2) {
    fprintf(stderr, "Error, usage: %s configFile\n", argv[0]);
    return -1;
  }

  // make sure that config file exists
  struct stat statBuf;
  if (stat(argv[1], &statBuf)) {
    fprintf(stderr, "Error, no such configFile %s", argv[1]);
    return -1;
  }

  signal_handling::setup_handlers();

  // populate config and store path to config file for config reload
  Config::parse_config(std::string(argv[1]));
  std::string configPath = std::string(argv[1]);
  Config::print_config();

  Logger::set_log_file_name(Config::config[Config::CKEY_LOG_FILE]);

  // create output stream
  std::unique_ptr<MsgOutputStream>  out = create_configured_output_stream();
  if (!out) {
    LOGGER_LOG_ERROR("Error, could not create configured output stream.");
    return -1;

  }
  if (out->open() != NO_ERROR) {
    LOGGER_LOG_ERROR("Error, could not open output stream.");
    return -1;
  }

  SynchronizedQueue<void*> extractor_to_transformer;
  SynchronizedQueue<void*> transformer_to_loader;
  std::shared_ptr<Statistics> stats = std::make_shared<Statistics>();

  ExtractorStep extractor(nullptr, &extractor_to_transformer, stats);
  TransformerStep transformer(&extractor_to_transformer,
      &transformer_to_loader, stats);
  LoaderStep loader(&transformer_to_loader, nullptr, stats, std::move(out));

  extractor.set_config_path(configPath);
  extractor.start();
  transformer.start();
  loader.start();

  // wait for pipeline threads to finish
  extractor.join();
  transformer.join();
  loader.join();

  if (!signal_handling::running) {
    LOGGER_LOG_INFO("Exiting on stop request\n");
  } else {
    LOGGER_LOG_INFO("Exiting on stdin EOF\n");
  }

  return 0;
}
