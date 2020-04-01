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

#ifndef COMMON_GLOBAL_LOGGER_H_
#define COMMON_GLOBAL_LOGGER_H_

#include <iostream>
#include <sstream>
#include <fstream>
#include <cstdint>
#include <string>
#include <mutex>

#define LOG(logger, msg)                    \
  logger.log(                               \
    static_cast<std::ostringstream&>(       \
      std::ostringstream().flush() << msg   \
    ).str(),                                \
    __FUNCTION__,                           \
    __LINE__                                \
  );

// always log fatal, errors, and warnings
#define LOG_FATAL(msg) LOG(fatal_logger(), msg)
#define LOG_ERROR(msg) LOG(error_logger(), msg)
#define LOG_WARN(msg) LOG(warn_logger(), msg)

#ifndef INFO
#define LOG_INFO(msg) do {} while(0)
#else
#define LOG_INFO(msg) LOG(info_logger(), msg)
#endif

#ifndef DEBUG
#define LOG_DEBUG(msg) do {} while(0)
#else
#define LOG_DEBUG(msg) LOG(debug_logger(), msg)
#endif

#ifndef PERF
#define LOG_PERF(msg) do {} while(0)
#else
#define LOG_PERF(msg) LOG(performance_logger(), msg)
#endif

enum class Level : uint8_t {
  Fatal, Error, Warning, Info, Debug, Performance
};

class LogBackend;

class Logger {
private:
  static std::string log_file_name;
  Level level;
  LogBackend *backend;
  std::mutex lock;

public:
  Logger(Level l);
  ~Logger();
  static void set_log_file_name(std::string filename);
  void log(std::string const msg, char const *function, int line);
  std::string pretty_utc_time();
};

Logger& fatal_logger();
Logger& error_logger();
Logger& warn_logger();
Logger& info_logger();
Logger& debug_logger();
Logger& performance_logger();

class LogBackend {
public:
  virtual ~LogBackend() {};
  virtual void log_msg(std::string msg) = 0;
};

class ConsoleBackend: public LogBackend {
public:
  ~ConsoleBackend() {};
  void log_msg(std::string msg);
};

class FileBackend: public LogBackend {
private:
  std::ofstream out_file;

public:
  FileBackend(std::string filename);
  ~FileBackend();
  void log_msg(std::string msg);
};


#endif /* COMMON_GLOBAL_LOGGER_H_ */
