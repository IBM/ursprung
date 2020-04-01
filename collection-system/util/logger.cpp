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

#include "logger.h"

#include <iostream>
#include <string.h>
#include <unistd.h>

std::string Logger::log_file_name = "";

Logger::Logger(Level l) :
    level { l }, lock { } {
  // check if we have a log configuration present in the current directory
  if (!log_file_name.empty()) {
    std::cout << "Log file name set, will start logging to "
        << Logger::log_file_name << std::endl;
    backend = new FileBackend(Logger::log_file_name);
  } else {
    // otherwise, use console backend
    std::cout << "Log file name not set, logging to console" << std::endl;
    backend = new ConsoleBackend();
  }
}

Logger::~Logger() {
  delete backend;
}

void Logger::set_log_file_name(std::string filename) {
  log_file_name = filename;
}

void Logger::log(std::string msg, char const *function, int line) {
  std::string log_msg = pretty_utc_time() + " [" + function + ":"
      + std::to_string(line) + "]";

  switch (level) {
  case Level::Fatal:
    log_msg += " [FATAL] - " + msg;
    break;
  case Level::Error:
    log_msg += " [ERROR] - " + msg;
    break;
  case Level::Warning:
    log_msg += " [WARN] - " + msg;
    break;
  case Level::Info:
    log_msg += " [INFO] - " + msg;
    break;
  case Level::Debug:
    log_msg += " [DEBUG] - " + msg;
    break;
  case Level::Performance:
    log_msg += " [PERF] - " + msg;
    break;
  default:
    log_msg += " [NONE] - " + msg;
    break;
  }

  std::unique_lock<std::mutex> uniqueLock(lock);
  backend->log_msg(log_msg);
}

std::string Logger::pretty_utc_time() {
  time_t now = time(NULL);
  char buf[64];
  ctime_r(&now, buf);
  buf[strlen(buf) - 1] = '\0'; // wipe newline
  return buf;
}

Logger& fatal_logger() {
  static Logger logger(Level::Fatal);
  return logger;
}

Logger& error_logger() {
  static Logger logger(Level::Error);
  return logger;
}

Logger& warn_logger() {
  static Logger logger(Level::Warning);
  return logger;
}

Logger& info_logger() {
  static Logger logger(Level::Info);
  return logger;
}

Logger& debug_logger() {
  static Logger logger(Level::Debug);
  return logger;
}

Logger& performance_logger() {
  static Logger logger(Level::Performance);
  return logger;
}

void ConsoleBackend::log_msg(std::string msg) {
  std::cout << msg << std::endl;
}

FileBackend::FileBackend(std::string filename) {
  out_file.open(filename, std::ofstream::out | std::ofstream::app);
}

FileBackend::~FileBackend() {
  out_file.close();
}

void FileBackend::log_msg(std::string msg) {
  out_file << msg << std::endl;
}
