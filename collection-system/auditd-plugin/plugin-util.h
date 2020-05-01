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

#ifndef AUDITD_PLUGIN_PLUGIN_UTIL_H_
#define AUDITD_PLUGIN_PLUGIN_UTIL_H_

#include <string>
#include <auparse.h>

class AuparseInterface {
public:
  /*
   * Returns system call record number from auditd stream, or -1 if
   * not found. At return, au state is unchanged.
   */
  static int get_syscall_record_number(auparse_state_t *au);
};

class Statistics {
private:
  int report_freq_in_seconds;
  std::string report_prefix;
  ssize_t num_received_events;
  ssize_t num_skipped_events;
  ssize_t num_sent_events;
  time_t last_report;

  void try_report();
  void report();

public:
  Statistics();

  void received_auditd_event() {
    num_received_events++;
    try_report();
  }
  void skipped_auditd_event() {
    num_skipped_events++;
    try_report();
  }
  void sent_event() {
    num_sent_events++;
    try_report();
  }
};

#endif /* AUDITD_PLUGIN_PLUGIN_UTIL_H_ */
