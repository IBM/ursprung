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

#include <string.h>

#include "plugin-util.h"
#include "logger.h"

/*------------------------------
 * AuparseInterface
 *------------------------------*/

int AuparseInterface::get_syscall_record_number(auparse_state_t *au) {
  int pos = auparse_get_record_num(au);

  auparse_first_record(au);
  bool found = false;
  int record_number = 0;
  do {
    if (strcmp(auparse_get_type_name(au), "SYSCALL") == 0) {
      found = true;
      break;
    }
    record_number++;
  } while (0 < auparse_next_record(au));

  auparse_goto_record_num(au, pos);
  return found ? record_number : -1;
}

/*------------------------------
 * Statistics
 *------------------------------*/

Statistics::Statistics() :
    num_received_events { 0 },
    num_skipped_events { 0 },
    num_sent_events { 0 },
    report_freq_in_seconds { 1 } {
  last_report = time(NULL);
  report_prefix = "Report (interval " + std::to_string(report_freq_in_seconds) + " seconds)";
}

void Statistics::try_report() {
  time_t now = time(NULL);
  if (report_freq_in_seconds < difftime(now, last_report)) {
    report();
    last_report = now;
    // FIXME possible race condition if both Extractor and Loader are calling tryReport
    num_received_events = 0;
    num_skipped_events = 0;
    num_sent_events = 0;
  }
}

void Statistics::report() {
  double compression_factor = (1.0 * num_received_events) / num_sent_events;
  char report[256];
  snprintf(report, sizeof(report), "%s:\n  %ld auditd events received\n  "
      "%ld auditd events skipped\n %ld OS events sent\n  %.1f event compression "
      "factor", report_prefix.c_str(), num_received_events, num_skipped_events,
      num_sent_events, compression_factor);
  LOG_INFO(report);
}
