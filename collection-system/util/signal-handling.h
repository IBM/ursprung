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

#ifndef UTIL_SIGNAL_HANDLING_H_
#define UTIL_SIGNAL_HANDLING_H_

#include <signal.h>

#include "logger.h"

namespace signal_handling {
static volatile sig_atomic_t running = 1;
static volatile sig_atomic_t hup = 0;

static void term_handler(int sig) {
  LOGGER_LOG_INFO("Shutting down...");
  running = 0;
}

static void hup_handler(int sig) {
    hup = 1;
}

static void setup_handlers() {
  struct sigaction sa;
  // unblock all signals
  sigfillset(&sa.sa_mask);
  sigprocmask(SIG_UNBLOCK, &sa.sa_mask, 0);
  sa.sa_flags = 0;
  sigemptyset(&sa.sa_mask);
  // by default, set ignore handler for all signals
  sa.sa_handler = SIG_DFL;
  for (int i = 1; i < NSIG; i++)
    sigaction(i, &sa, NULL);
  // now set the handlers for SIGTERM, SIGINT, and SIGHUP
  sa.sa_handler = term_handler;
  sigaction(SIGTERM, &sa, NULL);
  sa.sa_handler = term_handler;
  sigaction(SIGINT, &sa, NULL);
  sa.sa_handler = hup_handler;
  sigaction(SIGHUP, &sa, NULL);
}
}

#endif /* UTIL_SIGNAL_HANDLING_H_ */
