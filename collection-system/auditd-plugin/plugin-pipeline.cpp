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

#include <signal.h>
#include <libaudit.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <librdkafka/rdkafkacpp.h>

#include "plugin-pipeline.h"
#include "plugin-util.h"
#include "logger.h"
#include "auditd-event.h"
#include "config.h"
#include "signal-handling.h"
#include "error.h"
#include "msg-output-stream.h"

#define GETTID 186

// pointer to indicate that no more events are coming
void *DONE_PTR = (void*) 0xdeadbeef;

/*------------------------------
 * Stage
 *------------------------------*/

void PipelineStep::mask_signals() {
  sigset_t signal_mask;
  sigemptyset(&signal_mask);
  sigfillset(&signal_mask);

  int rc = pthread_sigmask(SIG_BLOCK, &signal_mask, NULL);
  if (rc) {
    LOG_ERROR("Stage: Error, pthread_sigmask failed: "
            << std::string(strerror(errno)));
  }
}

/*------------------------------
 * Extractor
 *------------------------------*/

void ExtractorStep::handle_audisp_event(auparse_state_t *au,
    auparse_cb_event_t cb_event_type, void *user_data) {
  ExtractorStep *that = (ExtractorStep*) user_data;
  int num = 0;

  if (cb_event_type != AUPARSE_CB_EVENT_READY)
    return;

  // Position at first record in this event.
  //auparse_first_record(au);
  while (auparse_goto_record_num(au, num) > 0) {
    // filter for our key
    const char *record_key = auparse_find_field(au, "key");
    std::string record_key_str("");
    if (record_key) {
      record_key_str = record_key;
    }

    if (record_key && Config::config[Config::CKEY_AUDITD_KEY].compare(record_key) == 0) {
      LOG_DEBUG("Keeping event: key " << record_key_str);
    } else {
      if (record_key) {
        LOG_DEBUG("Skipping event whose first record has key " << record_key_str
                << " != " << that->state_->cfg->_auditdKey);
        stats->skipped_auditd_event();
      } else {
        LOG_DEBUG("Skipping event without key");
        stats->skipped_auditd_event();
      }
      num++;
      continue;
    }

    // we only want syscall events
    bool is_syscall_event = (0 <= AuparseInterface::get_syscall_record_number(au));
    if (!is_syscall_event) {
      stats->skipped_auditd_event();
      num++;
      continue;
    }

    // update statistics
    stats->received_auditd_event();

    // send to next stage of pipeline
    SyscallEvent *se = nullptr;
#ifdef __linux__
    SyscallEvent *se = new SyscallEvent(au);
#endif
    that->out->push(se);
    num++;
  }
}

int ExtractorStep::run() {
  mask_signals();
  char tmp[MAX_AUDIT_MESSAGE_LENGTH];
  pid_t tid = syscall(GETTID);
  LOG_DEBUG("Extractor running with pid " << tid);

  // initialize the auparse library
  au = auparse_init(AUSOURCE_FEED, 0);
  if (au == NULL) {
    LOG_ERROR("Extractor exiting due to auparse init errors");
    // propagate
    if (out) {
      out->push(DONE_PTR);
    }
    return -1;
  }

  // there may be some mumbo jumbo so that 'this' is set using std::bind, but this approach works OK
  auparse_add_callback(au, &ExtractorStep::handle_audisp_event, this, NULL);

  do {
    fd_set read_mask;
    struct timeval tv;
    int retval = -1;
    int read_size = 0;

    // load configuration
    if (signal_handling::hup) {
      LOG_INFO("Detected SIGHUP, reloading config.");
      signal_handling::hup = 0;
      Config::parse_config(config_path);
      Config::print_config();
    }
    do {
      // if we timed out & have events, shake them loose
      if (retval == 0 && auparse_feed_has_data(au)) {
        auparse_feed_age_events(au);
      }

      tv.tv_sec = 3;
      tv.tv_usec = 0;
      FD_ZERO(&read_mask);
      FD_SET(0, &read_mask);
      if (auparse_feed_has_data(au))
        retval = select(1, &read_mask, NULL, NULL, &tv);
      else
        retval = select(1, &read_mask, NULL, NULL, NULL);
    } while (retval <= 0 && signal_handling::running); // no need to check for errno == EINTR, we masked off all signals

    // main event loop
    if (retval > 0) {
      if ((read_size = read(0, tmp, MAX_AUDIT_MESSAGE_LENGTH)) > 0) {
        auparse_feed(au, tmp, strnlen(tmp, read_size));
      }
    }
    // check EOF
    if (read_size == 0) {
      LOG_INFO("Found EOF, stopping main loop.");
      break;
    }
  } while (signal_handling::running);

  LOG_INFO("Detected SIGTERM, shutting down.");

  // flush any accumulated events from queue
  auparse_flush_feed(au);
  auparse_destroy(au);

  // tell downstream no more is coming
  if (out) {
    out->push(DONE_PTR);
  }

  return 0;
}

/*------------------------------
 * Transformer
 *------------------------------*/

int TransformerStep::run() {
  mask_signals();
  pid_t tid = syscall(GETTID);
  LOG_INFO("Transformer running with pid " << tid);

  time_t start_readtime;
  time_t curr_time;
  unsigned long long num_events_processed = 0;
  // TODO make configurable
  int reap_freq = 1;
  // reap process events at least every 5 seconds
  int reap_freq_time = 5;
  start_readtime = time(NULL);

  // loop until we see DONE_PTR
  while (1) {
    // blocking
    void *elt = in->pop();

    if (elt == DONE_PTR) {
      break;
    }

    if (elt != NULL) {
      // normal event, apply to our model
      SyscallEvent *se = (SyscallEvent*) elt;
      num_events_processed++;
      // passes ownership of se to osModel
      osModel.apply_syscall(se);
    }

    // regularly propagate ready events downstream
    curr_time = time(NULL);
    if (num_events_processed % reap_freq == 0
        || difftime(curr_time, start_readtime) >= reap_freq_time) {
      send_ready_events();
      start_readtime = time(NULL);
    }
  }

  // cleanup
  LOG_INFO("Transformer::stopping");
  send_ready_events();
  if (out) {
    out->push(DONE_PTR);
  }

  return 0;
}

void TransformerStep::send_ready_events() {
  std::vector<Event*> reaped_events = osModel.reap_os_events();
  LOG_DEBUG("Transformer: Reaped " << std::to_string(reaped_events.size()) << " os events");

  for (Event *e : reaped_events) {
    assert(e);

    // filter syscall events if configured
    bool keep = true;
    if (e->get_type() == EventType::SYSCALL_EVENT) {
      if (!Config::get_bool(Config::CKEY_EMIT_SYSCALL_EVENTS)) {
        keep = false;
      }
    }

    if (keep) {
      // send event to next stage
      out->push(e);
    } else {
      LOG_DEBUG("Transformer: Filtering out event " << e->serialize());
    }
  }
}

/*------------------------------
 * Loader
 *------------------------------*/

LoaderStep::LoaderStep(SynchronizedQueue<void*> *in, SynchronizedQueue<void*> *out,
    std::shared_ptr<Statistics> stats, std::unique_ptr<MsgOutputStream> out_stream) :
    PipelineStep(in, out, stats),
    out_stream { std::move(out_stream) } {
  assert(in);
  assert(!out);

  // get hostname
  char hn[128];
  snprintf(hn, sizeof(hn), "unknown");
  gethostname(hn, sizeof(hn));
  hn[sizeof(hn) - 1] = '\0';

  // use "hostname" not "FQDN" by default
  char *first_dot = strchr(hn, '.');
  if (first_dot) {
      *first_dot = '\0';
  }

  // additional domain qualifiers can be specified through the hostname suffix
  hostname = hn + Config::config[Config::CKEY_HOSTNAME_SUFFIX];
}

int LoaderStep::run() {
  mask_signals();
  pid_t tid = syscall(GETTID);
  LOG_DEBUG("Loader running with pid " << tid);

  while (1) {
    void *elt = in->pop();
    if (elt == DONE_PTR) {
      break;
    }
    Event *evt = (Event*) elt;

    // extract the partition key component (pid or pgid)
    std::string key;
    switch (evt->get_type()) {
    case EventType::PROCESS_EVENT:
      key = ((ProcessEvent*) evt)->pid;
      break;
    case EventType::PROCESS_GROUP_EVENT:
      key = ((ProcessGroupEvent*) evt)->pgid;
      break;
    case EventType::SYSCALL_EVENT:
      key = ((SyscallEvent*) evt)->pid;
      break;
    case EventType::IPC_EVENT:
      key = ((IPCEvent*) evt)->src_pid;
      break;
    case EventType::SOCKET_EVENT:
      key = ((SocketEvent*) evt)->pid;
      break;
    case EventType::SOCKET_CONNECT_EVENT:
      key = ((SocketConnectEvent*) evt)->pid;
      break;
    default:
      assert(!"Error, unknown OSEventType");
    }
    std::string combined_key = key + hostname;

    if (out_stream->send(evt->serialize(), RdKafka::Topic::PARTITION_UA, &combined_key) == NO_ERROR) {
      stats->sent_event();
    } else {
      LOG_DEBUG("prov-auditd: sendData returned MoRc " << std::to_string(sendRc));
    }

    delete evt;
  }

  // cleanup
  LOG_INFO("Loader::stopping");
  if (out) {
    out->push(DONE_PTR);
  }

  return 0;
}
