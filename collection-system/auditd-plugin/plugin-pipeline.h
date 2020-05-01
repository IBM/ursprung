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

#ifndef AUDITD_PLUGIN_PLUGIN_PIPELINE_H_
#define AUDITD_PLUGIN_PLUGIN_PIPELINE_H_

#include <thread>
#include <auparse.h>

#include "sync-queue.h"
#include "event.h"
#include "logger.h"
#include "plugin-util.h"
#include "os-model.h"

/**
 * A stage represents a step in the event processing pipeline
 * of the auditd plugin. A stage has an input and an output queue
 * to read from/write to the previous/next stage.
 *
 * The events on the queue are pointers and the last step is
 * responsible for cleaning up the memory.
 *
 * As the queues themselves are also shared between different
 * steps, the main program needs to create and clean up the queues
 * on exit.
 */
class PipelineStep {
private:
  std::shared_ptr<Statistics> stats;
protected:
  /*
   * This program is an audisp plugin, and as such we may receive
   * SIGHUP or SIGTERM as control signals. Only the main() thread should
   * handle these (or any) signals so we mask them in the processing
   * pipeline steps.
   */
  void mask_signals();

  std::thread thread;
  SynchronizedQueue<void*> *in;
  SynchronizedQueue<void*> *out;

public:
  PipelineStep(SynchronizedQueue<void*> *in, SynchronizedQueue<void*> *out,
      std::shared_ptr<Statistics> stats) :
      in { in },
      out { out },
      stats { stats } {}
  virtual ~PipelineStep() {};

  virtual int run() = 0;
  void start() {
    thread = std::thread(&PipelineStep::run, this);
  }
  void join() {
    LOG_DEBUG("Waiting for thread to finish.");
    thread.join();
  }
};

/**
 * This is the first pipeline step, which:
 *
 *  1. Polls audisp
 *  2. Converts audisp events to SyscallEvents
 *  3. Sends those to the next stage
 *
 *  This step does not have an input queue.
 */
class ExtractorStep: public PipelineStep {
private:
  /*
   * The extractor is responsible for reloading the plugin
   * config when SIGHUP has been issued so we store the path
   * as part of the extractor. If the path is not set, SIGHUP
   * will be ignored.
   */
  std::string config_path;
  auparse_state_t *au;

public:
  ExtractorStep(SynchronizedQueue<void*> *in, SynchronizedQueue<void*> *out,
      std::shared_ptr<Statistics> stats) :
      PipelineStep(in, out, stats),
      au { nullptr } {
    assert(!in);
    assert(out);
  }
  virtual ~ExtractorStep() {}

  /* Callback for auparse. */
  static void handle_audisp_event(auparse_state_t *au, auparse_cb_event_t cb_event_type,
      void *user_data);
  virtual int run() override;
  void set_config_path(std::string path) { config_path = path; }
};

/**
 * This stage is the brains of the auditd plugin. It:
 *
 *  1. Receives SyscallEvents
 *  2. Applies them to the OSModel
 *  3. Periodically emits OSEvents to the next stage
 */
class TransformerStep: public PipelineStep {
private:
  OSModel osModel;

public:
  TransformerStep(SynchronizedQueue<void*> *in, SynchronizedQueue<void*> *out,
      std::shared_ptr<Statistics> stats) :
      PipelineStep(in, out, stats),
      osModel { } {
    assert(in);
    assert(out);
  }
  virtual ~TransformerStep() {}

  virtual int run() override;
  void send_ready_events();
};

/*
 * This stage send the transformed records to its destination. It:
 *
 * 1. Receives Events as strings
 * 2. Sends them to the configured output stream
 */
class LoaderStep: public PipelineStep {
private:
  std::string hostname;
  std::unique_ptr<MsgOutputStream> out_stream;

public:
  LoaderStep(SynchronizedQueue<void*> *in, SynchronizedQueue<void*> *out,
      std::shared_ptr<Statistics> stats, std::unique_ptr<MsgOutputStream> out_stream);
  virtual ~LoaderStep() {
    out_stream->close();
  }

  virtual int run() override;
};

#endif /* AUDITD_PLUGIN_PLUGIN_PIPELINE_H_ */
