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

#ifndef PROV_AUDITD_PROCESSES_H_
#define PROV_AUDITD_PROCESSES_H_

#include <map>
#include <list>
#include <vector>
#include <set>
#include <string>

#include "../os-model/files.h"
#include "../os-model/os-common.h"
#include "auditd-event.h"

class LiveThread;

/**
 * Represents a process that is currently active on the system, i.e.
 * we saw it clone'd or issue a syscall, and we have not yet seen it exit.
 * We route syscalls here to update the process' internal state (e.g.
 * pgid, command line, etc.).
 */
class LiveProcess {
  friend class ProcessEvent;

public:
  osm_pid_t pid_;
  osm_pid_t ppid_;
  // for completeness, this should be a list of <pgid, timestamp> pairs
  osm_pgid_t pgid_;
  // for completeness, this should be a list of <X, timestamp> pairs
  // since you can nest exec calls
  std::string exec_cwd;
  std::vector<std::string> exec_cmd_line;
  std::string start_time_utc_;
  std::string finish_time_utc_;
  std::map<int, FileDescriptor> fds;
  std::map<osm_pid_t, LiveThread*> threads;
  bool is_thread;

  LiveProcess(const LiveProcess *parent, osm_pid_t pid, std::string start_time_utc,
      bool inherit_fds = true);
  /*
   * These are for "prehistoric" processes, those that predate the event stream.
   * We know about a prehistoric process because it either made a syscall or
   * because it was referenced in some syscall (e.g. setpgid, kill).
   */
  LiveProcess(const SyscallEvent *se);
  LiveProcess(osm_pid_t pid);
  ~LiveProcess() {};

  void setpgid(osm_pgid_t pgid);
  void execve(std::string cwd, std::vector<std::string> cmd_line);
  void vfork(std::string start_time_utc, osm_pid_t ppid, osm_pgid_t pgid);
  void exit_group(std::string finish_time_utc);

  ProcessEvent* to_process_event();
};

/**
 * Represents a thread that is currently active on the system, i.e.
 * we saw it clone'd or issue a syscall, and we have not yet seen it exit.
 * We route syscalls here to update its internal state (e.g.
 * pgid, command line, etc.).
 */
class LiveThread: public LiveProcess {
  friend class ProcessEvent;

public:
  /*
   * NB: We cannot tell whether a process is a thread and hence, don't need
   * constructors for prehistoric threads. We can only identify a thread
   * by the arguments to clone.
   */
  LiveThread(LiveProcess *parent, osm_pid_t pid, std::string start_time_utc);
  ~LiveThread() {};

  LiveProcess *parent;
};

/**
 * Represents a process group that is currently active on the system, i.e.
 * we saw someone setpgid to it, and there is at least one LiveProcess
 * that is a member. This class works with IDs, not objects.
 *
 * NB: This class has the same naming convention as LiveProcess
 * but is more of a helper of ProcessTable than an independent entity.
 */
class LiveProcessGroup {
  friend class ProcessGroupEvent;

public:
  std::set<osm_pid_t> current_members;
  std::set<osm_pid_t> former_members;
  osm_pgid_t pgid;
  std::string start_time_utc;
  std::string finish_time_utc;

  LiveProcessGroup(osm_pgid_t pgid, const std::string &start_time_utc);
  ~LiveProcessGroup() {};

  osm_rc_t add_process(osm_pid_t process);
  osm_rc_t remove_process(osm_pid_t process);
  bool has_process(osm_pid_t process) const;
  bool is_empty() const;
  void make_dead(const std::string &time);

  ProcessGroupEvent* to_process_group_event();
};

/**
 * Represents the set of LiveProcess's and their corresponding state (threads, file
 * descriptors, process groups etc.) and tracks those that have died but not
 * yet been reaped.
 */
class ProcessTable {
private:
  typedef std::map<osm_pid_t, LiveProcess*> live_proc_map;
  typedef std::map<osm_pgid_t, LiveProcessGroup*> live_pg_map;

  live_proc_map live_processes;
  /*
   * NB: Unlike a LiveProcess, we cannot create a LiveProcessGroup
   * based on hearsay (e.g. the pgid listed in the syscall record or a
   * setpgid to join a group we don't know about). This is because if there
   * is one unknown member, there may be others, so we could never safely
   * declare the pgroup dead. This situation is entirely possible (e.g. daemon
   * pgroups created before we start). However, we are interested in tracking
   * pgroups created as a result of user jobs started *after* we start. The
   * prehistoric pgroups are not interesting.
   *
   * As a result, we don't attempt to deal with prehistoric process groups.
   * If we need them, we have to take `ps` snapshot(s) when we start
   * to get the initial set of memberships.
   *
   * Zombie process groups are possible if zombie processes are possible.
   * We identify a zpg if we try to add a process to a process group
   * and find that process already present.
   */
  live_pg_map live_process_groups;
  std::vector<ProcessEvent*> dead_processes;
  std::vector<ProcessGroupEvent*> dead_process_groups;
  std::vector<IPCEvent*> finished_ipcs;
  std::vector<SocketEvent*> finished_sockets;
  std::vector<SocketConnectEvent*> finished_socket_connects;
  std::string hostname;

  /*
   * Syscall handlers. When these are called, a corresponding LiveProcess
   * must already exist for the caller.
   */
  void clone(SyscallEvent *se);
  void execve(SyscallEvent *se);
  void setpgid(SyscallEvent *se);
  void exit(SyscallEvent *se);
  void exit_group(SyscallEvent *se);
  void vfork(SyscallEvent *se);
  void pipe(SyscallEvent *se);
  void close(SyscallEvent *se);
  void dup2(SyscallEvent *se);
  void socket(SyscallEvent *se);
  void connect(SyscallEvent *se);
  void bind(SyscallEvent *se);

  /* Clean up process. On return, lp is no longer valid nor present in ProcessTable. */
  void finalize_process(LiveProcess *lp, const std::string &time);
  /* Clean up thread. On return, lt is no longer valid nor present in ProcessTable. */
  void finalize_thread(LiveThread *lt, const std::string &time, bool delete_from_parent);
  /* Clean up process group. On return, lt is no longer valid nor present in ProcessTable. */
  void finalize_process_group(LiveProcessGroup *lpg, const std::string &time);

  LiveProcess* add_caller_if_unseen(const SyscallEvent *se);
  LiveProcess* add_process_if_unseen(osm_pid_t pid);
  LiveProcess* get_live_process(osm_pid_t pid);
  LiveProcessGroup* get_live_process_group(osm_pgid_t pid);
  void register_live_process(LiveProcess *lp);
  void register_live_process_group(LiveProcessGroup *lpg);

  /*
   * If lp's lpg is not prehistoric, add lp to the lpg and return the lpg.
   * If no such lpg exists (i.e. prehistoric), returns nullptr.
   * If this is a new lpg, create it before before calling this method.
   *
   * Warning: This may retire an old LPG, so if you have a pointer to that
   * then you should update it. This case is why we need joinTime.
   */
  LiveProcessGroup* try_to_add_process_to_process_group(const LiveProcess *lp,
      std::string &joinTime);
  /* Add specified process to process group and assumes that process group exists. */
  LiveProcessGroup* add_process_to_process_group(const LiveProcess *lp,
      LiveProcessGroup *lpg, std::string &joinTime);

  /* Deletes the process from live processes and add it to list of dead processes. */
  void remove_process_from_state(LiveProcess *lp);
  /* Deletes the thread from live process. */
  void remove_thread_from_state(LiveThread *lt, bool deleteFromParent);
  /* Deletes the process group from live group and add it to list of dead process groups. */
  void remove_process_group_from_state(LiveProcessGroup *lpg, const std::string &time);

public:
  ProcessTable() {};
  ~ProcessTable();

  ProcessTable(const ProcessTable&) = delete;
  ProcessTable& operator=(const ProcessTable &x) = delete;

  osm_rc_t apply_syscall(SyscallEvent *se);
  /* Return all finished events (caller is responsible to free these events). */
  std::vector<Event*> reap_os_events();
};

#endif /* PROV_AUDITD_PROCESSES_H_ */
