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

#ifndef EVENT_H
#define EVENT_H

#include <string>
#include <memory>
#include <vector>

class Event;

typedef int osm_pid_t;
typedef int osm_pgid_t;
typedef std::shared_ptr<Event> evt_t;

// delimiter for serialization/deserialization
const std::string SER_DELIM = ",";

// different event types
enum EventType : int {
  FS_EVENT =              1,
  PROCESS_EVENT =         2,
  PROCESS_GROUP_EVENT =   3,
  SYSCALL_EVENT =         4,
  IPC_EVENT =             5,
  SOCKET_EVENT =          6,
  SOCKET_CONNECT_EVENT =  7,
  TEST_EVENT =            8
};

// names for the different event types
const std::vector<std::string> event_type_to_string = {
    "NA",
    "FSEvent",
    "ProcessEvent",
    "ProcessGroupEvent",
    "SyscallEvent",
    "IPCEvent",
    "SocketEvent",
    "SocketConnectEvent",
    "TestEvent"
};

enum ConsumerSource {
  CS_PROV_GPFS = 0,
  CS_PROV_AUDITD = 1
};

enum ConsumerDestination {
  CD_ODBC = 0,
  CD_FILE = 1
};

/**
 * An event is the unit of communication between provenance
 * sources and consumers. Provenance sources create events,
 * serialize them, and emit them. Consumers receive and
 * deserialize events, execute rules based on the data in
 * the events, and add events to the provenance store.
 */
class Event {
protected:
  std::string node_name;
  std::string send_time;
  /**
   * Converts the specified string into a varchar formatted string
   * for insertion into a DB table.
   */
  std::string format_as_varchar(const std::string &str, int limit = -1) const;

public:
  Event() {}
  virtual ~Event() {}

  static evt_t deserialize_event(const std::string &event);

  virtual std::string serialize() const =0;
  virtual std::string format_for_dst(ConsumerDestination c_dst) const =0;
  /**
   * Get the value for the specified message field.
   * If the field doesn't exist, this function returns
   * an empty string.
   */
  virtual std::string get_value(std::string field) const = 0;
  virtual EventType get_type() const =0;

  void set_node_name(std::string name) { node_name = name; }
  std::string get_node_name() { return node_name; }
  void set_send_time(std::string time) { send_time = time; }
  std::string get_send_time() { return send_time; }
};

/**
 * This event is only needed for testing purposes.
 */
class TestEvent: public Event {
private:
  static const int NUM_FIELDS = 3;
  std::string f1;
  std::string f2;
  std::string f3;

public:
  TestEvent(const std::string &serialized_Event);
  TestEvent(std::string f1, std::string f2, std::string f3);
  ~TestEvent() {}

  virtual std::string serialize() const override;
  virtual std::string format_for_dst(ConsumerDestination c_dst) const override;
  virtual std::string get_value(std::string field) const override;
  virtual EventType get_type() const override {
    return TEST_EVENT;
  }
};

#endif // OS_EVENT_H
