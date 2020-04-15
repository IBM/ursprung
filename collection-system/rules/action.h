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

#ifndef RULES_ACTION_H_
#define RULES_ACTION_H_

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <exception>
#include <thread>
#include <regex>

#include "intermediate-message.h"
#include "msg-output-stream.h"
#include "db-connector.h"
#include "error.h"
#include "logger.h"
#include "sync-queue.h"
#include "action-state.h"

// libhg
extern "C" {
#include "client.h"
#include "utils.h"
}

// string constants
const std::string DATE_FORMAT = "%Y-%m-%d %H:%M:%S";
const std::string DB_LOAD_RULE = "DBLOAD";
const std::string DB_TRANSFER_RULE = "DBTRANSFER";
const std::string LOG_LOAD_RULE = "LOGLOAD";
const std::string TRACK_RULE = "TRACK";
const std::string CAPTURESOUT_RULE = "CAPTURESOUT";
// possible destinations for provenance collected by actions
const std::string DB_DST = "DB";
const std::string FILE_DST = "FILE";

class LogLoadField;

// helper functions
std::string convert_date_field(std::string date, LogLoadField *field);

typedef SynchronizedQueue<std::shared_ptr<IntermediateMessage>> a_queue_t;
typedef std::map<std::string, std::pair<long long int, unsigned long long>> parse_state_t;

/**
 * This is the base class for all actions. An action has an execute() method
 * which runs the specific action. The base class also provides a parseAction()
 * method which takes a string and returns the correct action object. When
 * implementing new actions, parseAction() needs to be updated.
 *
 * NB: Action's can't be copy constructed or assigned.
 */
class Action {
protected:
  bool running;
  std::string rule_id;
  std::vector<std::thread> consumer_threads;
  a_queue_t *action_queue;
  MsgOutputStream *out;
  std::string out_dest;
  std::unique_ptr<ActionStateBackend> state_backend;

  void run_consumer();
  /*
   * Takes the 'INTO' part of an action definition and parses it
   * to create the correct output stream. The 'from' parameter
   * specifies the index in the 'dst' string at which the 'INTO'
   * part starts.
   */
  int init_output_stream(std::string dst, size_t from);
  /*
   * Takes the 'INTO' part of an action definition and parses it
   * to create the correct state backend. The 'from' parameter
   * specifies the index in the 'dst' string at which the 'INTO'
   * part starts.
   */
  int init_state(std::string dst, size_t from);
  virtual int execute(std::shared_ptr<IntermediateMessage> msg) = 0;

public:
  Action();
  virtual ~Action();
  Action(const Action&) = delete;
  Action& operator=(const Action &x) = delete;

  static std::shared_ptr<Action> parse_action(std::string action);
  void start_action_consumers(int numThreads);
  void stop_action_consumers();
  void set_rule_iD(std::string rid) { rule_id = rid; }
  a_queue_t* get_action_queue() { return action_queue; }

  // TODO make this configurable
  virtual int get_num_consumer_threads() const = 0;
  virtual std::string get_type() const = 0;
  virtual std::string str() const = 0;
};

/**
 * A DBLoadAction loads data from a .csv file into a target table.
 * The action is specified as follows:
 *
 * DBLOAD eventfield INTO dbUser:dbPassword@dbHost:dbPort/targetTable USING schema
 *
 * eventfield specifies, which field from the event contains the path to
 * the .csv file that should be loaded. The database connection string contains
 * all necessary information to connect to the target database and also defines
 * the target table. The schema should be a comma-separated string of the columns
 * in the target table into which the .csv file should be inserted.
 */
class DBLoadAction: public Action {
private:
  std::string event_field;

public:
  DBLoadAction(std::string action);
  virtual ~DBLoadAction() {};

  std::string get_event_field() const { return event_field; }

  virtual int execute(std::shared_ptr<IntermediateMessage>) override;
  virtual int get_num_consumer_threads() const override { return 10; }
  virtual std::string get_type() const override;
  virtual std::string str() const override;
};

/**
 * A DBTransferAction allows to query an existing database and import
 * the output of that query into a target table. The action is specified
 * as follows:
 *
 * DBTRANSFER query/queryStateAttribute FROM dbconnection
 *   INTO dbUser:dbPassword@dbHost:dbPort/targetTable USING schema
 *
 * The query defines the query that should be run on the source table which is
 * accessed via ODBC and the specified DSN. The query is expected to explicitly
 * list all selected attributes (SELECT * is not allowed) and additionally, list
 * the queryStateAttribute as the first selected attribute.
 *
 * The queryStateAttribute is important. It specifies, which column in the source schema
 * represents the action state. The action state is needed to remember, which was the
 * last row in the source database that has already been inserted so that in case the
 * event is triggered several times, it does not query and import already loaded data.
 *
 * This requires the queryStateAttribute to be monotonically increasing for each
 * inserted database row. Attributes that fulfill this requirement are, e.g., a
 * timestamp or an automatically incremented id.
 */
class DBTransferAction: public Action {
private:
  std::string query_state;
  std::string query;
  std::string state_attribute_name;
  std::string connection_string;
  std::unique_ptr<DBConnector> source_db_wrapper;

public:
  DBTransferAction(std::string action);
  virtual ~DBTransferAction() {};

  std::string get_query() const { return query; }
  std::string get_state_attribute_name() const { return state_attribute_name; }
  std::string get_connection_string() const { return connection_string; }

  virtual int execute(std::shared_ptr<IntermediateMessage>) override;
  virtual int get_num_consumer_threads() const override { return 1; }
  virtual std::string get_type() const override;
  virtual std::string str() const override;
};

/**
 * A LogLoadAction allows to parse log files after new data has been
 * added and transform and load matching data into a database. The action
 * is specified as follows:
 *
 * LOGLOAD eventfield MATCH phrase FIELDS 0,1,2 DELIM delimiter
 *   INTO dbUser:dbPassword@dbHost:dbPort/targetTable USING schema
 *
 * The eventfield specifies, which field in the received event contains
 * the pathinformation to the log file. The matching phrase is a regex which
 * is searched for in the new part of the logfile and when a matching line
 * is found, the data to load is extract.
 *
 * Data extraction uses the specified delimiter as a field delimiter for
 * the log entries, and the fields (a comma-separated list of numbers) specifies
 * which fields of the log entries should be loaded. The database connection
 * string contains all connection information and the target table for
 * the target database and the schema specifies, into which columns in the target
 * table the transformed log data should be loaded.
 */
class LogLoadAction: public Action {
private:
  /*
   * A single action can cover several log files (e.g. if the condition is a path regex). We
   * keep the state for each individual file that is watched by this action in this parsing state.
   */
  parse_state_t parsing_state;
  std::string event_field;
  std::string matching_string_str;
  std::regex matching_string;
  std::string delimiter;
  std::vector<LogLoadField*> fields;
  /* Fields to store internal state to correctly parse broken lines. */
  std::vector<char> line_fragment;
  int line_overflow = 0;

public:
  LogLoadAction(std::string action);
  virtual ~LogLoadAction();

  std::string get_event_field() const { return event_field; }
  std::string get_matching_string() const { return matching_string_str; }
  std::string get_delimiter() const { return delimiter; }
  std::vector<LogLoadField*> get_fields() const { return fields; }

  virtual int execute(std::shared_ptr<IntermediateMessage>) override;
  virtual int get_num_consumer_threads() const override { return 1; }
  virtual std::string get_type() const override;
  virtual std::string str() const override;
};

/**
 * A Track Action allows to track content changes for a file. The action
 * is specified as follows:
 *
 * TRACK pathregex INTO dbUser:dbPassword@dbHost:dbPort
 *
 * The pathregex specifies the file(s) that should be tracked.
 * The rules also requires the db connection string to associate
 * the update content events with the correct entries in the
 * database.
 *
 * TODO How to deal with renames of tracked files?
 */
class TrackAction: public Action {
private:
  std::regex path_regex;
  std::string path_regex_str;
  std::string repo_path;
  hg_handle *repo_handle;
  /*
   * This map stores the inode for a CLOSE WRITE event, for
   * which the copy to the hg repository has failed. If the
   * copy fails, there's two main reasons:
   * 1. The file has been deleted before the event has arrived to trigger the copy
   * 2. The file has been renamed before the event has arrived to trigger the copy
   * In the first, case, there's nothing we can do to recover the
   * content for tracking. However, in the second case, we can copy
   * the renamed version of the file to store its content.
   */
  std::map<std::string, bool> failed_cp_state;

public:
  TrackAction(std::string action);
  virtual ~TrackAction();

  std::string get_path_regex() const { return path_regex_str; }
  std::string get_repo_path() const { return repo_path; }
  virtual int execute(std::shared_ptr<IntermediateMessage>) override;
  virtual std::string str() const override;
  virtual std::string get_type() const override;
  virtual int get_num_consumer_threads() const override { return 1; }
};

/**
 * A Stdout Capture Action allows to hijack the stdout of a certain process (e.g.
 * matching a certain command line) and extract provenance records from
 * the hijacked stream.
 *
 * CAPTURESOUT MATCH phrase FIELDS 0,1,2 DELIM delimiter
 *   INTO dbUser:dbPassword@dbHost:dbPort/targetTable USING schema
 *
 * The matching phrase is a regex which is searched for in process' stdout
 * and when a matching line is found, the data to load is extracted.
 *
 * Note that this rule is stateless, i.e. if the consumer crashes or is killed,
 * it will not attempt to attach to any previously tracked process again as that
 * process might have changed in the meantime.
 */
class StdoutCaptureAction: public Action {
private:
  std::string matching_string;
  std::regex matching_regex;
  std::string delimiter;
  std::vector<LogLoadField*> fields;

public:
  StdoutCaptureAction(std::string action);
  virtual ~StdoutCaptureAction() {};

  std::string get_matching_string() const { return matching_string; }
  std::regex get_matching_regex() const { return matching_regex; }
  std::string get_delimiter() const { return delimiter; }
  std::vector<LogLoadField*> get_fields() const { return fields; }

  virtual int execute(std::shared_ptr<IntermediateMessage>) override;
  virtual int get_num_consumer_threads() const override { return 1000; }
  virtual std::string get_type() const override;
  virtual std::string str() const override;
};

/**
 * Represents a field from a LOGLOAD action. There are three types
 * of fields:
 *
 * 1. Field identifiers (numbers) which are taken from the log file
 * 2. Ranges of field identifiers, separated by a '-' to combine several
 *    fields from the log file into one attribute to load
 * 3. Field names, which indicates that attributes from the trigger event
 *    should also be imported into the DB.
 */
class LogLoadField {
private:
  int field_id;
  int until_field_id;
  int timeoffset;
  bool is_range;
  bool is_event_field_name;
  bool is_timestamp;
  bool is_composite;
  std::string field_name;
  std::vector<int> field_ids;

public:
  LogLoadField(std::string field);

  bool is_range_field() { return is_range; }
  bool is_event_field() { return is_event_field_name; }
  bool is_timestamp_field() { return is_timestamp; }
  bool is_composite_field() { return is_composite; }
  int get_field_id() { return field_id; }
  int get_until_field_id() { return until_field_id; }
  int get_timeoffset() { return timeoffset; }
  std::string get_field_name() { return field_name; }
  std::vector<int> get_field_ids() { return field_ids; }
  std::string str();
};

#endif /* RULES_ACTION_H_ */
