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

#include "intermediate-message.h"
#include "msg-output-stream.h"
#include "odbc-wrapper.h"
#include "error.h"
#include "logger.h"
#include "sync-queue.h"

// libhg
extern "C" {
#include "client.h"
#include "utils.h"
}

const std::string DB_LOAD_RULE = "DBLOAD";
const std::string DB_TRANSFER_RULE = "DBTRANSFER";
const std::string LOG_LOAD_RULE = "LOGLOAD";
const std::string TRACK_RULE = "TRACK";
const std::string CAPTURESOUT_RULE = "CAPTURESOUT";

class LogLoadField;

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
  std::string ruleID;
  std::vector<std::thread> consumerThreads;
  a_queue_t *actionQueue;
  MsgOutputStream *out;

  void runConsumer();
  virtual int execute(std::shared_ptr<IntermediateMessage> msg) = 0;

public:
  Action();
  virtual ~Action();
  Action(const Action&) = delete;
  Action& operator=(const Action &x) = delete;

  static std::shared_ptr<Action> parseAction(std::string action);
  void startActionConsumers(int numThreads);
  void stopActionConsumers();
  void setRuleID(std::string rid) { ruleID = rid; }
  a_queue_t* getActionQueue() { return actionQueue; }

  // TODO make this configurable
  virtual int getNumConsumerThreads() const = 0;
  virtual std::string getType() const = 0;
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
  std::string dbServer;
  std::string dbUser;
  std::string dbPassword;
  std::string eventField;
  std::string tablename;
  std::string schema;
  bool header;

public:
  DBLoadAction(std::string action);
  virtual ~DBLoadAction() {};

  std::string getEventField() const { return eventField; }
  std::string getTablename() const { return tablename; }
  std::string getSchema() const { return schema; }
  std::string getDBServer() const { return dbServer; }
  std::string getDBUser() const { return dbUser; }
  std::string getDBPassword() const { return dbPassword; }
  bool hasHeader() const { return header; }

  virtual int execute(std::shared_ptr<IntermediateMessage>) override;
  virtual int getNumConsumerThreads() const override { return 10; }
  virtual std::string getType() const override;
  virtual std::string str() const override;
};

/**
 * A DBTransferAction allows to query an existing database and import
 * the output of that query into a target table. The action is specified
 * as follows:
 *
 * DBTRANSFER query/queryStateAttribute FROMDSN dsn
 *   TO dbUser:dbPassword@dbHost:dbPort/targetTable USING schema
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
  std::string queryState;
  std::string targetDbServer;
  std::string targetDbUser;
  std::string targetDbPassword;
  std::string targetTablename;
  std::string targetSchema;
  std::string query;
  std::string stateAttributeName;
  std::string odbcDSN;
  OdbcWrapper *sourceDB;
  OdbcWrapper *targetDB;

public:
  DBTransferAction(std::string action);
  virtual ~DBTransferAction();

  std::string getTargetDbServer() const { return targetDbServer; }
  std::string getTargetDbUser() const { return targetDbUser; }
  std::string getTargetDbPassword() const { return targetDbPassword; }
  std::string getTargetTablename() const { return targetTablename; }
  std::string getTargetSchema() const { return targetSchema; }
  std::string getQuery() const { return query; }
  std::string getStateAttributeName() const { return stateAttributeName; }
  std::string getOdbcDSN() const { return odbcDSN; }

  virtual int execute(std::shared_ptr<IntermediateMessage>) override;
  virtual int getNumConsumerThreads() const override { return 1; }
  virtual std::string getType() const override;
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
  // A single action can cover several log files (e.g. if the condition is a path regex). We
  // keep the state for each individual file that is watched by this action in this parsing state.
  parse_state_t parsingState;
  std::string dbServer;
  std::string dbUser;
  std::string dbPassword;
  std::string eventField;
  std::string tablename;
  std::string schema;
  std::regex matchingString;
  std::string delimiter;
  std::vector<LogLoadField*> fields;
  OdbcWrapper *targetDB;

  // fields to store internal state to correctly parse broken lines
  std::vector<char> lineFragment;
  int lineOverflow = 0;

public:
  LogLoadAction(std::string action);
  virtual ~LogLoadAction();

  std::string getEventField() const { return eventField; }
  std::string getTablename() const { return tablename; }
  std::string getSchema() const { return schema; }
  std::string getDBServer() const { return dbServer; }
  std::string getDBUser() const { return dbUser; }
  std::string getDBPassword() const { return dbPassword; }
  std::regex getMatchingString() const { return matchingString; }
  std::string getDelimiter() const { return delimiter; }
  std::vector<LogLoadField*> getFields() const { return fields; }

  virtual int execute(std::shared_ptr<IntermediateMessage>) override;
  virtual int getNumConsumerThreads() const override { return 1; }
  virtual std::string getType() const override;
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
  std::string dbServer;
  std::string dbUser;
  std::string dbPassword;
  std::regex pathRegex;
  std::string pathRegexStr;
  OdbcWrapper *targetDB;
  hg_handle *repoHandle;

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
  std::map<std::string, bool> failedCpState;

public:
  TrackAction(std::string action);
  virtual ~TrackAction();

  std::string getDBServer() const { return dbServer; }
  std::string getDBUser() const { return dbUser; }
  std::string getDBPassword() const { return dbPassword; }
  std::regex getPathRegex() const { return pathRegex; }

  virtual int execute(std::shared_ptr<IntermediateMessage>) override;
  virtual std::string str() const override;
  virtual std::string getType() const override;
  virtual int getNumConsumerThreads() const override { return 1; }
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
  std::string dbServer;
  std::string dbUser;
  std::string dbPassword;
  std::string tablename;
  std::string schema;
  std::string matchingString;
  std::regex matchingRegex;
  std::string delimiter;
  std::vector<LogLoadField*> fields;

public:
  StdoutCaptureAction(std::string action);
  virtual ~StdoutCaptureAction();

  std::string getTablename() const { return tablename; }
  std::string getSchema() const { return schema; }
  std::string getDBServer() const { return dbServer; }
  std::string getDBUser() const { return dbUser; }
  std::string getDBPassword() const { return dbPassword; }
  std::string getMatchingString() const { return matchingString; }
  std::regex getMatchingRegex() const { return matchingRegex; }
  std::string getDelimiter() const { return delimiter; }
  std::vector<LogLoadField*> getFields() const { return fields; }

  virtual int execute(std::shared_ptr<IntermediateMessage>) override;
  virtual int getNumConsumerThreads() const override { return 1000; }
  virtual std::string getType() const override;
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
  int fieldID;
  int untilFieldID;
  int timeoffset;
  bool isRange;
  bool isEventFieldName;
  bool isTimestamp;
  bool isComposite;
  std::string fieldName;
  std::vector<int> fieldIds;

public:
  LogLoadField(std::string field);

  bool isRangeField() { return isRange; }
  bool isEventField() { return isEventFieldName; }
  bool isTimestampField() { return isTimestamp; }
  bool isCompositeField() { return isComposite; }
  int getFieldID() { return fieldID; }
  int getUntilFieldID() { return untilFieldID; }
  int getTimeoffset() { return timeoffset; }
  std::string getFieldName() { return fieldName; }
  std::vector<int> getFieldIds() { return fieldIds; }
};

#endif /* RULES_ACTION_H_ */
