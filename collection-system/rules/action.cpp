///**
// * Copyright 2020 IBM
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//#include <iostream>
//#include <fstream>
//#include <sstream>
//#include <regex>
//#include <exception>
//#include <ctime>
//#include <iomanip>
//#include <cstdlib>
//
//#include <unistd.h>
//#include <sys/types.h>
//#include <sys/stat.h>
//#include <errno.h>
//
//#include "action.h"
//#include "odbc-wrapper.h"
//#include "logger.h"
////#include "provd-client.h"
//
//// 4KB buffer size to store log file lines (we assume that 4K
//// is enough to hold the longest line in any log file.
//#define MAX_LINE_LENGTH 4096
//
//// regex constants for each rule
//const std::regex DB_LOAD_SYNTAX = std::regex("DBLOAD [a-zA-Z]* INTO "
//    "(.*):(.*)@(.*):[0-9]*/(.*) USING (.*)");
//const std::regex DB_TRANSFER_SYNTAX = std::regex("DBTRANSFER (.*)/[a-zA-Z0-9]* FROMDSN "
//    "[a-zA-Z0-9]* TO (.*):(.*)@(.*):[0-9]*/(.*) USING (.*)");
//const std::regex LOG_LOAD_SYNTAX = std::regex("LOGLOAD [a-zA-Z0-9]* MATCH (.)* FIELDS "
//    "(.)* DELIM (.*) INTO (.*):(.*)@(.*):[0-9]*/(.*) USING (.*)");
//const std::regex TRACK_SYNTAX = std::regex("TRACK (.*) INTO (.*):(.*)@(.*):[0-9]*");
//const std::regex CAPTURESOUT_SYNTAX = std::regex("CAPTURESOUT MATCH (.)* FIELDS "
//    "(.)* DELIM (.*) INTO (.*):(.*)@(.*):[0-9]*/(.*) USING (.*)");
//
//// string constants
//// TODO make configurable
//const std::string REPO_LOCATION="/opt/ibm/metaocean/contenttracking";
//const std::string DATE_FORMAT = "%Y-%m-%d %H:%M:%S";
//
//
///*------------------------------
// * Helper functions
// *------------------------------*/
//
///**
// * Inserts a new record for the state of the specified
// * rule into the database.
// */
//int insertState(OdbcWrapper *dbConnection, std::string ruleID,
//    std::string state, std::string target = "") {
//  // TODO either store prepared query as const or use actual prepared query
//  std::string preparedQuery = "INSERT INTO rulestate (id,target,state) values ('"
//      + ruleID + "','" + target + "','" + state + "')";
//  if (dbConnection->submit_query(preparedQuery) != ODBC_SUCCESS) {
//    LOG_ERROR("Error while inserting new state: state " << state
//        << ", rule " << ruleID << ", target " << target);
//    return ERROR_NO_RETRY;
//  }
//  return NO_ERROR;
//}
//
///**
// * Update the state of the specified rule in the database.
// */
//int updateState(OdbcWrapper *dbConnection, std::string ruleID,
//    std::string state, std::string target = "") {
//  // TODO either store prepared query as const or use actual prepared query
//  std::string preparedQuery = "UPDATE rulestate SET state='" + state
//      + "' WHERE id='" + ruleID + "' AND target='" + target + "'";
//  if (dbConnection->submit_query(preparedQuery) != ODBC_SUCCESS) {
//    LOG_ERROR("Error while updating state: state " << state <<
//        ", rule " << ruleID << ", target " << target);
//    return ERROR_NO_RETRY;
//  }
//  return NO_ERROR;
//}
//
///**
// * Read back the state of an action from the specified
// * rule from the database.
// */
//int lookupState(OdbcWrapper *dbConnection, char *stateBuffer,
//    std::string ruleID, std::string target = "") {
//  // TODO either store prepared query as const or use actual prepared query
//  std::string preparedQuery = "SELECT state FROM rulestate WHERE id='" + ruleID
//      + "'" + " AND target='" + target + "'";
//  if (dbConnection->submit_query(preparedQuery) != ODBC_SUCCESS) {
//    LOG_ERROR("Error while retrieving state from DB: rule " << ruleID
//        << ", target " << target << ". Can't retrieve existing state.");
//    return ERROR_NO_RETRY;
//  }
//
//  // we have existing state
//  odbc_rc odbcRc;
//  std::string row;
//  odbcRc = dbConnection->get_row(stateBuffer);
//  if (odbcRc == ODBC_SUCCESS)
//    return NO_ERROR;
//  else if (odbcRc == ODBC_NO_DATA)
//    return ERROR_EOF;
//  else
//    return ERROR_NO_RETRY;
//}
//
///**
// * Convert a date field by adding the specified time offset.
// *
// * TODO make data format configurable
// * At the moment we're assuming a fixed date format
// * of 'YYYY-mm-dd HH:MM:SS'.
// */
//std::string convertDateField(std::string date, LogLoadField *field) {
//  // convert string to struct tm
//  struct tm tm;
//  strptime(date.c_str(), DATE_FORMAT.c_str(), &tm);
//  // check if we moved to a new day
//  if (tm.tm_hour + field->getTimeoffset() >= 24) {
//    // TODO Make conversion more robust (using, e.g., external library or <ctime> tools)
//    // Currently, this doesn't work for many corner cases, e.g. months breaks, leap years, etc.
//    tm.tm_mday = tm.tm_mday + 1;
//  }
//  tm.tm_hour = (tm.tm_hour + field->getTimeoffset()) % 24;
//  // convert new date back to string
//  char newDate[32];
//  strftime(newDate, sizeof(newDate), "%Y-%m-%d %H:%M:%S", &tm);
//  return std::string(newDate);
//}
//
///**
// * Parse the return code of an hg command.
// */
//int parseHgReturnCode(hg_handle *handle, std::string &out, std::string &err) {
//  hg_header *header;
//  char buff[4096];
//  int n;
//  int exitcode = 0;
//  int exit = 0;
//
//  while (!exit) {
//    header = hg_read_header(handle);
//    switch (header->channel) {
//    case o:
//      while ((n = hg_rawread(handle, buff, 4096)), n > 0) {
//        out = out + std::string(buff);
//      }
//      break;
//    case e:
//      while ((n = hg_rawread(handle, buff, 4096)), n > 0) {
//        err = err + std::string(buff);
//      }
//      break;
//    case I:
//    case L:
//      break;
//    case r:
//      exitcode = hg_exitcode(handle);
//      exit = 1;
//      break;
//    case unknown_channel:
//      break;
//    }
//  }
//
//  return exitcode;
//}
//
///**
// * Extract a record from a line based on a specified
// * delimiter and a set of LogLoadFields.
// */
//std::string extractRecordFromLine(std::string line, std::string delimiter,
//    std::vector<LogLoadField*> fields, std::shared_ptr<IntermediateMessage> msg) {
//  size_t pos = 0;
//  std::stringstream record;
//  std::string token;
//  std::vector<std::string> tokens;
//
//  while ((pos = line.find(delimiter)) != std::string::npos) {
//    token = line.substr(0, pos);
//    tokens.push_back(token);
//    line.erase(0, pos + delimiter.length());
//  }
//  tokens.push_back(line);
//
//  // TODO add range check on fieldIDs and tokens
//  for (unsigned int k = 0; k < fields.size(); k++) {
//    LogLoadField *field = fields.at(k);
//    if (field->isRangeField()) {
//      std::string fieldValue;
//      int parseUntil = field->getUntilFieldID() == -1 ?
//          tokens.size() - 1 : field->getUntilFieldID();
//
//      for (int j = field->getFieldID(); j <= parseUntil; j++) {
//        fieldValue += tokens.at(j) + ((j == parseUntil) ? "" : " ");
//      }
//
//      if (field->isTimestampField()) {
//        record << convertDateField(fieldValue, field);
//      } else {
//        record << fieldValue;
//      }
//      record << ((k == fields.size() - 1) ? "" : ",");
//    } else if (field->isEventField()) {
//      std::string eventFieldVal = msg->get_value(field->getFieldName());
//      record << eventFieldVal << ((k == fields.size() - 1) ? "" : ",");
//    } else if (field->isCompositeField()) {
//      std::vector<int> ids = field->getFieldIds();
//      for (unsigned int j = 0; j < ids.size(); j++) {
//        record << tokens.at(ids.at(j));
//      }
//      record << ",";
//    } else {
//      if (field->isTimestampField()) {
//        record << convertDateField(tokens.at(field->getFieldID()), field)
//            << ((k == fields.size() - 1) ? "" : ",");
//      } else {
//        record << tokens.at(field->getFieldID())
//            << ((k == fields.size() - 1) ? "" : ",");
//      }
//    }
//  }
//  record << std::endl;
//
//  return record.str();
//}
//
///*------------------------------
// * Action
// *------------------------------*/
//
//std::shared_ptr<Action> Action::parseAction(std::string action) {
//  try {
//    if (action.substr(0, 6) == DB_LOAD_RULE) {
//      return std::make_shared<DBLoadAction>(action);
//    } else if (action.substr(0, 10) == DB_TRANSFER_RULE) {
//      return std::make_shared<DBTransferAction>(action);
//    } else if (action.substr(0, 7) == LOG_LOAD_RULE) {
//      return std::make_shared<LogLoadAction>(action);
//    } else if (action.substr(0, 5) == TRACK_RULE) {
//      return std::make_shared<TrackAction>(action);
//    } else if (action.substr(0, 11) == CAPTURESOUT_RULE) {
//      return std::make_shared<StdoutCaptureAction>(action);
//    } else {
//      LOG_WARN("No action matched for provided action " << action);
//      return nullptr;
//    }
//  } catch (const std::invalid_argument &e) {
//    throw;
//  } catch (const DBConnectionException &e) {
//    throw;
//  }
//}
//
//void Action::runConsumer() {
//  while (running) {
//    LOG_DEBUG(ruleID << " - waiting for action to consume");
//    std::shared_ptr<IntermediateMessage> msg = actionQueue->pop();
//    if (msg) {
//      LOG_DEBUG(ruleID << " - Received new message, executing action");
//
//#ifdef PERF
//      std::string val = msg->get_value("eventTime");
//      LOG_DEBUG("Got event time " << val);
//
//      // extract milliseconds from time
//      size_t dotPos = val.find(".", 0);
//      std::string timeNoMillis = val.substr(0, dotPos);
//      int millis = std::stoi(val.substr(dotPos + 1, 3));
//
//      // convert to timestamp
//      std::tm t = { 0 };
//      std::istringstream ss(timeNoMillis);
//      ss >> std::get_time(&t, "%Y-%m-%d %H:%M:%S");
//      time_t timestamp = std::mktime(&t);
//      long timestampMillis = timestamp * 1000 + millis;
//
//      // get current timestamp
//      long ms = std::chrono::duration_cast<std::chrono::milliseconds>(
//          std::chrono::system_clock::now().time_since_epoch()).count();
//      LOG_DEBUG("Got current time " << ms);
//
//      // compute difference and log it
//      long lat = ms - timestampMillis;
//      LOG_PERF("Rulelatency: " << lat);
//#endif
//
//      execute(std::move(msg));
//    }
//  }
//  LOG_INFO(ruleID << " - finished");
//}
//
//void Action::startActionConsumers(int numThreads) {
//  LOG_INFO(ruleID << " - starting action consumer");
//  for (int i = 0; i < numThreads; i++) {
//    consumerThreads.push_back(std::thread(&Action::runConsumer, this));
//  }
//}
//
//void Action::stopActionConsumers() {
//  running = false;
//  // push a nullptr for each active thread to unblock pop()
//  for (unsigned int i = 0; i < consumerThreads.size(); i++) {
//    actionQueue->push(std::unique_ptr<IntermediateMessage>(nullptr));
//  }
//  for (std::thread &t : consumerThreads) {
//    t.join();
//  }
//}
//
//Action::Action() :
//    actionQueue(new SynchronizedQueue<std::shared_ptr<IntermediateMessage>>()),
//    running(true),
//    out() {}
//
//Action::~Action() {
//    if (actionQueue) delete actionQueue;
//    if (out) {
//        out->flush();
//        delete out;
//    }
//}
//
///*------------------------------
// * DBLoadAction
// *------------------------------*/
//
//DBLoadAction::DBLoadAction(std::string action) {
//    if (!std::regex_match(action, DB_LOAD_SYNTAX)) {
//        LOG_ERROR("DBLoadAction " << action << " is not specified correctly.");
//        throw std::invalid_argument(action + " not specified correctly.");
//    }
//
//    size_t intoPos = action.find("INTO", 0);
//    eventField = action.substr(DB_LOAD_RULE.length() + 1, intoPos - (DB_LOAD_RULE.length() + 2));
//
//    size_t usingPos = action.find("USING", 0);
//    std::string connectionString = action.substr(intoPos + 4 + 1, usingPos - (intoPos + 4 + 2));
//    size_t atPos = connectionString.find("@", 0);
//    std::string userPassword = connectionString.substr(0, atPos);
//
//    size_t colonPos = userPassword.find(":");
//    dbUser = userPassword.substr(0, colonPos);
//    dbPassword = userPassword.substr(colonPos + 1, userPassword.length());
//
//    std::string serverTable = connectionString.substr(atPos + 1, connectionString.length());
//    size_t slashPos = serverTable.find("/");
//
//    dbServer = serverTable.substr(0, slashPos);
//    tablename = serverTable.substr(slashPos + 1, serverTable.length());
//
//    size_t headerPos = action.find("HEADER", 0);
//
//    if (headerPos != std::string::npos) {
//        header = true;
//        schema = action.substr(usingPos + 6, (headerPos - 1) - (usingPos + 6));
//    } else {
//        header = false;
//        schema = action.substr(usingPos + 6, action.length() - (usingPos + 6));
//    }
//
//    out = new BatchedDBOutStream(MO_DSN, dbUser, dbPassword, schema, tablename);
//    dynamic_cast<BatchedDBOutStream*>(out)->setAddInfo();
//    if (header) dynamic_cast<BatchedDBOutStream*>(out)->setHeader();
//    out->open();
//}
//
//int DBLoadAction::execute(std::shared_ptr<IntermediateMessage> msg) {
//  LOG_DEBUG("Executing DBLoadAction " << this->str());
//
//  int rc = NO_ERROR;
//  std::ostringstream errstr;
//  std::string value = msg->get_value(eventField);
//  out->set_loadfile(value);
//
//  rc = out->send(errstr);
//  if (rc != NO_ERROR) {
//    LOG_ERROR(
//        "Problems while bulk loading data from " << value << " into DB."
//        << " Provenance may be incomplete. Action: " << this->str());
//  }
//  return rc;
//}
//
//std::string DBLoadAction::str() const {
//  return "DBLOAD " + eventField + " INTO " + dbUser + ":" + dbPassword + "@"
//      + dbServer + "/" + tablename + " USING " + schema
//      + (header ? " HEADER" : "");
//}
//
//std::string DBLoadAction::getType() const {
//  return DB_LOAD_RULE;
//}
//
///*------------------------------
// * DBTransferAction
// *------------------------------*/
//
//DBTransferAction::DBTransferAction(std::string action) {
//    if (!std::regex_match(action, DB_TRANSFER_SYNTAX)) {
//        GLOG_ERROR("DBTransferAction " << action << " is not specified correctly.");
//        // TODO here we should also enforce the requirement that attributes need to be specified
//        // explicitely in the select clause and that the first selected attribute has to be the
//        // state attribute
//        throw std::invalid_argument(action + " not specified correctly.");
//    }
//
//    size_t fromDsnPos = action.find("FROMDSN", 0);
//    std::string queryState = action.substr(DB_TRANSFER_RULE.length() + 1, fromDsnPos - (DB_TRANSFER_RULE.length() + 2));
//    size_t slashPos = queryState.find("/");
//
//    query = queryState.substr(0, slashPos);
//    stateAttributeName = queryState.substr(slashPos + 1, queryState.length());
//
//    size_t toPos = action.find("TO", 0);
//    odbcDSN = action.substr(fromDsnPos + 7 + 1, toPos - (fromDsnPos + 7 + 2));
//
//    size_t usingPos = action.find("USING", 0);
//    std::string connectionString = action.substr(toPos + 2 + 1, usingPos - (toPos + 2 + 2));
//    size_t atPos = connectionString.find("@", 0);
//    std::string userPassword = connectionString.substr(0, atPos);
//
//    size_t colonPos = userPassword.find(":");
//    targetDbUser = userPassword.substr(0, colonPos);
//    targetDbPassword = userPassword.substr(colonPos + 1, userPassword.length());
//
//    std::string serverTable = connectionString.substr(atPos + 1, connectionString.length());
//    slashPos = serverTable.find("/");
//
//    targetDbServer = serverTable.substr(0, slashPos);
//    targetTablename = serverTable.substr(slashPos + 1, serverTable.length());
//
//    targetSchema = action.substr(usingPos + 6, action.length() - (usingPos + 6));
//
//    sourceDB = new OdbcWrapper(odbcDSN, "", "");
//    targetDB = new OdbcWrapper(MO_DSN, targetDbUser, targetDbPassword);
//
//    odbc_rc odbcRc;
//    odbcRc = sourceDB->connect();
//    if (odbcRc != ODBC_SUCCESS) {
//        GLOG_ERROR("Error while connecting to source DB " << odbcDSN);
//        throw DBConnectionException();
//    }
//    odbcRc = targetDB->connect();
//    if (odbcRc != ODBC_SUCCESS) {
//        GLOG_ERROR("Error while connecting to target DB " << MO_DSN);
//        throw DBConnectionException();
//    }
//
//    out = new BatchedDBOutStream(MO_DSN, targetDbUser, targetDbPassword, targetSchema, targetTablename);
//    out->open();
//
//    rlog = new RuleLogger("DBTRANSFER");
//}
//
//DBTransferAction::~DBTransferAction() {
//    // destructor handles disconnection
//    delete sourceDB;
//    delete targetDB;
//}
//
//MoRc DBTransferAction::execute(std::shared_ptr<IntermediateMessage> msg) {
//    GLOG_DEBUG("Executing DBTransferAction " << this->str());
//
//    MoRc rc = ERR_NONE;
//    odbc_rc odbcRc;
//    char rowBuffer[1024];
//    bool firstRow = true;
//    std::string row;
//    std::ostringstream errstr;
//    std::string tmpLoadFile = "/tmp/tmp_dbtransfer_file_" + ruleID;
//    std::string preparedQuery = query;
//
//    // restore any existing state
//    if (queryState.empty()) {
//        // check if there has been any state saved
//        char stateBuffer[1024];
//        rc = lookupState(targetDB, stateBuffer, ruleID);
//        if (rc == ERR_NO_RETRY) {
//            GLOG_ERROR("Problems while trying to restore state for " << this->str() << ". Will work"
//                    << " with existing query state " << queryState);
//        } else {
//            std::string queryStateString = (rc == ERR_NONE) ? std::string(stateBuffer) : "";
//            if (!queryStateString.empty()) {
//                GLOG_INFO("DBTransferAction " << this->str() << ": restored " << queryStateString << " state from disk.");
//                queryState = queryStateString;
//            } else {
//                // initialize state in the database
//                rc = insertState(targetDB, ruleID, queryState);
//                if (rc != ERR_NONE) {
//                    GLOG_ERROR("Problems while adding state for rule " << this->str()
//                            << ". State can't be backed up at the moment.");
//                } else {
//                    GLOG_INFO("DBTransferAction " << this->str() << ": no existing state found");
//                }
//            }
//        }
//    }
//
//    // construct query
//    preparedQuery.append(" where " ).
//            append(stateAttributeName).
//            append(" is not null ");
//    if (!queryState.empty()) {
//        // there is state so we have to add the correct WHERE clause to the query
//        preparedQuery.append(" and ").
//                append(stateAttributeName).
//                append(" > '").
//                append(queryState).
//                append("'");
//    }
//    preparedQuery.append(" order by ").append(stateAttributeName).append(" desc");
//
//    // send query to database
//    odbcRc = sourceDB->submitQuery(preparedQuery);
//    if (odbcRc != ODBC_SUCCESS) {
//        GLOG_ERROR("Error while submitting query to DB. Can't retrieve data from source db."
//                << " Provenance may be incomplete. Action: " << this->str());
//        rc = ERR_NO_RETRY;
//        return rc;
//    }
//
//    // extract rows and write to tmp file
//    ofstream outfile(tmpLoadFile, std::ios_base::trunc);
//    bool streamEmpty = true;
//    while(sourceDB->getRow(rowBuffer) == ODBC_SUCCESS) {
//        row = std::string(rowBuffer);
//        if (firstRow) {
//            std::stringstream ss(row);
//            std::string item;
//            getline(ss, item, ',');
//            // TODO correctly synchronize on queryState once action handling is multi-threaded
//            queryState = item;
//            rc = updateState(targetDB, ruleID, queryState);
//            if (rc != ERR_NONE) {
//                GLOG_ERROR("Problems while updating state for rule " << this->str()
//                        << ". State can't be backed up at the moment.");
//            }
//            firstRow = false;
//        }
//        outfile << row << std::endl;
//        if (streamEmpty) {
//            streamEmpty = false;
//        }
//    }
//    outfile.close();
//
//    // It is possible, that we triggered a DBTransfer action but didn't find
//    // any new data (e.g. when the action is triggered by a WRITE to a DB file
//    // but when the WRITE is received, the data hasn't actually been flushed to
//    // disk). Hence, we check here whether we have any data to send or not.
//    if (!streamEmpty) {
//        out->set_loadfile(tmpLoadFile);
//        rc = out->send(errstr);
//        if (rc != ERR_NONE) {
//            GLOG_ERROR("Problems while adding newly retrieved db data to DB."
//                    << " Provenance may be incomplete. Action: " << this->str());
//        }
//    } else {
//        GLOG_DEBUG("DBTransferAction " << this->str() << " didn't receive any new data.");
//    }
//
//    return rc;
//}
//
//std::string DBTransferAction::str() const {
//    return "DBTRANSFER " + query + " FROM " + odbcDSN + " TO " + targetDbPassword
//            + "@" + targetDbServer + "/" + targetTablename + " USING " + targetSchema;
//}
//
//std::string DBTransferAction::getType() const {
//    return DB_TRANSFER_RULE;
//}
//
///***********************************
// * IMPLEMENTATION OF LogLoadAction *
// ***********************************/
//
//LogLoadAction::LogLoadAction(std::string action) {
//    if (!std::regex_match(action, LOG_LOAD_SYNTAX)) {
//        GLOG_ERROR("LogLoadAction " << action << " is not specified correctly.");
//        throw std::invalid_argument(action + " not specified correctly.");
//    }
//
//    size_t matchPos = action.find("MATCH", 0);
//    eventField = action.substr(LOG_LOAD_RULE.length() + 1, matchPos - (LOG_LOAD_RULE.length() + 2));
//
//    size_t fieldsPos = action.find("FIELDS", matchPos);
//    matchingString = std::regex("(.*)" + action.substr(matchPos + 6, fieldsPos - (matchPos + 5 + 2)) + "(.*)");
//
//    size_t delimPos = action.find("DELIM", fieldsPos);
//    std::string fieldsString = action.substr(fieldsPos + 7, delimPos - (fieldsPos + 6 + 2));
//
//    std::stringstream ss(fieldsString);
//    std::string field;
//    while (getline(ss, field, ',')) {
//      try {
//        fields.push_back(new LogLoadField(field));
//      } catch (std::invalid_argument& e) {
//          GLOG_ERROR("Problems while parsing LogLoad field " << field << ". Reason invalid argument to " << e.what()
//                  << ". Field will not be added to the LogLoad fields.");
//      }
//    }
//
//    size_t intoPos = action.find("INTO", delimPos);
//    delimiter = action.substr(delimPos + 6, intoPos - (delimPos + 5 + 2));
//
//    size_t usingPos = action.find("USING", intoPos);
//    std::string connectionString = action.substr(intoPos + 4 + 1, usingPos - (intoPos + 4 + 2));
//    size_t atPos = connectionString.find("@", 0);
//    std::string userPassword = connectionString.substr(0, atPos);
//
//    size_t colonPos = userPassword.find(":");
//    dbUser = userPassword.substr(0, colonPos);
//    dbPassword = userPassword.substr(colonPos + 1, userPassword.length());
//
//    std::string serverTable = connectionString.substr(atPos + 1, connectionString.length());
//    size_t slashPos = serverTable.find("/");
//
//    dbServer = serverTable.substr(0, slashPos);
//    tablename = serverTable.substr(slashPos + 1, serverTable.length());
//
//    schema = action.substr(usingPos + 6, action.length() - (usingPos + 6));
//
//    targetDB = new OdbcWrapper(MO_DSN, dbUser, dbPassword);
//
//    odbc_rc odbcRc;
//    odbcRc = targetDB->connect();
//    if (odbcRc != ODBC_SUCCESS) {
//        GLOG_ERROR("Error while connecting to target DB " << MO_DSN);
//        throw DBConnectionException();
//    }
//
//    out = new BatchedDBOutStream(MO_DSN, dbUser, dbPassword, schema, tablename);
//    out->open();
//
//    rlog = new RuleLogger("LOGLOAD");
//}
//
//LogLoadAction::~LogLoadAction() {
//    for (unsigned int i = 0; i < fields.size(); i++) {
//        delete fields.at(i);
//    }
//    // destructor handles disconnection
//    delete targetDB;
//}
//
//MoRc LogLoadAction::execute(std::shared_ptr<IntermediateMessage> msg) {
//    GLOG_DEBUG("Executing LogLoadAction " << this->str());
//
//    MoRc rc = ERR_NONE;
//    std::string value;
//    std::ostringstream errstr;
//    std::string tmpLoadFile = "/tmp/tmp_logload_file_" + ruleID;
//    std::pair<long long int, unsigned long long> state;
//
//    msg->get_value(eventField, &value);
//
//    // get inode of file to deal with log rollover
//    struct stat sb;
//    int err;
//    if ((err = stat(value.c_str(), &sb)) < 0) {
//        GLOG_ERROR("stat() failed with " << errno << "(" << err << ")."
//                << " Can't retrieve inode for " << value);
//        GLOG_ERROR("Exiting LogLoadAction " << this->str()
//                << " for received message " << msg->getID());
//        return ERR_NO_RETRY;
//    }
//    unsigned long long inode = sb.st_ino;
//
//    // retrieve existing state
//    if (parsingState.find(value) == parsingState.end()) {
//        // check if we have state stored in the database
//        char stateBuffer[1024];
//        rc = lookupState(targetDB, stateBuffer, ruleID, value);
//        if (rc == ERR_NO_RETRY) {
//            GLOG_ERROR("Problems while trying to restore state for " << this->str() << ". Will start"
//                                << " parsing " << value << " from 0.");
//            state.first = 0;
//            state.second = inode;
//            parsingState[value] = state;
//            // try to add state to DB
//            insertState(targetDB, ruleID, std::to_string(parsingState[value].first) + "," + std::to_string(
//                                    parsingState[value].second), value);
//        } else {
//            std::string existingState = (rc == ERR_NONE) ? std::string(stateBuffer) : "";
//            if (!existingState.empty()) {
//                // we had state for this value in the database
//                // the format of the retrieved state is 'offset,inode'
//                size_t pos = existingState.find(",");
//                state.first = std::stoll(existingState.substr(0, pos));
//                state.second = std::stoll(existingState.substr(pos + 1, std::string::npos));
//                parsingState[value] = state;
//                GLOG_INFO("LogLoadAction " << this->str() << ": restored map state");
//            } else {
//                // we haven't seen this value before so initialize it in the map and the database
//                state.first = 0;
//                state.second = inode;
//                parsingState[value] = state;
//                rc = insertState(targetDB, ruleID, std::to_string(parsingState[value].first) + ","
//                        + std::to_string(parsingState[value].second), value);
//                if (rc != ERR_NONE) {
//                    GLOG_ERROR("Problems while adding state for new rule " << this->str()
//                            << ". State can't be backed up at the moment.");
//                } else {
//                    GLOG_INFO("LogLoadAction " << this->str() << ": no existing state found");
//                }
//            }
//        }
//    }
//
//    // check if inode has changed
//    if (parsingState[value].second != inode) {
//        // if inode has changed due to log rollover (i.e. same path but different inode)
//        // reset the parsing state to store the new inode and start parsing from 0 again
//        parsingState[value].first = 0;
//        parsingState[value].second = inode;
//        // TODO could there still be unread data in the old file?
//    }
//
//
//    // open log file and seek to last parsed position
//    std::ifstream infile(value);
//    infile.seekg(parsingState[value].first);
//
//    // We are reading the file in batches of 4KB and extracting data line by
//    // line. If we're at the end of the file and we have not ended the current
//    // line, we store the partial line in lineFragment. On the next action trigger,
//    // we read until the end of the previously stored partial line and combine it
//    // with the lineFragment to restore the full line.
//    // NOTE: This code is not yet able to combine lines across *read* calls and hence,
//    // we assume that no line exceeds 4KB, i.e. we can always read at least one line break
//    // per read call.
//    char buffer[MAX_LINE_LENGTH];
//    std::ofstream outfile(tmpLoadFile, std::ios_base::trunc);
//    std::string line;
//    bool foundEntries = false;
//    bool read = true;
//
//    while (read) {
//        infile.read(buffer, MAX_LINE_LENGTH);
//        size_t bytesRead = infile.gcount();
//
//        // error handling
//        if (infile.eof()) {
//            read = false;
//        }
//        if (infile.bad()) {
//            GLOG_ERROR("Error while reading, bad stream. LogLoad not reading from " << value);
//            read = false;
//        }
//
//        int lineoffset = 0;
//        for (uint i = 0; i < bytesRead; i++) {
//            if (buffer[i] == '\n') {
//                // read one line
//                int lineLength = i - lineoffset;
//                if (lineOverflow > 0) {
//                    lineLength += lineOverflow;
//                }
//
//                std::vector<char> line(lineLength);
//                if (lineOverflow > 0) {
//                    GLOG_DEBUG("Appending previous line fragment to currently read line.");
//                    for (unsigned j = 0; j < lineFragment.size(); j++) {
//                        line[j] = lineFragment[j];
//                    }
//                    for (unsigned j = lineoffset; j < i; j++) {
//                        line[j - lineoffset + lineOverflow] = buffer[j];
//                    }
//                    lineOverflow = 0;
//                } else {
//                    // insert inserts from [lineoffset, i), i.e. buffer[i] is *excluded*
//                    for (unsigned j = lineoffset; j < i; j++) {
//                        line[j - lineoffset] = buffer[j];
//                    }
//                }
//
//                lineoffset = i + 1;
//                std::string lineStr(line.begin(), line.end());
//                GLOG_DEBUG("Read line '" << lineStr << "'");
//
//                // match the line to find possible records to extract
//                if (std::regex_match(lineStr, matchingString)) {
//                    std::string record = extractRecordFromLine(lineStr, delimiter, fields, msg);
//                    outfile << record;
//                    foundEntries = true;
//                }
//            }
//            if (i == bytesRead - 1 && buffer[i] != '\n') {
//                // we're not ending with a new line
//                // store everything from the last line break in a buffer
//                lineFragment.resize(bytesRead - lineoffset);
//                for (unsigned j = lineoffset; j < bytesRead; j++) {
//                    lineFragment[j - lineoffset] = buffer[j];
//                }
//                lineOverflow = bytesRead - lineoffset;
//                std::string lineFragmentStr(lineFragment.begin(), lineFragment.end());
//                GLOG_DEBUG("Read data doesn't end with new line, storing broken line '" <<
//                        lineFragmentStr << "' with line overflow " << lineOverflow);
//            }
//        }
//
//        parsingState[value].first = parsingState[value].first + bytesRead;
//    }
//    // TODO this needs to be synchronized if we make action handling multi-threaded
//    rc = updateState(targetDB, ruleID, std::to_string(parsingState[value].first) + ","
//            + std::to_string(parsingState[value].second), value);
//    if (rc != ERR_NONE) {
//        GLOG_ERROR("Problems while updating state for rule " << this->str()
//                << ". State can't be backed up at the moment.");
//    }
//
//    outfile.close();
//    infile.close();
//
//    if (foundEntries) {
//        out->set_loadfile(tmpLoadFile);
//        rc = out->send(errstr);
//        if (rc != ERR_NONE) {
//            GLOG_ERROR("Problems while bulk loading data from " << tmpLoadFile << " into DB."
//                    << " Provenance may be incomplete. Action: " << this->str());
//        }
//    }
//
//    return rc;
//}
//
//std::string LogLoadAction::str() const {
//    // convert the fields vector to a string
//    std::stringstream ss;
//    for(size_t i = 0; i < fields.size(); ++i) {
//      if(i != 0) {
//        ss << ",";
//      }
//      ss << fields[i];
//    }
//
//    return "LOGLOAD " + eventField + " MATCH " + " FIELDS " + ss.str() + " DELIM " + delimiter
//            + " INTO " + dbUser + ":" + dbPassword + "@" + dbServer + "/" + tablename + " USING " + schema;
//}
//
//std::string LogLoadAction::getType() const {
//    return LOG_LOAD_RULE;
//}
//
///**********************************
// * IMPLEMENTATION OF LogLoadField *
// **********************************/
//
//LogLoadField::LogLoadField(std::string field) {
//    size_t pos = 0;
//    size_t slashPos = std::string::npos;
//
//    // default values
//    isTimestamp = false;
//    isEventFieldName = false;
//    isRange = false;
//    isComposite = false;
//    timeoffset = 0;
//    untilFieldID = -1;
//    fieldID = -1;
//
//    slashPos = field.find("/");
//    if (slashPos != std::string::npos) {
//        // this field represents a timestamp
//        isTimestamp = true;
//        timeoffset = std::stoi(field.substr(slashPos + 1));
//    }
//
//    try {
//        if ((pos = field.find("-")) != std::string::npos) {
//            // this is a range field
//            fieldID = std::stoi(field.substr(0, (pos - 0)));
//            std::string untilFieldIDStr = field.substr(pos + 1, (slashPos - (pos + 1)));
//            if (untilFieldIDStr == "e") {
//                // -1 in a range field indicates that we parse everything until the end
//                // of the log line
//                untilFieldID = -1;
//            } else {
//                untilFieldID = std::stoi(untilFieldIDStr);
//            }
//            isRange = true;
//        } else if (!field.empty() && !isTimestamp && (pos = field.find("+")) != std::string::npos) {
//            // this is a composite field
//            std::string plus = "+";
//            while ((pos = field.find(plus)) != std::string::npos) {
//                fieldIds.push_back(std::stoi(field.substr(0, pos)));
//                field.erase(0, pos + plus.length());
//            }
//            fieldIds.push_back(std::stoi(field));
//            isComposite = true;
//        } else if (!field.empty() && !isTimestamp && field.find_first_not_of("0123456789") != std::string::npos) {
//            // not a number so we interpret this as an event field name
//            // Note that event field names do currently not support timestamp conversions, i.e.
//            // they can't contain a timeoffset.
//            fieldName = field;
//            isEventFieldName = true;
//        } else {
//            fieldID = std::stoi(field.substr(0, (slashPos - 0)));
//        }
//    } catch (std::invalid_argument& e) {
//        throw;
//    }
//}
//
///*********************************
// * IMPLEMENTATION OF TrackAction *
// *********************************/
//
//TrackAction::TrackAction(std::string action) {
//    if (!std::regex_match(action, TRACK_SYNTAX)) {
//        GLOG_ERROR("TrackAction " << action << " is not specified correctly.");
//        throw std::invalid_argument(action + " not specified correctly.");
//    }
//
//    size_t intoPos = action.find("INTO", 0);
//    pathRegex = std::regex(action.substr(TRACK_RULE.length() + 1, intoPos - (TRACK_RULE.length() + 2)));
//    pathRegexStr = action.substr(TRACK_RULE.length() + 1, intoPos - (TRACK_RULE.length() + 2));
//
//    std::string connectionString = action.substr(intoPos + 4 + 1,
//            action.length() - (intoPos + 4 + 2));
//    size_t atPos = connectionString.find("@", 0);
//    std::string userPassword = connectionString.substr(0, atPos);
//
//    size_t colonPos = userPassword.find(":");
//    dbUser = userPassword.substr(0, colonPos);
//    dbPassword = userPassword.substr(colonPos + 1, userPassword.length());
//    dbServer = connectionString.substr(atPos + 1, connectionString.length());
//
//    // open connection to metaocean DB
//    targetDB = new OdbcWrapper(MO_DSN, dbUser, dbPassword);
//    odbc_rc odbcRc;
//    odbcRc = targetDB->connect();
//    if (odbcRc != ODBC_SUCCESS) {
//        GLOG_ERROR("Error while connecting to target DB " << MO_DSN);
//        throw DBConnectionException();
//    }
//
//    // open connection to repo for tracking content changes
//    repoHandle = hg_open(REPO_LOCATION.c_str(), NULL);
//
//    rlog = new RuleLogger("TRACK");
//}
//
//TrackAction::~TrackAction() {
//    // destructor handles disconnection
//    delete targetDB;
//    hg_close(&repoHandle);
//}
//
//MoRc TrackAction::execute(std::shared_ptr<IntermediateMessage> msg) {
//    GLOG_DEBUG("Executing TrackAction " << this->str());
//
//    MoRc rc = ERR_NONE;
//    int err = 0;
//    std::string src;
//    std::string inode;
//    std::string event;
//
//    msg->get_value("path", &src);
//    msg->get_value("inode", &inode);
//    msg->get_value("event", &event);
//
//    if (event == "RENAME") {
//        if (failedCpState.find(inode) != failedCpState.end()) {
//            // get the new name of the renamed file so we can copy it correctly
//            msg->get_value("dstPath", &src);
//            // clean up the entry in the map
//            failedCpState.erase(inode);
//        } else {
//            // if we have a RENAME event without state, don't process it and just exit
//            return ERR_NONE;
//        }
//    } else if (event == "UNLINK") {
//        if (failedCpState.find(inode) != failedCpState.end()) {
//            // clean up the entry in the map and exit
//            failedCpState.erase(inode);
//        }
//        // if we have an UNLINK event, don't process it and just exit
//        return ERR_NONE;
//    }
//
//#ifdef PERF
//    rlog->inc_delivered(1);
//    rlog->timer_start();
//#endif
//
//    // TODO can we improve this and replace the system() call?
//    std::string cmd = "cp " + src + " " + REPO_LOCATION + "/" + inode;
//
//    // FIXME Without stat'ing the src file, it can happen that there's an error during copying:
//    // "cp: skipping file ‘/path/to/example’, as it was replaced while being copied"
//    // The error indicates a concurrent modification while cp is running (either inode
//    // or device changes during copy) and is most likely due to some NFS behavior as
//    // we are currently mounting GPFS on the metaocean VM through NFS.
//    //
//    // It is unclear whether the stat just delays the cp long enough for NFS to get back
//    // in sync or actually updates the file handle but it seems to help. The exact
//    // reason for this behavior needs to be verified to make this robust.
//    struct stat statbuf_pre;
//    stat(src.c_str(), &statbuf_pre);
//
//    err = system(cmd.c_str());
//    if (err != 0) {
//        GLOG_ERROR("Problems while copying file");
//        // store the state of the failed inode
//        failedCpState[inode] = true;
//        rc = ERR_NO_RETRY;
//#ifdef PERF
//        rlog->inc_delivered(1);
//        rlog->timer_start();
//#endif
//        return rc;
//    }
//
//    std::string stdOut, errOut;
//
//    // run 'hg add .'
//    const char* add[] = { "add", ".", "--cwd", REPO_LOCATION.c_str(), NULL };
//    hg_rawcommand(repoHandle, (char**) add);
//    err = parseHgReturnCode(repoHandle, stdOut, errOut);
//    if (err != 0) {
//        GLOG_ERROR("Problems while running hg add: " << err << " - " << errOut << ": Not tracking current version of " << src);
//        rc = ERR_NO_RETRY;
//#ifdef PERF
//        rlog->inc_delivered(1);
//        rlog->timer_start();
//#endif
//        return rc;
//    }
//
//    // run 'hg commit'
//    stdOut = "";
//    errOut = "";
//    const char* commit[] = { "commit", "-m", "commit", "--cwd", REPO_LOCATION.c_str(), NULL };
//    hg_rawcommand(repoHandle, (char**) commit);
//    err = parseHgReturnCode(repoHandle, stdOut, errOut);
//    if (err != 0) {
//        GLOG_ERROR("Problems while running hg commit: " << err << " - " << errOut << ": Not tracking current version of " << src);
//        rc = ERR_NO_RETRY;
//#ifdef PERF
//        rlog->inc_delivered(1);
//        rlog->timer_start();
//#endif
//        return rc;
//    }
//
//    // get the commit ID from the last commit
//    stdOut = "";
//    errOut = "";
//    const char* identify[] = { "--debug", "identify", "-i", "--cwd", REPO_LOCATION.c_str(), NULL };
//    hg_rawcommand(repoHandle, (char**) identify);
//    err = parseHgReturnCode(repoHandle, stdOut, errOut);
//    if (err != 0) {
//        GLOG_ERROR("Problems while running hg identify: " << err << " - " << errOut << ": Won't add commit record to database for " << src);
//        rc = ERR_NO_RETRY;
//#ifdef PERF
//        rlog->inc_delivered(1);
//        rlog->timer_start();
//#endif
//        return rc;
//    }
//
//    // insert commit record into database
//    odbc_rc odbcRc;
//    std::string clusterName;
//    std::string nodeName;
//    std::string fsName;
//    std::string eventTime;
//    msg->get_value("clusterName", &clusterName);
//    msg->get_value("nodeName", &nodeName);
//    msg->get_value("fsName", &fsName);
//    msg->get_value("eventTime", &eventTime);
//
//    std::ostringstream preparedQuery;
//    preparedQuery << "INSERT INTO versions (clusterName,nodeName,fsName,path,inode,eventTime,commitId) VALUES (";
//    preparedQuery << "'" << clusterName << "',";
//    preparedQuery << "'" << nodeName << "',";
//    preparedQuery << "'" << fsName << "',";
//    preparedQuery << "'" << src << "',";
//    preparedQuery << "'" << inode << "',";
//    preparedQuery << "'" << eventTime << "',";
//    preparedQuery << "'" << stdOut << "')";
//
//    std::string query = preparedQuery.str();
//    odbcRc = targetDB->submitQuery(query);
//    if (odbcRc != ODBC_SUCCESS) {
//        GLOG_ERROR("Error while inserting new version " << stdOut << " for " << src);
//        rc = ERR_NO_RETRY;
//    }
//
//#ifdef PERF
//    rlog->timer_stop();
//    rlog->timer_print_batch();
//#endif
//
//    return rc;
//}
//
//std::string TrackAction::str() const {
//    return "TRACK " + pathRegexStr + " INTO " + dbUser + ":" + dbPassword + "@" + dbServer;
//}
//
//std::string TrackAction::getType() const {
//    return TRACK_RULE;
//}
//
///*****************************************
// * IMPLEMENTATION OF StdoutCaptureAction *
// *****************************************/
//
//StdoutCaptureAction::StdoutCaptureAction(std::string action) {
//    if (!std::regex_match(action, CAPTURESOUT_SYNTAX)) {
//        GLOG_ERROR("StdoutCaptureAction " << action << " is not specified correctly.");
//        throw std::invalid_argument(action + " not specified correctly.");
//    }
//
//    size_t matchPos = action.find("MATCH", 0);
//    size_t fieldsPos = action.find("FIELDS", matchPos);
//    matchingString = action.substr(matchPos + 6, fieldsPos - (matchPos + 5 + 2));
//    matchingRegex = std::regex(matchingString);
//
//    size_t delimPos = action.find("DELIM", fieldsPos);
//    std::string fieldsString = action.substr(fieldsPos + 7, delimPos - (fieldsPos + 6 + 2));
//
//    std::stringstream ss(fieldsString);
//    std::string field;
//    while (getline(ss, field, ',')) {
//      try {
//        fields.push_back(new LogLoadField(field));
//      } catch (std::invalid_argument& e) {
//          GLOG_ERROR("Problems while parsing LogLoad field " << field << ". Reason invalid argument to " << e.what()
//                  << ". Field will not be added to the LogLoad fields.");
//      }
//    }
//
//    size_t intoPos = action.find("INTO", delimPos);
//    delimiter = action.substr(delimPos + 6, intoPos - (delimPos + 5 + 2));
//
//    size_t usingPos = action.find("USING", intoPos);
//    std::string connectionString = action.substr(intoPos + 4 + 1, usingPos - (intoPos + 4 + 2));
//    size_t atPos = connectionString.find("@", 0);
//    std::string userPassword = connectionString.substr(0, atPos);
//
//    size_t colonPos = userPassword.find(":");
//    dbUser = userPassword.substr(0, colonPos);
//    dbPassword = userPassword.substr(colonPos + 1, userPassword.length());
//
//    std::string serverTable = connectionString.substr(atPos + 1, connectionString.length());
//    size_t slashPos = serverTable.find("/");
//
//    dbServer = serverTable.substr(0, slashPos);
//    tablename = serverTable.substr(slashPos + 1, serverTable.length());
//
//    schema = action.substr(usingPos + 6, action.length() - (usingPos + 6));
//
//    out = new BatchedDBOutStream(MO_DSN, dbUser, dbPassword, schema, tablename);
//    out->open();
//
//    rlog = new RuleLogger("CAPTURESOUT");
//}
//
//StdoutCaptureAction::~StdoutCaptureAction() {
//    GLOG_INFO("Deleting stdout capture action.");
//}
//
//MoRc StdoutCaptureAction::execute(std::shared_ptr<IntermediateMessage> msg) {
//    int err = 0;
//    std::string nodeName;
//    std::string pidStr;
//
//    // retrieve name and pid of node and process to trace
//    msg->get_value("nodeName", &nodeName);
//    msg->get_value("pid", &pidStr);
//    GLOG_DEBUG("Executing stdout capture action for " << nodeName << ":" << pidStr);
//
//    // send trace request to provd daemon on target node
//    uint32_t pid = atoi(pidStr.c_str());
//    ProvdClient client;
//    if ((err = client.connectToServer(nodeName)) < 0) {
//        GLOG_ERROR("Couldn't connect to provd server on " << nodeName);
//        return ERR_NO_RETRY;
//    }
//    if ((err = client.submitTraceProcRequest(pid, matchingString)) < 0) {
//        GLOG_ERROR("Couldn't submit trace proc request.");
//        return ERR_NO_RETRY;
//    }
//
//    // receive matching lines from target node until process finishes
//    std::string tmpLoadFile = "/tmp/tmp_logload_file_" + ruleID + "_" + std::to_string(rand());
//    std::ofstream outfile(tmpLoadFile, std::ios_base::trunc);
//    bool foundEntries = false;
//    std::string line;
//    uint32_t lineCounter = 0;
//    while (client.receiveLine(&line) > 0) {
//        GLOG_DEBUG("Received matching line " << line);
//        foundEntries = true;
//        // we found a matching line, extract the information
//        std::string record = extractRecordFromLine(line, delimiter, fields, msg);
//        if (record.size() > 0) {
//            outfile << record;
//            lineCounter++;
//        }
//    }
//    GLOG_DEBUG("StdoutCapture Action done, received " << lineCounter << " lines.");
//
//    // finish up
//    if ((err = client.disconnectFromServer()) < 0) {
//        GLOG_ERROR("Problems while disconnecting from provd server " << err);
//        return ERR_NO_RETRY;
//    }
//    outfile.close();
//    if (foundEntries) {
//        int rc;
//        std::ostringstream errstr;
//        out->set_loadfile(tmpLoadFile);
//        rc = out->send(errstr);
//        if (rc != ERR_NONE) {
//            GLOG_ERROR("Problems while bulk loading data from " << tmpLoadFile
//                    << " into DB." << " Provenance may be incomplete. Action: "
//                    << this->str());
//        }
//    }
//
//    return ERR_NONE;
//}
//
//std::string StdoutCaptureAction::str() const {
//    return "CAPTURESOUT";
//}
//
//std::string StdoutCaptureAction::getType() const {
//    return CAPTURESOUT_RULE;
//}
