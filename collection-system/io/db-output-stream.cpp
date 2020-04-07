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

#include <sstream>
#include <unordered_map>

#include "db-output-stream.h"
#include "error.h"
#include "logger.h"

DBOutputStream::DBOutputStream(const std::string &dsn,
    const std::string &username, const std::string &passwd,
    const std::string &db_schema, const std::string &tablename, bool async) :
    dsn { dsn, username, passwd, db_schema, tablename } {
  if (async) {
    batch_queue = new SynchronizedQueue<std::vector<std::string>>();
    inserter = std::thread(&DBOutputStream::run_inserter, this);
  }
}

DBOutputStream::~DBOutputStream() {
  if (async) {
    running = false;
    // push an empty batch for the inserter thread to unblock pop()
    std::vector<std::string> empty_batch;
    batch_queue->push(empty_batch);
    inserter.join();
    delete batch_queue;
  }
}

void DBOutputStream::run_inserter() {
  LOG_INFO("Running auditd inserter thread");
  std::vector<std::string> batch;
  while (running) {
    batch = batch_queue->pop();
    if (!batch.empty()) {
      send_sync(batch);
    }
  }
  LOG_INFO("Inserter thread exiting.");
}

int DBOutputStream::open() {
  // nothing to do for DBOutputStream
  return NO_ERROR;
}

void DBOutputStream::close() {
  // nothing to do for DBOutputStream
}

void DBOutputStream::flush() const {
  // nothing to do for DBOutputStream
}

int DBOutputStream::send(const std::string &msg_str, int partition, const std::string *key) {
  LOG_WARN("Call to not implemented DBOutputStream::send.");
  return NO_ERROR;
}

int DBOutputStream::send_batch(const std::vector<std::string> &records) {
  if (async) {
    send_async(records);
    return NO_ERROR;
  } else {
    return send_sync(records);
  }
}

/**
 * Adds a new multiplex group to the DB output stream. The group defines
 * the value of the key, which indicates that the record should be moved
 * to the target_table using the target_schema.
 */
void DBOutputStream::set_multiplex_group(std::string target_table, std::string target_schema, std::string key) {
  tablenames.push_back(target_table);
  db_schemas.push_back(target_schema);
  attr_keys.push_back(key);
}

void DBOutputStream::send_async(std::vector<std::string> records) {
  // TODO what happens if we also pass a const& here, is it going to be copied?
  batch_queue->push(records);
}

/**
 * Inserts the current payload into the database. This method will
 * read all the content first and then split it into the three
 * different auditd event types. It will insert each subset
 * separately.
 */
int DBOutputStream::send_sync(const std::vector<std::string> &records) {
  std::unordered_map<std::string, std::vector<std::vector<std::string>>> payloadContents;
  std::unordered_map<std::string, std::vector<std::string>> tmp;
  std::string recordType;
  size_t pos;

  // initialize payloadContents
  for (std::string value : attr_keys) {
    std::vector<std::vector<std::string>> vec;
    payloadContents[value] = vec;
  }

  // split records in batches of batchSize for each table
  if (multiplex) {
    for (std::string record : records) {
      // extract the record type, stored in the first entry of the CSV record
      pos = record.find(",", 0);
      recordType = record.substr(0, pos);
      record.replace(0, pos + 1, "");
      if (tmp.find(recordType) != tmp.end()) {
        // TODO can we optimize this to omit a call to format_csv_line if record comes from consumer
        tmp[recordType].push_back(format_csv_line(record));
        if (tmp[recordType].size() % batch_size == 0) {
          payloadContents[recordType].push_back(tmp[recordType]);
          tmp[recordType].clear();
        }
      } else {
        std::vector<std::string> vec;
        vec.push_back(record);
        tmp[recordType] = vec;
      }
    }
  } else {
    // if we're not multiplexing across target tables, we only have a single record type
    assert(attr_keys.size() == 1);
    std::string record_type = attr_keys.at(0);
    std::vector<std::string> vec;
    tmp[record_type] = vec;

    for (std::string record : records) {
      tmp[record_type].push_back(format_csv_line(record));
      if (tmp[record_type] % batch_size == 0) {
        payloadContents[recordType].push_back(tmp[recordType]);
        tmp[recordType].clear();
      }
    }
  }

  // add any unfinished batches for sending
  for (std::string value : attr_keys) {
    if (tmp[value].size() > 0) {
      payloadContents[value].push_back(tmp[value]);
      tmp[value].clear();
    }
  }

  // send batches for each table to DB
  int rc = NO_ERROR;
  for (unsigned int k = 0; k < attr_keys.size(); k++) {
    rc = parallel_send_to_db(payloadContents[attr_keys[k]], tablenames[k], db_schemas[k]);
    if (rc != NO_ERROR) {
      LOG_ERROR("Problems when sending auditd events for " << attr_keys[k]);
    }
  }
  return rc;
}

/**
 * Takes a list of batches as input and inserts each batch in
 * a separate thread into the DB using the specified table and schema.
 */
int DBOutputStream::parallel_send_to_db(const std::vector<std::vector<std::string>> &batches,
    std::string table, std::string schema) {
  std::vector<std::thread> insertThreads;
  for (unsigned int i = 0; i < batches.size(); i++) {
    LOG_DEBUG("Sending stream of size " << batches[i].size() << " to DB for " << tablename);
    insertThreads.push_back(std::thread(&DBOutputStream::send_to_db,
        this, std::ref(batches[i]), table, schema));
    // TODO correct error handling using promises and futures
  }

  // wait for all threads to complete the insert
  for (unsigned int i = 0; i < insertThreads.size(); i++) {
    insertThreads[i].join();
  }

  return NO_ERROR;
}

/**
 * Inserts the specified batch in the specified table
 * using the specified schema.
 *
 * NB: We are currently establishing a new connection every time send_to_db is called.
 * This is so we're not getting into trouble when different threads are calling this
 * method. We can optimize this by, e.g., using a thread pool with existing persistent
 * connections.
 */
int DBOutputStream::send_to_db(const std::vector<std::string> &batch,
    std::string table, std::string schema) {
  int rc = NO_ERROR;
  odbc_rc err;

  // prepare the query
  std::string query;
  query.append("INSERT INTO ").append(table).append(" (").append(
      schema).append(")").append(" VALUES ");
  for (unsigned int j = 0; j < batch.size(); j++) {
    query.append("(").append(batch[j]).append(")");
    query.append(j == batch.size() - 1 ? "" : ",");
  }

  // connect to the database and submit the query
  OdbcWrapper odbc_wrapper(dsn.dsn_name, dsn.username, dsn.password);
  err = odbc_wrapper.connect();
  if (err != ODBC_SUCCESS) {
    LOG_ERROR("Error while connecting to target DB " << dsn.dsn_name);
    throw DBConnectionException();
  }
  err = odbc_wrapper.submit_query(query);
  if (err != ODBC_SUCCESS) {
    LOG_ERROR("Problems when submitting query " << query << " to database: " << err);
    rc = ERROR_NO_RETRY;
  }
  odbc_wrapper.disconnect();

  return rc;
}

/**
 * Takes a CSV string as input and returns a newly formatted string, ready
 * for insertion into a database. The returned string has the following
 * properties:
 *
 * - All CSV entries are in single quotes
 * - If an entry is in double quotes, double quotes are replaced by single quotes
 * - Single quotes inside an entry are escaped by a double single quote
 * - Empty or NA entries are replaced with NULL
 * - NULL entries are not in single quotes
 */
std::string DBOutputStream::format_csv_line(const std::string &line) {
  std::size_t pos;
  std::string processed_line;
  int i = 0;
  bool processing = true;

  if (add_info) {
    processed_line.append("'").append(get_utc_time()).append("',");
  }

  while (processing) {
    // detect whether we've encountered a (single or double) quoted
    // entry or the entry is not quoted
    std::string entry_split = ",";
    if (line[i] == '\"') {
      entry_split = "\",";
    } else if (line[i] == '\'') {
      entry_split = "\',";
    }
    int skip = entry_split.length() - 1;

    // find the next quote + delimiter combination, which marks the
    // end of the entry
    pos = line.find(entry_split, i);

    // determine the end of the entry based on whether this is the last
    // entry or not
    size_t to = pos != std::string::npos ? pos - (i + skip) : (line.length() - skip) - (i + skip);
    std::string entry = line.substr(i + skip, to);

    // check if entry should be NULL
    if (entry == "NA" || entry == "") {
      processed_line.append("NULL");
    } else {
      // if not, escape all single quotes and add escaped entry in single quotes
      size_t quote_pos = 0;
      while (std::string::npos != (quote_pos = entry.find("'", quote_pos))) {
        entry.replace(quote_pos, 1, "\'\'", 2);
        quote_pos += 2;
      }
      processed_line.append("'").append(entry).append("'");
    }

    // update position in original string or stop if we're done
    if (pos != std::string::npos) {
      i = pos + entry_split.length();
      processed_line.append(",");
    } else {
      processing = false;
    }
  }

  return processed_line;
}

/**
 * Get the current UTC time, formatted as '%Y-%m-%d %H:%M:%S.sss'.
 */
std::string DBOutputStream::get_utc_time() {
  // get current UTC time
  auto now = std::chrono::system_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch()) % 1000;
  auto timer = std::chrono::system_clock::to_time_t(now);
  std::tm utc_bt = *std::gmtime(&timer);
  std::ostringstream utc;
  utc << std::put_time(&utc_bt, "%Y-%m-%d %H:%M:%S");
  utc << '.' << std::setfill('0') << std::setw(3) << ms.count();

  return utc.str();
}
