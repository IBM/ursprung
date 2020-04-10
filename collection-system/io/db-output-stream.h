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

#ifndef IO_DB_OUTPUT_STREAM_H_
#define IO_DB_OUTPUT_STREAM_H_

#include <thread>

#include "msg-output-stream.h"
#include "sync-queue.h"
#include "odbc-wrapper.h"

typedef std::unique_ptr<SynchronizedQueue<std::vector<std::string>>> b_queue_t;

/**
 * Output stream to send (insert) messages to a database via ODBC.
 * Inserts are batched.
 *
 * A DBOutputStream can multiplex incoming messages across different
 * tables. For that purpose, a DBOutputStream can have different
 * target tables (and their respective schemas) and for each
 * target table, the defining attribute value. It also stores
 * the attribute position, at which the defining attribute can
 * be found in the record. This assumes that the defining
 * attribute is at the same position for every record.
 */
class DBOutputStream: public MsgOutputStream {
private:
  /* Multiplexing properties. */
  std::vector<std::string> tablenames;
  std::vector<std::string> db_schemas;
  std::vector<std::string> attr_keys;
  int attr_position = -1;
  const bool multiplex;

  /*
   * Defines how many records will be inserted into the database by one thread
   * in a single INSERT statement. Default value is 1000.
   */
  int batch_size = 1000;
  /* Indicates whether a timestamp should be added to each record before sending. */
  bool add_info = false;
  /* Specifies whether the payload has a header line. */
  bool header = false;
  /* Specifies whether DB insert should be run asynchronously. */
  bool async = false;
  /* Thread for asynchronous insertion into DB. */
  std::thread inserter;
  b_queue_t batch_queue;
  bool running = true;
  dsn_t dsn;

  /*
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
  std::string format_csv_line(const std::string &line);
  std::string get_utc_time();
  /*
   * Takes a list of batches as input and inserts each batch in
   * a separate thread into the DB using the specified table and schema.
   */
  int parallel_send_to_db(const std::vector<std::vector<std::string>> &batches,
      std::string table, std::string schema);
  int send_to_db(const std::vector<std::string> &batch, std::string table,
      std::string schema);
  int send_sync(const std::vector<std::string> &records);
  void send_async(std::vector<std::string> records);

public:
  DBOutputStream(const std::string &dsn, const std::string &username,
      const std::string &passwd, const std::string &dbSchema,
      const std::string &tablename, bool async, bool multiplex = false, int pos = -1);
  ~DBOutputStream();

  virtual int open() override;
  virtual void close() override;
  virtual int send(const std::string &msg_str, int partition,
        const std::string *key = nullptr) override;
  virtual int send_batch(const std::vector<std::string> &msgs) override;
  virtual void flush() const override;

  void set_batch_size(int size) { batch_size = size; };
  void set_header() { header = true; }
  void unset_header() { header = false; }
  void set_add_info() { add_info = true; }
  void unset_add_info() { add_info = false; }
  /*
   * Adds a new multiplex group to the DB output stream. The group defines
   * the value of the key, which indicates that the record should be moved
   * to the target_table using the target_schema.
   *
   * If the stream is not multiplexed (multiplex = false), this function
   * will emit a warning and return.
   */
  void set_multiplex_group(std::string target_table, std::string target_schema, std::string key);
  void run_inserter();
};

#endif /* IO_DB_OUTPUT_STREAM_H_ */
