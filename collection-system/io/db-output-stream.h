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
  bool multiplex = false;

  // TODO make configurable
  int batch_size = 1000;
  /* Indicates whether a timestamp should be added to each record before sending. */
  bool add_info = false;
  /* Specifies whether the payload has a header line. */
  bool header = false;
  /* Specifies whether DB insert should be run asynchronously. */
  bool async = false;
  /* Thread for asynchronous insertion into DB. */
  std::thread inserter;
  SynchronizedQueue<std::vector<std::string>> *batch_queue;
  bool running = true;
  dsn_t dsn;

  std::string format_csv_line(const std::string &line);
  std::string get_utc_time();
  int parallel_send_to_db(const std::vector<std::vector<std::string>> &batches,
      std::string table, std::string schema);
  int send_to_db(const std::vector<std::string> &batch, std::string table,
      std::string schema);
  int send_sync(const std::vector<std::string> &records);
  void send_async(std::vector<std::string> records);

public:
  DBOutputStream(const std::string &dsn, const std::string &username,
      const std::string &passwd, const std::string &dbSchema,
      const std::string &tablename, bool async = false);
  virtual ~DBOutputStream() {};

  virtual int open() override;
  virtual void close() override;
  virtual int send(const std::string &msg_str, int partition,
        const std::string *key = nullptr) override;
  virtual int send_batch(const std::vector<std::string> &msgs) override;
  virtual void flush() const override;

  void set_header() { header = true; }
  void unset_header() { header = false; }
  void set_add_info() { add_info = true; }
  void unset_add_info() { add_info = false; }
  void set_multiplex(int pos) { multiplex = true; attr_position = pos; }
  void unset_multiplex() { multiplex = false; }
  void set_multiplex_group(std::string target_table, std::string target_schema, std::string key);
  void run_inserter();
};

#endif /* IO_DB_OUTPUT_STREAM_H_ */
