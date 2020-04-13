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

#ifndef SQL_ODBC_WRAPPER_H_
#define SQL_ODBC_WRAPPER_H_

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <string>
#include <exception>

typedef enum {
  DB_SUCCESS,
  DB_ERROR,
  DB_NO_DATA
} db_rc;


/**
 * This class defines the interface for a database connector
 * to talk to different databases (DB2, Postgres, ODBC-based, etc.)
 * Any connector needs to provide ways of connecting and disconnecting
 * to/from the source database and to submit queries and retrieve
 * results.
 */
class DBConnector {
public:
  virtual ~DBConnector() {};

  virtual db_rc connect() = 0;
  virtual bool is_connected() = 0;
  virtual db_rc disconnect() = 0;
  virtual db_rc submit_query(std::string query) = 0;
  virtual db_rc get_row(char *row_buffer) = 0;
};

/**
 * This class defines a simple wrapper around ODBC to
 * connect to an existing DSN and submit queries to it.
 */
class OdbcConnector: public DBConnector {
public:
  OdbcConnector(std::string dsn, std::string user, std::string pw);
  ~OdbcConnector();

  virtual db_rc connect() override;
  virtual bool is_connected() override;
  virtual db_rc disconnect() override;
  virtual db_rc submit_query(std::string query) override;
  virtual db_rc get_row(char *row_buffer) override;

private:
  SQLHENV env_handle;
  SQLHDBC db_handle;
  SQLHSTMT stmt_handle;
  std::string dsn_name;
  std::string user;
  std::string pw;
};

/**
 * Custom exception for DB connection failures.
 */
class DBConnectionException: public std::exception {
public:
  virtual const char* what() const throw () {
    return "Problems while trying to connect to ODBC database.";
  }
};

#endif /* SQL_ODBC_WRAPPER_H_ */
