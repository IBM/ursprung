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

// TODO remove
using namespace std;

typedef enum {
  ODBC_SUCCESS,
  ODBC_ERROR,
  ODBC_NO_DATA
} odbc_rc;

typedef struct dsn {
  std::string dsn_name;
  std::string username;
  std::string password;
  std::string db_schema;
  std::string tablename;
} dsn_t;

/**
 * This class defines a simple wrapper around ODBC to
 * connect to an existing DSN and submit queries to it.
 */
class OdbcWrapper {
public:
  OdbcWrapper(string dsn, string user, string pw);
  ~OdbcWrapper();

  odbc_rc connect();
  bool is_connected();
  odbc_rc disconnect();
  odbc_rc submit_query(string query);
  odbc_rc get_row(char *row_buffer);

private:
  SQLHENV env_handle;
  SQLHDBC db_handle;
  SQLHSTMT stmt_handle;
  string dsn_name;
  string user;
  string pw;
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
