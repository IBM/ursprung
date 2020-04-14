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

#include <cstring>
#include <stdexcept>
#include <chrono>
#include <thread>
#include "db-connector.h"
#include "logger.h"

/*------------------------------
 * OdbcConnector
 *------------------------------*/

const char *SQLSTATUS_NO_DATA = "02000";
const int NUM_RETRIES = 3;
const int SLEEP_TIME = 100;

/**
 * This function iterates through the ODBC diagnostics
 * messages from a specified handle and prints them.
 */
void extract_error(SQLHANDLE handle, SQLSMALLINT type) {
  SQLINTEGER i = 0;
  SQLINTEGER native;
  SQLCHAR state[7];
  SQLCHAR text[256];
  SQLSMALLINT len;
  SQLRETURN ret;

  do {
    ret = SQLGetDiagRec(type, handle, ++i, state, &native, text, sizeof(text), &len);
    if (SQL_SUCCEEDED(ret)) {
      char buffer[2048];
      sprintf(buffer, "%s:%d:%d:%s", state, i, native, text);
      LOG_ERROR("ODBC Error: " << buffer);
    }
  } while (ret == SQL_SUCCESS);
}

/**
 * Constructor. Throws a runtime exception in case of an error
 * during setting up and configuring the environment handle.
 *
 * The constructor takes as arguments the name of an existing DSN
 * and the corresponding user name and password.
 */
OdbcConnector::OdbcConnector(std::string dsn, std::string user, std::string pw) :
    env_handle(), db_handle(), stmt_handle(), dsn_name(dsn), user(user), pw(pw) {
  long rc;

  // allocate Environment handle and register version
  rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env_handle);
  if ((rc != SQL_SUCCESS) && (rc != SQL_SUCCESS_WITH_INFO)) {
    throw std::runtime_error("Couln't create ODBC handle. SQLAllocHandle returned with rc="
        + std::to_string(rc));
  }

  rc = SQLSetEnvAttr(env_handle, SQL_ATTR_ODBC_VERSION, (void*) SQL_OV_ODBC2, 0);
  if ((rc != SQL_SUCCESS) && (rc != SQL_SUCCESS_WITH_INFO)) {
    SQLFreeHandle(SQL_HANDLE_ENV, env_handle);
    throw std::runtime_error("Couln't set attributes on ODBC handle. SQLSetEnvAttr "
        "returned with rc=" + std::to_string(rc));
  }
}

OdbcConnector::~OdbcConnector() {
  this->disconnect();
  SQLFreeHandle(SQL_HANDLE_ENV, env_handle);
}

/**
 * Connect to the DSN.
 */
db_rc OdbcConnector::connect() {
  long rc;
  db_rc err = DB_SUCCESS;

  // allocate connection handle
  rc = SQLAllocHandle(SQL_HANDLE_DBC, env_handle, &db_handle);
  if ((rc != SQL_SUCCESS) && (rc != SQL_SUCCESS_WITH_INFO)) {
    LOG_ERROR("Error allocating db handle, rc=" << rc);
    return DB_ERROR;
  }

  // connect to the data source
  int retries = NUM_RETRIES;
  SQLSetConnectAttr(db_handle, SQL_LOGIN_TIMEOUT, (SQLPOINTER) 5, 0);

  bool connected = false;
  while (retries > 0 && !connected) {
    rc = SQLConnect(db_handle, (SQLCHAR*) dsn_name.c_str(), SQL_NTS,
        (SQLCHAR*) user.c_str(), SQL_NTS, (SQLCHAR*) pw.c_str(), SQL_NTS);

    if ((rc != SQL_SUCCESS) && (rc != SQL_SUCCESS_WITH_INFO)) {
      LOG_WARN("Problems connecting to data source " << dsn_name << ", rc="
          << rc << ". Retrying...");
      retries--;

      if (retries == 0) {
        extract_error(db_handle, SQL_HANDLE_DBC);
        LOG_ERROR("Can't connect to " << dsn_name);
        SQLFreeHandle(SQL_HANDLE_DBC, db_handle);
        err = DB_ERROR;
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME));
      }
    } else {
      if (retries < NUM_RETRIES) {
        LOG_INFO("Connected after " << (NUM_RETRIES - retries) << " retries");
      }
      connected = true;
    }
  }

  return err;
}

/**
 * Test whether the current wrapper has an active connection. Note
 * that the check using SQL_ATTR_CONNECTION_DEAD *only* checks whether
 * the connection was active the last time it has been attempted to
 * be accessed.
 *
 * To perform a check whether the connection is working, the SQL_COPT_SS_CONNECTION_DEAD
 * attribute can be used, however, this only works on Windows and is not available
 * under UNIX systems.
 *
 * See: https://stackoverflow.com/questions/7424298/efficient-way-to-test-odbc-connection
 */
bool OdbcConnector::is_connected() {
  SQLUINTEGER uintval;
  long rc;

  rc = SQLGetConnectAttr(db_handle, SQL_ATTR_CONNECTION_DEAD,
      (SQLPOINTER) &uintval, (SQLINTEGER) sizeof(uintval), NULL);

  if ((rc != SQL_SUCCESS) && (rc != SQL_SUCCESS_WITH_INFO)) {
    LOG_ERROR("Error while checking connection, rc=" << rc);
    extract_error(stmt_handle, SQL_HANDLE_STMT);
    return false;
  }

  // a return value of SQL_CD_FALSE means the connection is working
  // (https://docs.microsoft.com/en-us/sql/relational-databases/native-client-odbc-api/sqlgetconnectattr?view=sql-server-2017)
  if (uintval == SQL_CD_FALSE) {
    return true;
  }
  return false;
}

db_rc OdbcConnector::disconnect() {
  SQLFreeHandle(SQL_HANDLE_STMT, stmt_handle);
  SQLDisconnect(db_handle);
  SQLFreeHandle(SQL_HANDLE_DBC, db_handle);

  return DB_SUCCESS;
}

/**
 * Submit a query, specified by the provided string,
 * to the database. This does not return any results.
 * In case there are any results, those need to be retrieved
 * by calling getRow() separately in a loop.
 */
db_rc OdbcConnector::submit_query(std::string query) {
  long rc;

  SQLFreeHandle(SQL_HANDLE_STMT, stmt_handle);
  rc = SQLAllocHandle(SQL_HANDLE_STMT, db_handle, &stmt_handle);
  if ((rc != SQL_SUCCESS) && (rc != SQL_SUCCESS_WITH_INFO)) {
    LOG_ERROR("Error allocating query handle, rc=" << rc);
    extract_error(db_handle, SQL_HANDLE_DBC);
    return DB_ERROR;
  }

  // Execute query
  rc = SQLExecDirect(stmt_handle, (SQLCHAR*) query.c_str(), SQL_NTS);
  if ((rc != SQL_SUCCESS) && (rc != SQL_SUCCESS_WITH_INFO)) {
    LOG_ERROR("Error during query submission, rc=" << rc);
    extract_error(stmt_handle, SQL_HANDLE_STMT);

    // Check if we're disconnected. As we can only check the connection
    // *after* it has been used, we first submit the query and
    // if it fails, we check whether the connection is still
    // active. If not, we try to reconnect and resubmit the query.
    if (!is_connected()) {
      // we lost the connection for some reason, clean up connection and try to reconnect
      rc = disconnect();
      rc = connect();
      if (rc != DB_SUCCESS) {
        LOG_ERROR("Can't reconnect to DB, won't submit query " << query);
        return DB_ERROR;
      }

      // re-submit query after reconnecting
      rc = SQLAllocHandle(SQL_HANDLE_STMT, db_handle, &stmt_handle);
      if ((rc != SQL_SUCCESS) && (rc != SQL_SUCCESS_WITH_INFO)) {
        LOG_ERROR("Error allocating query handle, rc=" << rc);
        extract_error(db_handle, SQL_HANDLE_DBC);
        return DB_ERROR;
      }

      rc = SQLExecDirect(stmt_handle, (SQLCHAR*) query.c_str(), SQL_NTS);
      if ((rc != SQL_SUCCESS) && (rc != SQL_SUCCESS_WITH_INFO)) {
        LOG_ERROR("Error during query submission, rc=" << rc);
        extract_error(stmt_handle, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt_handle);
        return DB_ERROR;
      }
    } else {
      SQLFreeHandle(SQL_HANDLE_STMT, stmt_handle);
      return DB_ERROR;
    }
  }

  return DB_SUCCESS;
}

/**
 * Retrieve a single row from the result set after submitting a query
 * to the database. The row is copied to the passed buffer.
 *
 * This function will return ODBC_SUCCESS in case there is more
 * data available and ODBC_NO_DATA once all data has been read.
 * If there's an error, ODBC_ERROR is returned.
 */
db_rc OdbcConnector::get_row(char *row_buffer) {
  long rc;
  SQLSMALLINT columns;
  SQLUSMALLINT i;
  char buf[512];
  SQLNumResultCols(stmt_handle, &columns);

  // fetch next row
  rc = SQLFetch(stmt_handle);
  if ((rc != SQL_SUCCESS) && (rc != SQL_SUCCESS_WITH_INFO)) {
    // check if there's no more data left or we actually had an error
    SQLINTEGER native;
    SQLCHAR state[7];
    SQLCHAR text[256];
    SQLSMALLINT len;
    SQLRETURN ret;

    ret = SQLGetDiagRec(SQL_HANDLE_STMT, stmt_handle, 1, state, &native, text,
        sizeof(text), &len);
    if (SQL_SUCCEEDED(ret)) {
      if (!strcmp(SQLSTATUS_NO_DATA, (char*) state)) {
        return DB_NO_DATA;
      }
    }
    extract_error(stmt_handle, SQL_HANDLE_STMT);
    return DB_ERROR;
  }

  int offset = 0;
  for (i = 1; i <= columns; i++) {
    SQLLEN indicator;
    // retrieve column data as a string
    rc = SQLGetData(stmt_handle, i, SQL_C_CHAR, buf, sizeof(buf), &indicator);
    if (SQL_SUCCEEDED(rc)) {
      // Handle null columns
      if (indicator == SQL_NULL_DATA) {
        strcpy(buf, "''");
        indicator = 2;
      }
      strcpy(row_buffer + offset, buf);
      offset += (indicator);
      // don't add comma after last column
      if (i < columns) {
        strcpy(row_buffer + offset, ",");
        offset += 1;
      }
    } else {
      extract_error(stmt_handle, SQL_HANDLE_STMT);
      return DB_ERROR;
    }
  }

  return DB_SUCCESS;
}

/*------------------------------
 * ConnectorFactory
 *------------------------------*/

std::unique_ptr<DBConnector> ConnectorFactory::create_connector(
    std::string connection_string) {
  // TODO error handling if wrongly formatted string is received

  // At the moment, we only support creating OdbcConnectors, expecting
  // a connection string of username:password@dsn
  size_t at_pos = connection_string.find("@", 0);
  std::string user_password = connection_string.substr(0, at_pos);
  size_t colon_pos = user_password.find(":");

  std::string dsn = connection_string.substr(at_pos + 1,
      connection_string.length());
  std::string username = user_password.substr(0, colon_pos);
  std::string password = user_password.substr(colon_pos + 1,
      user_password.length());

  return std::make_unique<OdbcConnector>(dsn, username, password);
}
