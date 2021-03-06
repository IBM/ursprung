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

#include <regex>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include "action.h"

const std::string DEFAULT_REPO_LOCATION = "/opt/ursprung/contenttracking";
const std::regex TRACK_SYNTAX = std::regex("TRACK (.*) AT (.*) "
    "INTO (FILE (.*)|DB (.*):(.*)@(.*) USING (.*)/(.*))");

/**
 * Parse the return code of an hg command.
 */
int parse_hg_return_code(hg_handle *handle, std::string &out, std::string &err) {
  hg_header *header;
  char buff[4096];
  int n;
  int exitcode = 0;
  int exit = 0;

  while (!exit) {
    header = hg_read_header(handle);
    switch (header->channel) {
    case o:
      while ((n = hg_rawread(handle, buff, 4096)), n > 0) {
        out = out + std::string(buff);
      }
      break;
    case e:
      while ((n = hg_rawread(handle, buff, 4096)), n > 0) {
        err = err + std::string(buff);
      }
      break;
    case I:
    case L:
      break;
    case r:
      exitcode = hg_exitcode(handle);
      exit = 1;
      break;
    case unknown_channel:
      break;
    }
  }

  return exitcode;
}

TrackAction::TrackAction(std::string action) {
  // check that the action has the right syntax
  if (!std::regex_match(action, TRACK_SYNTAX)) {
    LOGGER_LOG_ERROR("TrackAction " << action << " is not specified correctly.");
    throw std::invalid_argument(action + " not specified correctly.");
  }

  // parse action
  size_t at_pos = action.find("AT", 0);
  size_t into_pos = action.find("INTO", 0);
  size_t regex_end;
  if (at_pos == action.npos) {
    repo_path = DEFAULT_REPO_LOCATION;
    regex_end = into_pos;
  } else {
    repo_path = action.substr(at_pos + 3, into_pos - 1 - (at_pos + 3));
    regex_end = at_pos;
  }
  path_regex_str = action.substr(TRACK_RULE.length() + 1, regex_end - (TRACK_RULE.length() + 2));
  path_regex = std::regex(path_regex_str);

  // parse output destination and set up stream
  if (init_output_stream(action, into_pos) != NO_ERROR) {
    throw std::invalid_argument(action + " could not create state.");
  }

  repo_handle = hg_open(repo_path.c_str(), NULL);
  if (!repo_handle) {
    // TODO should we initialize the repo if it doesn't exist?
    LOGGER_LOG_ERROR("Couldn't establish connection to target repo at " << repo_path
        << ". Content tracking will not be available.");
  }
}

TrackAction::~TrackAction() {
  // destructor handles disconnection
  if (repo_handle) {
    hg_close(&repo_handle);
  }
}

int TrackAction::execute(evt_t msg) {
  LOGGER_LOG_DEBUG("Executing TrackAction " << this->str());

  // check if we have a connection to the target repo
  if (!repo_handle) {
    LOGGER_LOG_WARN("Not executing " << this->str() << " as no repo connection established.");
    return ERROR_NO_RETRY;
  }

  std::string src = msg->get_value("path");
  std::string inode = msg->get_value("inode");
  std::string event = msg->get_value("event");

  if (event == "RENAME") {
    if (failed_cp_state.find(inode) != failed_cp_state.end()) {
      // get the new name of the renamed file so we can copy it correctly
      src = msg->get_value("dstPath");
      // clean up the entry in the map
      failed_cp_state.erase(inode);
    } else {
      // if we have a RENAME event without state, don't process it and just exit
      return NO_ERROR;
    }
  } else if (event == "UNLINK") {
    if (failed_cp_state.find(inode) != failed_cp_state.end()) {
      // clean up the entry in the map and exit
      failed_cp_state.erase(inode);
    }
    return NO_ERROR;
  }

  // TODO can we improve this and replace the system() call?
  std::string cmd = "cp " + src + " " + repo_path + "/" + inode;

  // FIXME Without stat'ing the src file, it can happen that there's an error during copying:
  // "cp: skipping file ‘/path/to/example’, as it was replaced while being copied"
  // The error indicates a concurrent modification while cp is running (either inode
  // or device changes during copy) and is most likely due to some NFS behavior as
  // we are currently mounting GPFS on the metaocean VM through NFS.
  //
  // It is unclear whether the stat just delays the cp long enough for NFS to get back
  // in sync or actually updates the file handle but it seems to help. The exact
  // reason for this behavior needs to be verified to make this robust.
  struct stat statbuf_pre;
  stat(src.c_str(), &statbuf_pre);
  if (system(cmd.c_str()) != 0) {
    LOGGER_LOG_ERROR("Problems while copying file: " << strerror(errno));
    // store the state of the failed inode
    failed_cp_state[inode] = true;
    return ERROR_NO_RETRY;
  }

  // run 'hg add .'
  std::string std_out;
  std::string std_err;
  const char *add[] = { "add", ".", "--cwd", repo_path.c_str(), NULL };
  hg_rawcommand(repo_handle, (char**) add);
  if (parse_hg_return_code(repo_handle, std_out, std_err) != 0) {
    LOGGER_LOG_ERROR("Problems while running hg add: " << std_err
        << ": Not tracking current version of " << src);
    return ERROR_NO_RETRY;
  }

  // run 'hg commit'
  std_out = "";
  std_err = "";
  const char *commit[] = { "commit", "-m", "commit", "-u", "ursprung",
      "--cwd", repo_path.c_str(), NULL };
  hg_rawcommand(repo_handle, (char**) commit);
  if (parse_hg_return_code(repo_handle, std_out, std_err) != 0) {
    LOGGER_LOG_ERROR("Problems while running hg commit: " << std_err
        << ": Not tracking current version of " << src);
    return ERROR_NO_RETRY;
  }

  // get the commit ID from the last commit
  std_out = "";
  std_err = "";
  const char *identify[] = { "--debug", "identify", "-i", "--cwd", repo_path.c_str(), NULL };
  hg_rawcommand(repo_handle, (char**) identify);
  if (parse_hg_return_code(repo_handle, std_out, std_err) != 0) {
    LOGGER_LOG_ERROR("Problems while running hg identify: " << std_err
        << ": Won't add commit record to database for " << src);
    return ERROR_NO_RETRY;
  }

  // insert commit record into database
  std::string cluster_name = msg->get_value("cluster_name");
  std::string node_name = msg->get_node_name();
  std::string fs_name = msg->get_value("fs_name");
  std::string event_time = msg->get_value("event_time");
  std::ostringstream record;
  record << "'" << cluster_name << "',";
  record << "'" << node_name << "',";
  record << "'" << fs_name << "',";
  record << "'" << src << "',";
  record << "'" << inode << "',";
  record << "'" << event_time << "',";
  record << "'" << std_out << "')";
  std::vector<std::string> records;
  records.push_back(record.str());
  int rc = out->send_batch(records);
  if (rc != NO_ERROR) {
    LOGGER_LOG_ERROR("Problems while bulk loading data into DB."
        << " Provenance may be incomplete. Action: " << this->str());
  }
  return rc;
}

std::string TrackAction::str() const {
  return "TRACK " + path_regex_str + " AT " + repo_path + " INTO " + out->str();
}

std::string TrackAction::get_type() const {
  return TRACK_RULE;
}
