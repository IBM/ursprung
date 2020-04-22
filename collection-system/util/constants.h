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

#ifndef UTIL_CONSTANTS_H_
#define UTIL_CONSTANTS_H_

#include <string>

namespace constants {
// constants describing the different tables for Scale events
const std::string SCALE_EVENTS_TABLENAME = "gpfseventsnarrow";
const std::string SCALE_EVENTS_SCHEMA = "event,clusterName,nodeName,"
    "fsName,path,inode,bytesRead,bytesWritten,pid,eventTime,dstPath,version";

// constants describing the different tables for Auditd events
const std::string AUDIT_SYSCALL_EVENTS_TABLENAME = "auditdsyscallevents";
const std::string AUDIT_SYSCALL_EVENTS_SCHEMA = "nodeName,eventId,pid,ppid,"
    "uid,gid,euid,egid,syscall,arg0,arg1,arg2,arg3,arg4,rc,eventTime,data1,data2";
const std::string AUDIT_SYSCALL_EVENTS_KEY = "SyscallEvent";

const std::string AUDIT_PROCESS_EVENTS_TABLENAME = "auditdprocessevents";
const std::string AUDIT_PROCESS_EVENTS_SCHEMA = "nodeName,pid,ppid,pgid,"
    "execCwd,execCmdLine,birthTime,deathTime";
const std::string AUDIT_PROCESS_EVENTS_KEY = "ProcessEvent";

const std::string AUDIT_PROCESSGROUP_EVENTS_TABLENAME = "auditdprocessgroupevents";
const std::string AUDIT_PROCESSGROUP_EVENTS_SCHEMA = "nodeName,pgid,birthTime,deathTime";
const std::string AUDIT_PROCESSGROUP_EVENTS_KEY = "ProcessGroupEvent";

const std::string AUDIT_IPC_EVENTS_TABLENAME = "auditdipcevents";
const std::string AUDIT_IPC_EVENTS_SCHEMA = "nodeName,srcPid,dstPid,srcBirth,dstBirth";
const std::string AUDIT_IPC_EVENTS_KEY = "IPCEvent";

const std::string AUDIT_SOCKET_EVENTS_TABLENAME = "auditdsocketevents";
const std::string AUDIT_SOCKET_EVENTS_SCHEMA = "nodeName,pid,port,openTime,closeTime";
const std::string AUDIT_SOCKET_EVENTS_KEY = "SocketEvent";

const std::string AUDIT_SOCKETCONNECT_EVENTS_TABLENAME = "auditdsocketconnectevents";
const std::string AUDIT_SOCKETCONNECT_EVENTS_SCHEMA = "nodeName,pid,dstPort,"
    "connectTime,dstNode";
const std::string AUDIT_SOCKETCONNECT_EVENTS_KEY = "SocketConnectEvent";

// define provenance input sources
const std::string AUDITD_SRC = "auditd";
const std::string SCALE_SRC = "scale";

// define supported stream types
const std::string ODBC_STREAM = "ODBC";
const std::string KAFKA_STREAM = "Kafka";
const std::string FILE_STREAM = "File";
}

#endif /* UTIL_CONSTANTS_H_ */
