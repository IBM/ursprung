CREATE TABLE gpfseventsNarrow (
	event varchar(20),
	clusterName varchar(32),
	nodeName varchar(128),
	fsName varchar(32),
	path varchar(256),
	inode int,
	bytesRead int,
	bytesWritten int,
	pid int,
	eventTime timestamp(3), /* GPFS provides ns precision but modb2rest.py chokes on it. */
	dstPath varchar(256),
	version varchar(32)
);

CREATE TABLE auditdProcessEvents (
  /* Node identifiers. */
    nodeName varchar(128)
  /* OS identifiers. */
  , pid integer
  , ppid integer
  , pgid integer
  /* What was it doing? */
  , execCwd varchar(256)
  , execCmdLine varchar(512)
  /* timestamp(3): auditd provides ms precision. */
  , birthTime timestamp(3)
  , deathTime timestamp(3)
);

CREATE TABLE auditdProcessGroupEvents (
  /* Node identifiers. */
    nodeName varchar(128)
  /* OS identifiers. */
  , pgid integer
  /* timestamp(3): auditd provides ms precision. */
  , birthTime timestamp(3)
  , deathTime timestamp(3)
);

CREATE TABLE auditdSyscallEvents (
  /* Who? */
  nodeName varchar(128)
  , eventId int
  , pid int
  , ppid int
  , uid int
  , gid int
  , euid int
  , egid int
  /* What? */
  , syscall varchar(10)
  /* Args are human-readable: O_CREAT|... */
  , arg0 varchar(200)
  , arg1 varchar(200)
  , arg2 varchar(200)
  , arg3 varchar(200)
  , arg4 varchar(200)
  , rc int
  /* When? */
  /* timestamp(3): auditd provides ms precision. */
  , eventTime timestamp(3)
  /* Additional info. */
  , data1 varchar(256) /* execve: command line */
  , data2 varchar(256) /* execve: cwd */
  /* Possible primary key:
   *   - node and pid are obviously necessary.
   *   - eventId and time uniquely identify the event
   *     in case of eventId reuse. */
   /* , PRIMARY KEY(nodeName, eventId, pid, eventTime) */
);

CREATE TABLE auditdIPCEvents(
	nodeName varchar(128),
	srcPid int,
	dstPid int,
	srcBirth timestamp(3),
	dstBirth timestamp(3)
);

CREATE TABLE auditdSocketEvents(
	nodeName varchar(128),
	pid int,
	port int,
	openTime timestamp(3),
	closeTime timestamp(3)
);

CREATE TABLE auditdSocketConnectEvents(
	nodeName varchar(128),
	pid int,
	dstPort int,
	connectTime timestamp(3),
	dstNode varchar(128)
);

CREATE TABLE workflowsteps (
	id varchar(64) not null,
	starttime varchar(64),
	endtime varchar(64),
	submittime varchar(64),
	jobid varchar(64),
	parentworkflowid varchar(64),
	name varchar(40),
	owner varchar(40),
	state int,
	primary key(id)
);

CREATE TABLE workflows (
	id varchar(64) not null,
	starttime varchar(64),
	endtime varchar(64),
	submittime varchar(64),
	jobid varchar(64),
	name varchar(40),
	owner varchar(40),
	state int,
	exitcode int,
	definition1 varchar(128),
	definition2 varchar(128),
	primary key(id)
);

CREATE TABLE scheduler (
	submittime varchar(64),
	jobid varchar(64) not null,
	jobpid int,
	jobpgid int,
	primary key(jobid)
);

CREATE TABLE warnerrorlogs(
	pid int,
	nodeName varchar(128),
	loglevel varchar(32),
	logmsg varchar(256),
	logtime timestamp
);

CREATE table rulestate(
	id varchar(32) not null,
	actionname varchar(32),
	target varchar(128) not null,
	state varchar(64),
	primary key(id,target)
);

CREATE TABLE versions(
	clusterName varchar(128),
	nodeName varchar(128),
	fsName varchar(32),
	path varchar(256),
	inode bigint,
	eventTime timestamp(3),
	commitId varchar(48)
);
