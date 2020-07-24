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

// load environment variables from .env
const dotenv = require('dotenv');
dotenv.config();

var constants = require('./constants');
var express = require('express');
var request = require('request');
var assert = require('assert');
var bodyParser = require('body-parser');
var cors = require('cors');
const odbc = require('odbc');

var app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(cors());

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// start app
app.listen(constants.PORT, function () {
    console.log(`UI backend started on port ${constants.PORT} with dsn ${constants.DSN}`);
});

// configure logger
const winston = require('winston');
const tsFormat = () => (new Date()).toLocaleString();
const logger = winston.createLogger({
    transports: [
        new (winston.transports.Console)({
            timestamp: tsFormat,
            colorize: true,
            level: 'info'
        })
    ]
});

/**
 * Table for various request types.
 * Each entry's name is the requestType string.
 * Value is an object with keys:
 *   requiredParms - arg validation
 *   sqlGenerator  - for SQL generation; accepts request.body.parms
 */
const requestTypeTable = {
    FILEPROV_BY_PATH: {
        // TODO If we consider name re-use, need a timestamp
        requiredParms: ["path"],
        sqlGenerator: sqlForProv_FileByFilename
    },
    FILEPROV_BY_INODE: {
        // TODO Path and inode reduce the probability of inode reuse collisions.
        // However, to entirely prevent them, we need a timestamp.
        requiredParms: ["inode", "path"],
        sqlGenerator: sqlForProv_FileByInode
    },
    PROCESSPROV_INITIAL: {
        requiredParms: ["processName"],
        sqlGenerator: sqlForProv_Process
    },
    PROCESSPROV_FS_ACCESSES: {
        requiredParms: ["nodeName", "pid", "birthTime", "deathTime"],
        sqlGenerator: sqlForProv_Process_FSAccess
    },
    PROCESSPROV_IPC: {
        requiredParms: ["nodeName", "pid", "birthTime", "deathTime", "pgid"],
        sqlGenerator: sqlForProv_Process_IPC
    },
    PROCESSPROV_TRUEIPC: {
        requiredParms: ["nodeName", "pid", "birthTime"],
        sqlGenerator: sqlForProv_Process_TrueIPC
    },
    PROCESSPROV_NET: {
        requiredParms: ["nodeName", "pid", "birthTime", "deathTime", "pgid"],
        sqlGenerator: sqlForProv_Process_Net
    },
    WORKFLOWS: {
        requiredParms: [],
        sqlGenerator: sqlForWorkflows
    },
    WORKFLOW_OUTPUT_FILES: {
        requiredParms: ["wid", "wstart", "wfinish"],
        sqlGenerator: sqlForWorkflows_Output_Files
    }, OUTPUT_FILES_WORKFLOW: {
        requiredParms: ["path", "inode"],
        sqlGenerator: sqlForOutput_File_Workflow
    }, PROCESSLOGS: {
        requiredParms: ["nodeName", "pid", "ppid", "birthTime", "deathTime"],
        sqlGenerator: sqlForProcessLogs
    },
    WORKFLOWS_PROCESSES: {
        requiredParms: ["job"],
        sqlGenerator: sqlForWorkflowProcesses
    },
    FILECOMMITID: {
        requiredParms: ["nodename", "path", "inode", "eventtime"],
        sqlGenerator: sqlForFileContent
    },
    FILECONTENT: {
        requiredParms: ["commitid", "path"]
    }
};

/**
 * Names of tables for use in SELECT, JOIN ON, and WHERE clauses.
 */
const sqlTableNames = {
    gpfs: "gpfsEvents",
    process: "processEvents",
    processGroup: "processGroupEvents",
    ipc: "ipcEvents",
    socket: "socketEvents",
    socketconnect: "socketConnectEvents",
    workflows: "workflowEvents",
    workflowSteps: "workflowStepEvents",
    scheduler: "schedulerEvents",
    processlogs: "processlogs",
    versions: "versions"
};

/**
 * Returns an object with the same keys as o,
 * whose values are vNew = `'${vOld}'`.
 */
function stringifyObjectProperties(o) {
    let ret = {};
    Object.getOwnPropertyNames(o).forEach((n) => {
        ret[n] = `'${o[n]}'`;
    });
    return ret;
}

/**
 * This is the endpoint for retrieving file content.
 * 
 * NOTE: The URL is currently hardcoded just for testing
 * (as this is the only request that has to go to our
 * custom REST service). Once we can update the REST
 * service in Kubernetes, we can send this to the
 * same base URL as the SQL queries.
 */
app.post('/provenance/filecontent', function (req, res) {
    // TODO rewrite to talk directly to database
    console.log("Not supported.");
    // const requestBody = req.body;
    // assert(isPostBodyValid(requestBody), `Error, invalid post body. body ${JSON.stringify(requestBody)}`);

    // let path = requestBody.params.path;
    // let commitid = requestBody.params.commitid;
    // const options = { url: targetURL }

    // const promiseSearchResult = promiseSearch(options);
    // Promise.all([promiseSearchResult])
    //     .then(function (result) {
    //         const response = {
    //             request: req.body
    //             , filecontent: result
    //         };
    //         logger.info(`Sending: ${JSON.stringify(response, null, 2)}`);
    //         res.send(response);
    //     })
    //     .catch((err) => {
    //         const msg = `Caught err: ${JSON.stringify(err)}`;
    //         logger.info(msg);
    //         res.send(msg);
    //     });
});

/**
 * This is the critical endpoint.
 * On a valid request (see isPostBodyValid),
 * query DB and return in res.data an object with key 'result'
 * whose value is the matching rows from the DB as POJOs.
 */
app.post('/provenance', function (req, res) {
    // validate input
    const requestBody = req.body;
    assert(isPostBodyValid(requestBody), `Error, invalid post body. body ${JSON.stringify(requestBody)}`);

    // Make all params quote-delimited so we don't have to worry about
    // char vs. numeric types when we use them in a SQL query.
    requestBody.params = stringifyObjectProperties(requestBody.params);

    // generate SQL
    const sql = genSQL(requestBody);

    // query DB
    const promiseSearchResult = promiseQuery(sql);
    Promise.all([promiseSearchResult])
        .then(function (result) {
            const records = result[0];
            logger.debug(`Got ${records.length} records: ${JSON.stringify(records)}`);

            // make object keys all lowercase
            const recordsLower = records.map((item) => {
                var key, keys = Object.keys(item);
                var n = keys.length;
                var newItem = {}
                while (n--) {
                    key = keys[n];
                    newItem[key.toLowerCase()] = item[key];
                }
                return newItem;
            });

            const response = {
                request: req.body,
                records: recordsLower
            };
            logger.debug(`Sending: ${JSON.stringify(response, null, 2)}`);
            res.send(response);
        })
        .catch((err) => {
            const msg = `Caught err: ${JSON.stringify(err)}`;
            logger.info(msg);
            res.send(msg);
        });
});

/**
 * Submit the specified query to the provenance database
 * through the ODBC DSN 'dashdb'. Return a promise with
 * the query results.
 * 
 * TODO make DSN configurable through environment
 */
function promiseQuery(queryStr) {
    console.log(`Query: ${JSON.stringify(queryStr)}`);
    return new Promise(function (resolve, reject) {
        odbc.connect(`DSN=${constants.DSN}`, function (error, conn) {
            conn.query(queryStr, function (error, result) {
                if (error) {
                    reject(error);
                } else {
                    resolve(result);
                }
            });
        });
    });
}

/**
 * Check whether the request contains all necessary parameters.
 */
function isPostBodyValid(requestBody) {
    // validate requestType
    const requestType = requestBody.requestType;
    if (!requestType) {
        logger.info(`Error, you must provide requestType`);
        return false;
    } else if (!requestTypeTable[requestType]) {
        logger.info(`Error, invalid requestType ${requestType}. Choose from ${Object.keys(requestTypeTable)}`);
        return false;
    }

    // validate params
    const params = requestBody.params;
    if (!params) {
        logger.info(`Error, you must provide params`);
        return false;
    } else {
        let hasRequiredParms = true;
        requestTypeTable[requestType].requiredParms.forEach((p) => {
            try {
                if (!params[p]) {
                    logger.info(`Missing required parm ${p}`);
                    hasRequiredParms = false;
                }
            } catch (e) {
                logger.info(`Missing required parm ${p}`);
                hasRequiredParms = false;
            }
        });

        if (!hasRequiredParms) {
            return false;
        }
    }

    return true;
}

/**
 * Generate SQL query based on the body of the POST request.
 */
function genSQL(reqBody) {
    const niceSQL = requestTypeTable[reqBody.requestType].sqlGenerator(reqBody.params);
    const terseSQL = sqlAsOneLiner(niceSQL);
    logger.info(`Query:
      niceSQL: ${niceSQL}
      terseSQL: <${terseSQL}>`
    );

    return terseSQL;
}

/**
 * Convert this sql to a one-liner.
 */
function sqlAsOneLiner(sql) {
    return sql
        .replace(/\n/g, ' ') // newlines -> whitespace
        .replace(/\/\*.*?\*\//g, '') // remove comments
        .replace(/\s+/g, ' ') // multiple whitespace -> ' '
        .replace(/\s*,\s*/g, ',') // remove whitespace around commas
        .trim(); // remove leading/trailing whitespace
}

/**
 * SQL query to get provenance information for the file identified by fileFilterClause.
 * 
 * TODO We could use UNIQUE to reduce duplicates. For example:
 * a process opens and closes the same file repeatedly
 * => we only need to know that it did IO to that file.
 */
function sqlForProv_File(fileFilterClause, onlyWrite, dateConstraint) {
    const process_birthTime = `${sqlTableNames.process}.birthTime`;
    const process_deathTime = `${sqlTableNames.process}.deathTime`;
    const gpfs_eventTime = `${sqlTableNames.gpfs}.eventTime`;
    const sql_birthPrecededEvent = sql_fuzzyLT(process_birthTime, gpfs_eventTime, fuzzyTimeErrorMs);
    const sql_eventPrecededDeath = sql_fuzzyLT(gpfs_eventTime, process_deathTime, fuzzyTimeErrorMs);

    var filter;
    if (onlyWrite) {
        filter = `(${sqlTableNames.gpfs}.event = 'CLOSE' AND 0 < ${sqlTableNames.gpfs}.bytesWritten)`;
    } else {
        filter = `
       (${sqlTableNames.gpfs}.event = 'CREATE')
       OR
       (${sqlTableNames.gpfs}.event = 'CLOSE' AND (0 < ${sqlTableNames.gpfs}.bytesRead OR 0 < ${sqlTableNames.gpfs}.bytesWritten))
       OR
       (${sqlTableNames.gpfs}.event = 'UNLINK')
       OR
       (${sqlTableNames.gpfs}.event = 'RENAME')
      `;
    }

    var dateFilter;
    if (dateConstraint) {
        dateFilter = `${sqlTableNames.gpfs}.eventTime <= ${dateConstraint}`;
    } else {
        dateFilter = `true`;
    }

    const sql = `
    SELECT
    /* What happened? */ ${sqlTableNames.gpfs}.event, ${sqlTableNames.gpfs}.eventTime
    /* To what? */     , ${sqlTableNames.gpfs}.path, ${sqlTableNames.gpfs}.dstPath, ${sqlTableNames.gpfs}.inode, ${sqlTableNames.gpfs}.version
    /* By whom? */     , ${sqlTableNames.process}.nodeName, ${sqlTableNames.process}.pid, ${sqlTableNames.process}.birthTime, ${sqlTableNames.process}.deathTime, ${sqlTableNames.process}.execCmdLine, ${sqlTableNames.process}.pgid, ${sqlTableNames.process}.ppid, ${sqlTableNames.process}.execcwd
    /* How much? */    , ${sqlTableNames.gpfs}.bytesRead, ${sqlTableNames.gpfs}.bytesWritten
    FROM gpfseventsNarrow AS ${sqlTableNames.gpfs} INNER JOIN auditdProcessEvents AS ${sqlTableNames.process}
    ON (
        ${sqlTableNames.gpfs}.nodeName = ${sqlTableNames.process}.nodeName
        AND
        ${sqlTableNames.gpfs}.pid = ${sqlTableNames.process}.pid
        AND
        /* Different event streams are not guaranteed to use the same time source.
         * For example, clock_gettime(CLOCK_REALTIME) vs. clock_gettime(CLOCK_REALTIME_COARSE).
         *   - auditd uses coarse time (https://www.redhat.com/archives/linux-audit/2018-August/msg00000.html)
         *   - Scale uses more accurate time (agrees with strace which uses CLOCK_REALTIME).
         * As a result, strict timestamp comparisons are invalid.
         * Another distance metric is needed -- here we are using "abs(X -Y) <= e".
         * This opens the door to false matches, e.g. pid re-use between Y and Y + e.
         * This is extremely unlikely.
         */
        (
         ${sql_birthPrecededEvent}
         AND
         ${sql_eventPrecededDeath}
        )
      )
    WHERE (
        (${fileFilterClause})
        AND (${filter})
        AND (1970 < EXTRACT(YEAR from ${sqlTableNames.process}.birthTime) /* TODO Processes with unknown start times are set to epoch (1970-01-01). But there should be no such processes. */)
        AND (${dateFilter})
      )
    `;

    return sql;
}

/**
 * SQL query to get provenance information for a file, by its name.
 */
function sqlForProv_FileByFilename(params) {
    console.log(`sqlForProv_FileByFilename`);
    const fileFilterClause = `${sqlTableNames.gpfs}.path = ${params.path} OR ${sqlTableNames.gpfs}.dstPath = ${params.path}`;
    return sqlForProv_File(fileFilterClause, false, params.dateConstraint);
}

/**
 * SQL query to get provenance information for a file, by its inode number.
 */
function sqlForProv_FileByInode(params) {
    console.log(`sqlForProv_FileByInode`);
    const fileFilterClause = `${sqlTableNames.gpfs}.inode = ${params.inode} AND (${sqlTableNames.gpfs}.path = ${params.path} OR ${sqlTableNames.gpfs}.dstPath = ${params.path})`;
    var onlyWrite = false;
    if (params.onlyWrite) {
        onlyWrite = true;
    }
    return sqlForProv_File(fileFilterClause, onlyWrite, params.dateConstraint);
}

/**
 * SQL query to retrieve all processes that match the specified regex. This
 * is implemented via LIKE.
 */
function sqlForProv_Process(params) {
    console.log(`sqlForProv_Process`);
    const sql = `SELECT * FROM auditdprocessevents AS ${sqlTableNames.process}
               WHERE ${sqlTableNames.process}.execcmdline LIKE '%${params.processName.substring(1, params.processName.length - 1)}%'`;

    return sql;
}

/**
 * SQL query to get FS provenance information (read/write set) for a process.
 */
function sqlForProv_Process_FSAccess(params) {
    // Find all the files with which this process interacted.
    // For this we already have a particular process defined in params,
    // so we only need to examine gpfseventsNarrow.
    console.log(`sqlForProv_Process_FSAccess`);

    const p_birthTime = `TO_TIMESTAMP(${params.birthTime}, 'YYYY-MM-DD HH24:MI:SS.MS')`;
    const p_deathTime = `TO_TIMESTAMP(${params.deathTime}, 'YYYY-MM-DD HH24:MI:SS.MS')`;
    const x_eventTime = `${sqlTableNames.gpfs}.eventTime`;

    var filter;
    if (params.onlyRead) {
        filter = `(${sqlTableNames.gpfs}.event = 'CLOSE' AND 0 < ${sqlTableNames.gpfs}.bytesRead)`;
    } else if (params.onlyWrite) {
        filter = `(${sqlTableNames.gpfs}.event = 'CLOSE' AND 0 < ${sqlTableNames.gpfs}.bytesWritten)`;
    } else {
        filter = `
      (${sqlTableNames.gpfs}.event = 'CREATE')
      OR
      (${sqlTableNames.gpfs}.event = 'CLOSE' AND (0 < ${sqlTableNames.gpfs}.bytesRead OR 0 < ${sqlTableNames.gpfs}.bytesWritten))
      OR
      (${sqlTableNames.gpfs}.event = 'UNLINK')
      OR
      (${sqlTableNames.gpfs}.event = 'RENAME')
      `;
    }

    var dateFilter;
    if (params.dateConstraint) {
        dateFilter = `${sqlTableNames.gpfs}.eventTime <= ${params.dateConstraint}`;
    } else {
        dateFilter = `true`;
    }

    const sql_birthPrecededEvent = sql_fuzzyLT(p_birthTime, x_eventTime, fuzzyTimeErrorMs);
    const sql_eventPrecededDeath = sql_fuzzyLT(x_eventTime, p_deathTime, fuzzyTimeErrorMs);

    const sql = `SELECT
    /* What happened? */ ${sqlTableNames.gpfs}.event, ${sqlTableNames.gpfs}.eventTime
    /* To what? */     , ${sqlTableNames.gpfs}.path, ${sqlTableNames.gpfs}.dstPath, ${sqlTableNames.gpfs}.inode
    /* How much? */    , ${sqlTableNames.gpfs}.bytesRead, ${sqlTableNames.gpfs}.bytesWritten
    FROM gpfseventsNarrow AS ${sqlTableNames.gpfs}
    WHERE (
      ( /* Identify the process. */
       ${sqlTableNames.gpfs}.nodeName = ${params.nodeName}
       AND ${sqlTableNames.gpfs}.pid = ${params.pid}
       AND (${sql_birthPrecededEvent} AND ${sql_eventPrecededDeath})
      )
      AND ( /* Filter for accesses of interest. */ ${filter})
      AND ( /* Date filter (optional). */ ${dateFilter})
    )
    `;

    return sql;
}

/**
 * SQL query to get IPC provenance information (read/write set) for the process
 * described in params.
 */
function sqlForProv_Process_IPC(params) {
    // find all the processes with which this process interacted over IPC
    console.log(`sqlForProv_Process_IPC`);

    const sql = `SELECT
    /* Who? */       ${sqlTableNames.process}.nodeName, ${sqlTableNames.process}.pid
    /* When? */    , ${sqlTableNames.process}.birthTime, ${sqlTableNames.process}.deathTime
    /* Details. */ , ${sqlTableNames.process}.execCmdLine, ${sqlTableNames.process}.pgid, ${sqlTableNames.process}.ppid
    FROM         auditdProcessEvents      AS ${sqlTableNames.process}
      /* INNER JOIN: Only keeps processes in whose process group membership we are confident. */
      INNER JOIN auditdProcessGroupEvents AS ${sqlTableNames.processGroup}
      ON ( /* Same process group: */
           /* - Same node */
           ${sqlTableNames.processGroup}.nodeName = ${sqlTableNames.process}.nodeName
           /* - Same pgid */
       AND ${sqlTableNames.processGroup}.pgid = ${sqlTableNames.process}.pgid
       AND ( /* - Lifetime subsumption.
              *   No fuzzy time because these tables both use the auditd time source. */
            ${sqlTableNames.processGroup}.birthTime <= ${sqlTableNames.process}.birthTime
        AND ${sqlTableNames.process}.deathTime <= ${sqlTableNames.processGroup}.deathTime
           )
         )
    WHERE (
       /* Same node. */
       ${sqlTableNames.process}.nodeName = ${params.nodeName}
       AND /* Same pgid. */ ${sqlTableNames.process}.pgid = ${params.pgid}
       AND
       /* We already have the process of interest, so filter it out from the result set.
        * NB There is a corner case here where a pgroup is so large that it contains
        * two different processes with the same pid. We ASSUME this doesn't happen. */
       ${sqlTableNames.process}.pid != ${params.pid}
    )
    `;

    return sql;
}

/**
 * SQL query to get IPC provenance information (read/write set) for the process
 * described in params. This is for "true" provenance and uses the auditdipcevents table.
 */
function sqlForProv_Process_TrueIPC(params) {
    // find all the processes with which this process interacted over IPC
    console.log(`sqlForProv_Process_TrueIPC`);
    const sql = `
    /* We first retrieve all the read dependencies of the queries process, i.e. every other process
     * that read output from the queries process. */
    (
    SELECT 'read' as event,
      ${sqlTableNames.process}.nodeName, ${sqlTableNames.process}.pid,
      ${sqlTableNames.process}.birthTime, ${sqlTableNames.process}.deathTime,
      ${sqlTableNames.process}.execCmdLine, ${sqlTableNames.process}.pgid, ${sqlTableNames.process}.ppid
    FROM auditdprocessevents as ${sqlTableNames.process}, auditdipcevents as ${sqlTableNames.ipc}
    WHERE ${sqlTableNames.process}.pid = ${sqlTableNames.ipc}.dstPid
    AND   ${sqlTableNames.process}.nodename = ${sqlTableNames.ipc}.nodeName
    AND   ${sqlTableNames.ipc}.srcPid = ${params.pid}
    AND   ${sqlTableNames.ipc}.srcBirth = ${params.birthTime}
    AND   ${sqlTableNames.ipc}.nodeName = ${params.nodeName}
    )
    UNION
    /* We now retrieve all the write dependencies of the queries process, i.e. every other process
     * that wrote to the input of the queries process. */
    (
    SELECT 'write' as event,
      ${sqlTableNames.process}.nodeName, ${sqlTableNames.process}.pid,
      ${sqlTableNames.process}.birthTime, ${sqlTableNames.process}.deathTime,
      ${sqlTableNames.process}.execCmdLine, ${sqlTableNames.process}.pgid, ${sqlTableNames.process}.ppid
    FROM auditdprocessevents as ${sqlTableNames.process}, auditdipcevents as ${sqlTableNames.ipc}
    WHERE ${sqlTableNames.process}.pid = ${sqlTableNames.ipc}.srcPid
    AND   ${sqlTableNames.process}.nodename = ${sqlTableNames.ipc}.nodeName
    AND   ${sqlTableNames.ipc}.dstPid = ${params.pid}
    AND   ${sqlTableNames.ipc}.dstBirth = ${params.birthTime}
    AND   ${sqlTableNames.ipc}.nodeName = ${params.nodeName}
    )`;
    return sql;
}

/**
 * SQL query to get Network provenance information (read/write set) for the process
 * described in params.
 */
function sqlForProv_Process_Net(params) {
    // find all the processes with which this process interacted over IPC
    console.log(`sqlForProv_Process_Net`);
    const sql = `
    /* We first retrieve all the 'connections made' dependencies of the queried process, i.e. every other process
     * to which the queried process has connected. */
    (
    SELECT 'read' as event,
      ${sqlTableNames.process}.nodeName, ${sqlTableNames.process}.pid,
      ${sqlTableNames.process}.birthTime, ${sqlTableNames.process}.deathTime,
      ${sqlTableNames.process}.execCmdLine, ${sqlTableNames.process}.pgid, ${sqlTableNames.process}.ppid
	FROM auditdsocketevents as ${sqlTableNames.socket},
	  auditdsocketconnectevents as ${sqlTableNames.socketconnect},
	  auditdprocessevents as ${sqlTableNames.process}
	WHERE ${sqlTableNames.socketconnect}.pid = ${params.pid}
	AND ${sqlTableNames.socketconnect}.connecttime >= ${params.birthTime}
	AND ${sqlTableNames.socketconnect}.connecttime <= ${params.deathTime}
	AND ${sqlTableNames.socketconnect}.nodename <= ${params.nodeName}
	AND ${sqlTableNames.socketconnect}.dstnode = ${sqlTableNames.socket}.nodename
	AND ${sqlTableNames.socketconnect}.dstport = ${sqlTableNames.socket}.port
	AND ${sqlTableNames.socketconnect}.connecttime >= ${sqlTableNames.socket}.opentime
	AND ${sqlTableNames.socketconnect}.connecttime <= ${sqlTableNames.socket}.closetime
	AND ${sqlTableNames.process}.pid = ${sqlTableNames.socket}.pid
	AND ${sqlTableNames.process}.nodename = ${sqlTableNames.socket}.nodename
	AND ${sqlTableNames.process}.birthtime <= ${sqlTableNames.socket}.opentime
	AND ${sqlTableNames.process}.deathtime >= ${sqlTableNames.socket}.closetime
    )
    UNION
    /* We now retrieve all the 'connections accepted' of the queried process, i.e. every other process
     * that connected to the queried process. */
    (
    SELECT 'write' as event,
      ${sqlTableNames.process}.nodeName, ${sqlTableNames.process}.pid,
      ${sqlTableNames.process}.birthTime, ${sqlTableNames.process}.deathTime,
      ${sqlTableNames.process}.execCmdLine, ${sqlTableNames.process}.pgid, ${sqlTableNames.process}.ppid
	FROM auditdsocketevents as ${sqlTableNames.socket},
	  auditdsocketconnectevents as ${sqlTableNames.socketconnect},
	  auditdprocessevents as ${sqlTableNames.process}
	WHERE ${sqlTableNames.socket}.pid = ${params.pid}
	AND ${sqlTableNames.socket}.opentime >= ${params.birthTime}
	AND ${sqlTableNames.socket}.closetime <= ${params.deathTime}
	AND ${sqlTableNames.socket}.nodename <= ${params.nodeName}
	AND ${sqlTableNames.socketconnect}.dstnode = ${sqlTableNames.socket}.nodename
	AND ${sqlTableNames.socketconnect}.dstport = ${sqlTableNames.socket}.port
	AND ${sqlTableNames.socketconnect}.connecttime >= ${sqlTableNames.socket}.opentime
	AND ${sqlTableNames.socketconnect}.connecttime <= ${sqlTableNames.socket}.closetime
	AND ${sqlTableNames.process}.pid = ${sqlTableNames.socketconnect}.pid
	AND ${sqlTableNames.process}.nodename = ${sqlTableNames.socketconnect}.nodename
    )`;
    return sql;
}

/**
 * SQL query to retrieve all workflows that have been run so far in the cluster.
 */
function sqlForWorkflows(params) {
    // retrieve all workflows that have been run so far
    console.log(`sqlForWorkflows`);
    const sql = `SELECT * FROM workflows AS ${sqlTableNames.workflows} ORDER BY starttime desc`;
    return sql;
}

/**
 * SQL query to get all output files written by a specific workflow.
 */
function sqlForWorkflows_Output_Files(params) {
    // find all the files which the specificed workflow wrote during the last workflow step
    console.log(`sqlForWorkflows_Output_Files`);

    const workflowStartTime = `${params.wstart}`;
    const workflowFinishTime = `${params.wfinish}`;
    const workflowID = `${params.wid}`;

    // TODO do we have to fuzzify times?
    //const sql_startPrecededEvent = sql_fuzzyLT(workflowStartTime, x_eventTime, fuzzyTimeErrorMs);
    //const sql_finishPrecededDeath = sql_fuzzyLT(workflowFinishTime, p_deathTime, fuzzyTimeErrorMs);

    const subquery = `SELECT
    ${sqlTableNames.workflowSteps}.name
    FROM workflowSteps AS ${sqlTableNames.workflowSteps}
    WHERE ${sqlTableNames.workflowSteps}.parentworkflowid=${params.wid}
    ORDER BY ${sqlTableNames.workflowSteps}.endtime DESC LIMIT 1`

    const sql = `SELECT
    ${sqlTableNames.gpfs}.pid, ${sqlTableNames.gpfs}.inode, ${sqlTableNames.gpfs}.path
    FROM gpfseventsNarrow AS ${sqlTableNames.gpfs}, auditdprocessevents AS ${sqlTableNames.process},
        scheduler AS ${sqlTableNames.scheduler}, workflows AS ${sqlTableNames.workflows},
        workflowSteps AS ${sqlTableNames.workflowSteps}
    WHERE
       /* Identify the workflow steps for the specified workflow. */
       ${sqlTableNames.workflows}.id = ${sqlTableNames.workflowSteps}.parentworkflowid
       AND
       /* Identify the job pids for the workflow steps. */
       ${sqlTableNames.workflowSteps}.jobid = ${sqlTableNames.scheduler}.jobid
       AND
       /* Identify the pids for the job ids. */
       ${sqlTableNames.scheduler}.jobpgid = ${sqlTableNames.process}.pgid
       AND
       /* Identify the pids that have interacted with the filesystem. */
       ${sqlTableNames.process}.pid = ${sqlTableNames.gpfs}.pid
       AND
       /* Only select the specified workflow. */
       ${sqlTableNames.workflows}.id=${workflowID}
       AND
       /* Only select write dependencies. */
       ${sqlTableNames.gpfs}.bytesWritten > 0 AND ${sqlTableNames.gpfs}.event='CLOSE'
       AND
       /* Only select the last workflow steps. */
       ${sqlTableNames.workflowSteps}.name in (${subquery})
       AND
       /* Restrict process events to the lifetime of the workflow. */
       ${sqlTableNames.process}.birthtime >= ${workflowStartTime}
       AND
       ${sqlTableNames.process}.deathtime <= ${workflowFinishTime}
       AND
       ${sqlTableNames.gpfs}.eventtime <= ${workflowFinishTime}
       AND
       ${sqlTableNames.gpfs}.eventtime >= ${workflowStartTime}`;

    return sql;
}

/**
 * SQL query to get the workflow that wrote a specific output file
 */
function sqlForOutput_File_Workflow(params) {
    console.log(`sqlForOutput_File_Workflow`);
    const path = `${params.path}`;
    const inode = `${params.inode}`;

    const sql = `SELECT
    ${sqlTableNames.workflows}.name, ${sqlTableNames.workflows}.definition1, ${sqlTableNames.workflows}.definition2
    FROM gpfseventsNarrow AS ${sqlTableNames.gpfs}, auditdprocessevents AS ${sqlTableNames.process},
        scheduler AS ${sqlTableNames.scheduler}, workflows AS ${sqlTableNames.workflows},
        workflowSteps AS ${sqlTableNames.workflowSteps}
    WHERE
       /* Filter for the specified output file. */
       ${sqlTableNames.gpfs}.path = ${path} AND ${sqlTableNames.gpfs}.inode = ${inode}
       AND
       /* Only select write dependencies. */
       ${sqlTableNames.gpfs}.bytesWritten > 0 AND ${sqlTableNames.gpfs}.event='CLOSE'
       AND
       /* Identify the pids that have interacted with the filesystem. */
       ${sqlTableNames.process}.pid = ${sqlTableNames.gpfs}.pid
       AND
       /* Identify the pids for the job ids. */
       ${sqlTableNames.scheduler}.jobpgid = ${sqlTableNames.process}.pgid
       AND
       /* Identify the job pids for the workflow steps. */
       ${sqlTableNames.workflowSteps}.jobid = ${sqlTableNames.scheduler}.jobid
       AND
       /* Identify the workflow steps for the specified workflow. */
       ${sqlTableNames.workflows}.id = ${sqlTableNames.workflowSteps}.parentworkflowid`;

    return sql;
}

/**
 * SQL query to retrieve all associated log entries for the process specified in params
 */
function sqlForProcessLogs(params) {
    // retrieve all logs for the specified process
    console.log(`sqlForProcessLogs`);

    const nodeName = `${params.nodeName}`;
    const pid = `${params.pid}`;
    const ppid = `${params.ppid}`;
    const birthTime = `${params.birthTime}`;
    const deathTime = `${params.deathTime}`;

    // TODO The selection on ppid is a hack to make this work for Spark.
    // In Spark, workers spawn executors, which are separated processes and vforked to run
    // inside their own JVM. We want to capture the worker logs but assoicate them
    // with the executor pid and since the worker spawns the executor, there's a parent
    // child relationship between the two.
    //
    // Is this the correct semantics? Should logs only be captured for the actual process
    // or is it ok to also display the logs from the parent process? Does that even make sense
    // in Spark?
    const sql = `SELECT
    ${sqlTableNames.processlogs}.loglevel,  ${sqlTableNames.processlogs}.logmsg
    FROM warnerrorlogs AS ${sqlTableNames.processlogs}
    WHERE
      (
        ${sqlTableNames.processlogs}.pid = ${pid}
        OR
        ${sqlTableNames.processlogs}.pid = ${ppid}
      )
      AND
      ${sqlTableNames.processlogs}.nodeName = ${nodeName}
      AND
      ${sqlTableNames.processlogs}.logtime > ${birthTime}
      AND
      ${sqlTableNames.processlogs}.logtime < ${deathTime}
    `;

    return sql;
}

/**
 * SQL query to retrieve all workflows that have been run so far in the cluster.
 */
function sqlForWorkflowProcesses(params) {
    // retrieve all workflows that have been run so far
    console.log(`sqlForWorkflowProcesses`);
    const job = `${params.job}`;

    const sql = `SELECT
                 pid AS id,
                 execcmdline AS name,
                 birthtime AS starttime,
                 nodename AS owner,
                 deathtime as endtime,
                 ppid as ppid,
                 pgid as pgid
               FROM auditdprocessevents AS ${sqlTableNames.process}
               WHERE execcmdline LIKE '%${job.substring(1, job.length - 1)}%' ORDER BY birthtime DESC`;

    return sql;
}

/**
 * SQL query to retrieve the commit id for a specific file update from a specific process.
 */
function sqlForFileContent(params) {
    console.log(`sqlForFileContent`);

    const nodename = `${params.nodename}`;
    const path = `${params.path}`;
    const inode = `${params.inode}`;
    const pid = `${params.pid}`;
    const eventtime = `${params.eventtime}`;

    const sql = `SELECT commitid
               FROM versions AS ${sqlTableNames.versions}
               WHERE nodename = ${nodename}
               AND path = ${path}
               AND inode = ${inode}
               AND eventtime = ${eventtime}`

    return sql;
}

// assume time sources are within 500 ms of each other
const fuzzyTimeErrorMs = 500;

/**
 * Returns a SQL clause that evaluates to true if t1 is fuzzily LT  t1 < t2 within error err,
 * i.e. evaluates to true if: t1 < t2 + errMs. errMs is an integer representing milliseconds.
 * t1, t2 might be something like 'gpfsEvents.eventTime'
 */
function sql_fuzzyLT(t1, t2, errMs) {
    // DB2 TIMESTAMPDIFF: https://www.ibm.com/support/knowledgecenter/en/SSEPEK_10.0.0/sqlref/src/tpc/db2z_bif_timestampdiff.html
    const TSD_US = 1; // See mapping in DB2 TIMESTAMPDIFF docs
    const us_per_ms = 1000;
    const errUs = errMs * us_per_ms;
    return `( ${t1} < (${t2} + '${errUs} MICROSECONDS') )`;
}

/**
 * Like sql_fuzzyLT but for equality.
 */
function sql_fuzzyEquals(t1, t2, errMs) {
    // DB2 TIMESTAMPDIFF: https://www.ibm.com/support/knowledgecenter/en/SSEPEK_10.0.0/sqlref/src/tpc/db2z_bif_timestampdiff.html
    const TSD_US = 1; // See mapping in DB2 TIMESTAMPDIFF docs
    const us_per_ms = 1000;
    const errUs = errMs * us_per_ms;
    // TODO Implement using two calls to sql_fuzzyLT instead of repeating this code.
    return `(
    ( ${t1} < (${t2} + '${errUs} MICROSECONDS') )
    AND
    ( ${t2} < (${t1} + '${errUs} MICROSECONDS') )
  )`;
}
