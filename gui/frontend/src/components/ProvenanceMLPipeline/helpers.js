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

import axios from 'axios';
import { BACKEND_ROOT_URL } from '../../constants'
import Workflow from './workflows';

var ProvNodes = require('../ProvenanceGraph/provNodes');
const assert = require('assert').strict;

const TARGET_URL = `${BACKEND_ROOT_URL}`;

export class PriorityQueue {

  constructor() {
    this.heap = [null]
  }

  insert(value, pri) {
    const entry = {
      val: value,
      priority: pri
    };
    this.heap.push(entry);

    let currIdx = this.heap.length - 1;
    let currParentIdx = Math.floor(currIdx / 2);

    while (this.heap[currParentIdx] && entry.priority > this.heap[currParentIdx].priority) {
      const parent = this.heap[currParentIdx];
      this.heap[currParentIdx] = entry;
      this.heap[currIdx] = parent;
      currIdx = currParentIdx;
      currParentIdx = Math.floor(currIdx / 2);
    }
  }

  remove() {
    if (this.heap.length === 0) {
      // that shouldn't happen
      assert("Heap length of 0?");
    }
    if (this.heap.length === 1) {
      return null;
    }
    if (this.heap.length === 2) {
      return this.heap.pop();
    }
    const result = this.heap[1];
    this.heap[1] = this.heap.pop();
    var currIdx = 1;
    var [left, right] = [2 * currIdx, 2 * currIdx + 1];
    var currChildIdx = (this.heap[right] && this.heap[right].priority >= this.heap[left].priority) ? right : left;

    while (this.heap[currChildIdx] && this.heap[currIdx].priority <= this.heap[currChildIdx].priority) {
      let currEntry = this.heap[currIdx]
      let currChild = this.heap[currChildIdx];
      this.heap[currChildIdx] = currEntry;
      this.heap[currIdx] = currChild;

      currIdx = currChildIdx;
      [left, right] = [2 * currIdx, 2 * currIdx + 1];
      if (!this.heap[left] && !this.heap[right]) {
        break;
      } else {
        currChildIdx = (this.heap[right] && this.heap[right].priority >= this.heap[left].priority) ? right : left;
      }
    }

    return result;
  }

  length() {
    return this.heap.length - 1;
  }
}

/**
 * Retrieve the processes for the specified regex.
 */
export function fetchJobProcesses(jobDescription) {
  console.log(`fetchJobProcesses`);

  const requestType = "WORKFLOWS_PROCESSES"
  const params = {
    job: jobDescription,
  };
  const reqBody = {
    requestType: requestType
    , params: params
  };

  return _post(TARGET_URL + "/provenance", reqBody)
    .then((response) => {
      // reformatting
      return {
        type: 'prov_workflows',
        records: response.data.records
      };
    })
    .then((workflows) => {
      return [workflows];
    });
}

/**
 * Finds and returns the output files for the given workflow.
 */
export function fillWorkflowOutputs(workflow) {
  console.log(`fillWorkflowOutputs`);

  const requestType = "PROCESSPROV_FS_ACCESSES"
  const params = {
    pid: workflow.id,
    birthTime: workflow.startTime,
    deathTime: workflow.endTime,
    nodeName: workflow.node,
    onlyWrite: true
  };
  const reqBody = {
    requestType: requestType,
    params: params
  };
  return _post(TARGET_URL + "/provenance", reqBody).then((outputFiles) => {
    outputFiles.data.records.map(e => workflow.outputs.push(e));
    return workflow;
  });

}

/**
 * Creates a new Workflow object instance (from workflows.js) and
 * fills the attributes based on the specified workflowId.
 */
export function fillWorkflow(workflowId, startTime, endTime, nodeName, cmdLine) {
  console.log(`fillWorkflow`);

  // start BFS
  var processNodesVisited = new Map();
  var fileNodesVisited = new Map();
  var processNodesToVisit = new PriorityQueue();
  var fileNodesToVisit = new PriorityQueue();

  // initialize start node
  var startNode = new ProvNodes.ProcessNode(nodeName, workflowId, undefined, startTime,
    endTime, undefined, cmdLine, undefined);
  var startConstraint = "2100-02-01 08:00:01.000";
  var start = {
    node: startNode,
    constraint: startConstraint
  }
  processNodesToVisit.insert(start, startConstraint);
  return visitProcessNodes(workflowId, processNodesVisited, fileNodesVisited, processNodesToVisit, fileNodesToVisit)
    .then((visitedNodes) => {
      var workflow = new Workflow(workflowId);
      visitedNodes[0].forEach((value, key, map) => {
        workflow.inputs.push(value);
      });
      visitedNodes[1].forEach((value, key, map) => {
        workflow.processes.push(value);
      });
      workflow.startTime = startTime;
      workflow.endTime = endTime;
      workflow.node = nodeName;
      return workflow;
    });
}

/**
 * Creates a new Workflow object instance (from workflows.js) and
 * fills the attributes based on the specified file object.
 */
export function fillWorkflowForFile(fileNode) {
  console.log(`fillWorkflow`);

  // start BFS
  var processNodesVisited = new Map();
  var fileNodesVisited = new Map();
  var processNodesToVisit = new PriorityQueue();
  var fileNodesToVisit = new PriorityQueue();

  // initialize start node
  var startNode = fileNode;
  var startConstraint = "2100-02-01 08:00:01.000";
  var start = {
    node: startNode,
    constraint: startConstraint
  }
  fileNodesToVisit.insert(start, startConstraint);
  return visitFileNodes(undefined, processNodesVisited, fileNodesVisited, processNodesToVisit, fileNodesToVisit)
    .then((visitedNodes) => {
      var workflow = new Workflow(undefined);
      visitedNodes[0].forEach((value, key, map) => {
        workflow.inputs.push(value);
      });
      visitedNodes[1].forEach((value, key, map) => {
        workflow.processes.push(value);
      });
      workflow.startTime = undefined;
      workflow.endTime = undefined;
      workflow.node = undefined;
      return workflow;
    });
}

/**
 * Recursion entry point for finding the neighbors of all process nodes
 * specified in processNodesToVisit.
 */
function visitProcessNodes(workflowId, processNodesVisited, fileNodesVisited, processNodesToVisit, fileNodesToVisit) {
  console.log(`[${workflowId}] Visting process nodes ${processNodesToVisit.length()}`);
  var processVisitsPromises = [];

  while (processNodesToVisit.length() > 0) {
    // check if we have visited the current process node before
    var currentEntry = processNodesToVisit.remove().val;
    let currentNode = currentEntry.node;
    let nodeId = currentNode.pid + currentNode.birthTime + currentNode.deathTime + currentNode.nodeName;
    if (processNodesVisited.has(nodeId)) {
      continue;
    }

    // if not, mark as visited and get all read dependencies
    processNodesVisited.set(nodeId, currentNode);

    const requestType = "PROCESSPROV_FS_ACCESSES"
    const params = {
      pid: currentNode.pid,
      birthTime: currentNode.birthTime,
      deathTime: currentNode.deathTime,
      nodeName: currentNode.nodeName,
      onlyRead: true,
      dateConstraint: currentEntry.constraint
    };
    const reqBody = {
      requestType: requestType,
      params: params
    };
    let processVisitPromise = _post(TARGET_URL + "/provenance", reqBody);
    processVisitsPromises.push(processVisitPromise);
  }

  // if we're not visiting any process nodes, we're done and return
  if (processVisitsPromises.length === 0) {
    return [fileNodesVisited, processNodesVisited];
  }

  return Promise.all(processVisitsPromises).then((visitsPerPromise) => {
    visitsPerPromise.forEach((processVisit) => {
      const fileDependencies = processVisit.data.records.map(e => {
        const next = {};
        next["node"] = new ProvNodes.FileNode(e.path, e.inode, e.event, e.bytesread, e.byteswritten, e.dstpath);
        next["constraint"] = fixWeirdBackendTimestamp(e.eventtime);
        return next;
      });
      for (let i = 0; i < fileDependencies.length; i++) {
        fileNodesToVisit.insert(fileDependencies[i], fileDependencies[i].constraint);
      }
    });
    // if there are no new files to visit after visiting all processes in the queue, break the recursion
    if (fileNodesToVisit.length() === 0) {
      return [fileNodesVisited, processNodesVisited];
    } else {
      return visitFileNodes(workflowId, processNodesVisited, fileNodesVisited, processNodesToVisit, fileNodesToVisit);
    }
  });
}

/**
 * Recursion entry point for finding the neighbors of all file nodes
 * specified in fileNodesToVisit.
 */
function visitFileNodes(workflowId, processNodesVisited, fileNodesVisited, processNodesToVisit, fileNodesToVisit) {
  console.log(`[${workflowId}] Visting file nodes ${fileNodesToVisit.length()}`);
  var fileVisitsPromises = [];

  while (fileNodesToVisit.length() > 0) {
    // check if we have visited the current process node before
    var currentEntry = fileNodesToVisit.remove().val;
    let currentNode = currentEntry.node;
    let nodeId = currentNode.path + currentNode.inode
    if (fileNodesVisited.has(nodeId)) {
      continue;
    }

    // if not, mark as visited and get all read dependencies
    fileNodesVisited.set(nodeId, currentNode);

    const requestType = "FILEPROV_BY_INODE"
    const params = {
      inode: currentNode.inode,
      path: currentNode.path,
      onlyWrite: true,
      dateConstraint: currentEntry.constraint
    };
    const reqBody = {
      requestType: requestType,
      params: params
    };
    let fileVisitPromise = _post(TARGET_URL + "/provenance", reqBody);
    fileVisitsPromises.push(fileVisitPromise);
  }

  // if we're not visiting any file nodes, we're done and return
  if (fileVisitsPromises.length === 0) {
    return [fileNodesVisited, processNodesVisited];
  }

  return Promise.all(fileVisitsPromises).then((visitsPerPromise) => {
    visitsPerPromise.forEach((fileVisit) => {
      const processDependencies = fileVisit.data.records.map(e => {
        const next = {};
        next["node"] = new ProvNodes.ProcessNode(e.nodename,
          e.pid, e.ppid, fixWeirdBackendTimestamp(e.birthtime), fixWeirdBackendTimestamp(e.deathtime),
          e.pgid, e.execcmdline, e.inode, e.path, e.dstpath, e.event, e.bytesread, e.byteswritten,
          undefined, undefined, e.execcwd);
        next["constraint"] = fixWeirdBackendTimestamp(e.eventtime);
        next["fileVersion"] = e.version;
        return next;
      });
      var fileId = fileVisit.data.request.params.path.replace(/(^')|('$)/g, '') +
        fileVisit.data.request.params.inode.replace(/(^')|('$)/g, '');
      var maxDate = "1970-01-01 00:00:00.000";
      var latest = "";
      for (var i = 0; i < processDependencies.length; i++) {
        if (processDependencies[i].constraint > maxDate) {
          maxDate = processDependencies[i].constraint;
          latest = processDependencies[i].fileVersion;
        }
      }
      var target = fileNodesVisited.get(fileId);
      target.version = latest;

      for (let i = 0; i < processDependencies.length; i++) {
        processNodesToVisit.insert(processDependencies[i], processDependencies[i].constraint);
      }
    });
    // if there are no new processes to visit after visiting all files in the queue, break the recursion
    if (processNodesToVisit.length() === 0) {
      return [fileNodesVisited, processNodesVisited];
    } else {
      return visitProcessNodes(workflowId, processNodesVisited, fileNodesVisited, processNodesToVisit, fileNodesToVisit);
    }
  });
}

/**
 * Returns promise fulfilled with the result of this POST.
 */
function _post(url, reqBody) {
  //console.log(`Making post to ${url} with body: ${JSON.stringify(reqBody)}`);
  return axios.post(url, reqBody);
}

/**
 * Convert a unixTimestamp to a UTC date. The timestamp is expected to
 * include milliseconds.
 */
export function convertTime(unixTimestamp) {
  if (!unixTimestamp) {
    return "N/A";
  }

  var a;
  var isnum = /^\d+$/.test(unixTimestamp);
  if (isnum) {
    a = new Date(parseInt(unixTimestamp));
  } else {
    a = new Date(unixTimestamp);
  }

  var year = a.getUTCFullYear();
  var month = a.getUTCMonth() + 1;
  var date = a.getUTCDate();
  var hour = a.getUTCHours();
  var min = a.getUTCMinutes() < 10 ? '0' + a.getUTCMinutes() : a.getUTCMinutes();
  var sec = a.getUTCSeconds() < 10 ? '0' + a.getUTCSeconds() : a.getUTCSeconds();
  var msec = a.getUTCMilliseconds();
  var time = year + '-' + month + '-' + date + ' ' + hour + ':' + min + ':' + sec + "." + msec;
  return time;
}

/**
 * The DB2 REST endpoint returns weird timestamps.
 * Conversion: 2018-07-26T23:14:24.577Z -> 2018-07-26 23:14:24.577
 */
export function fixWeirdBackendTimestamp(ts) {
  return ts
    .replace(/(\d+)T(\d+)/, "$1 $2")
    .replace(/(\d+)Z$/, "$1");
}
