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

var ProvNodes = require('./provNodes');

const TARGET_URL = `${BACKEND_ROOT_URL}`;

export const PROCESS_USED_FILE_INTERACTION = "prov_processesThatInteractedWithFile";
export const FILE_USEDBY_PROCESS_INTERACTION = "prov_filesThatProcessInteractedWith";
export const IPC_INTERACTION = "prov_processesThatProcessInteractedWith";
export const TRUEIPC_INTERACTION = "prov_processesThatProcessTruelyInteractedWith";
export const NET_INTERACTION = "prov_processesThatProcessInteractedWithThroughNetwork";
export const PROCESS_LOGS = "prov_processLogs";
export const FILECONTENT = "file_conent";

//TODO This idea of 'cardinal' is skipping the abstraction of "readFrom"
//and "wroteTo" and jumping straight to visualization. This is inelegant.
export const cardinal = {
  north: 'north',
  south: 'south',
  east: 'east',
  west: 'west'
};

export const cardinalOpposite = {};
cardinalOpposite[cardinal.north] = cardinal.south;
cardinalOpposite[cardinal.south] = cardinal.north;
cardinalOpposite[cardinal.east] = cardinal.west;
cardinalOpposite[cardinal.west] = cardinal.east;

/**
 * Implementation of an assert function which
 * checks the specified condition and outputs
 * the msg in case the condition is false.
 */
export function assert(cond, msg) {
  if (!cond) {
    alert(`Assert failed: ${msg}`);
  }
};

/**
 * POST the specified request to the specified URL
 * using axios.
 */
export function _post(url, reqBody) {
  console.log(`Making post to ${url} with body: ${JSON.stringify(reqBody)}`);
  return axios.post(url, reqBody);
}

/**
 * Takes an interaction description and a subject and determines
 * the perceived directionality of the interaction.
 */
export function interactionDescToCardinal(record, subject) {
  let relativeLocationFromSubject = "";

  if (record.desc.write) {
    // this file is an output, plot to the east
    relativeLocationFromSubject = cardinal.east;
  } else if (record.desc.read || record.desc.observed || record.desc.deleted) {
    // this file is an input, plot to the west
    relativeLocationFromSubject = cardinal.west;
  } else {
    assert(false, `Error, indeterminate process-file interaction ${JSON.stringify(record.desc, null, 2)}`);
  }

  if (subject) {
    return relativeLocationFromSubject;
  } else {
    return cardinalOpposite[relativeLocationFromSubject];
  }
}

/**
 * Checks whether the passed object is an instance
 * of a provenance node as defined in provNodes.js.
 */
export function isProvNode(node) {
  if (!node || !node.data) {
    return false;
  }

  const nodeType = node.data('type');
  return nodeType &&
    (nodeType === ProvNodes.nodeTypeInitialFile ||
      nodeType === ProvNodes.nodeTypeFile ||
      nodeType === ProvNodes.nodeTypeProcess);
};

/**
 * Converts the passed node Pojo into its corresponding
 * Node object.
 */
export function toProvNodeObject(node) {
  const nodeType = node.data('type');

  if (nodeType === ProvNodes.nodeTypeInitialFile) {
    return new ProvNodes.InitialFileNode(node.data('path'));
  } else if (nodeType === ProvNodes.nodeTypeFile) {
    return new ProvNodes.FileNode(node.data('path'), node.data('inode'))
  } else if (nodeType === ProvNodes.nodeTypeProcess) {
    return new ProvNodes.ProcessNode(node.data('nodeName'),
      node.data('pid'), node.data('ppid'),
      node.data('birthTime'), node.data('deathTime'),
      node.data('pgid'), node.data('execCmdLine'));
  } else {
    console.log(`Unknown node type ${nodeType} encountered while trying to convert to ProvNode`);
  }
};

/**
 * Get the provenance for the specified prov node.
 * 
 * Returns a promise which is fulfilled with an array of provenance
 * information. Each entry is an object with keys: type records
 * The type indicates the provenance type of the records.
 * The records are from the backend.
 */
export function fetchProv(node) {
  // Route to the appropriate handler.
  const nodeType = node.type;
  if (nodeType === ProvNodes.nodeTypeInitialFile || nodeType === ProvNodes.nodeTypeFile) {
    return fetchProvForFileNode(node);
  } else if (nodeType === ProvNodes.nodeTypeProcess) {
    return fetchProvForProcessNode(node);
  } else if (nodeType === ProvNodes.nodeTypeInitialProcess) {
    return fetchProvForInitialProcessNode(node);
  } else {
    return Promise.reject(`Error, unexpected node type ${nodeType}`);
  }
}

/**
 * Get the process provenance for the fileNode node.
 *
 * Returns a promise fulfilled with an array of provenance pseudo-objects
 * with keys 'type' and 'records'.
 */
function fetchProvForFileNode(node) {
  let requestType;
  let params;

  // Build the request body based on the node type
  const nodeType = node.type;
  if (nodeType === ProvNodes.nodeTypeInitialFile) {
    requestType = "FILEPROV_BY_PATH";
    params = { path: node.path };
  } else if (nodeType === ProvNodes.nodeTypeFile) {
    requestType = "FILEPROV_BY_INODE";
    params = { inode: node.inode, path: node.path };
  } else {
    return Promise.reject(`Error, unexpected node type ${nodeType}`);
  }

  const reqBody = {
    requestType: requestType,
    params: params
  };

  return _post(TARGET_URL + "/provenance", reqBody)
    .then((response) => {
      // reformatting
      const nodeObjects = response.data.records.map(e => new ProvNodes.ProcessNode(e.nodename,
        e.pid, e.ppid, fixWeirdBackendTimestamp(e.birthtime), fixWeirdBackendTimestamp(e.deathtime),
        e.pgid, e.execcmdline, e.inode, e.path, e.dstpath, e.event, e.bytesread, e.byteswritten, fixWeirdBackendTimestamp(e.eventtime)));
      return {
        type: PROCESS_USED_FILE_INTERACTION,
        records: nodeObjects
      };
    })
    .then((provNode) => {
      return [provNode];
    });
}

/**
 * Get the FS and IPC provenance for the processNode node.
 * 
 * Returns a promise fulfilled with an array of provenance
 * pseudo-objects.
 */
function fetchProvForProcessNode(node) {
  // Submit concurrent requests for FS and IPC info.
  const fileProv = _post(TARGET_URL + "/provenance", {
    requestType: 'PROCESSPROV_FS_ACCESSES',
    params: {
      nodeName: node.nodeName,
      pid: node.pid,
      birthTime: node.birthTime,
      deathTime: node.deathTime
    }
  })
    .then((response) => {
      // reformatting
      const nodeObjects = response.data.records.map(e => new ProvNodes.FileNode(e.path,
        e.inode, e.event, e.bytesread, e.byteswritten, e.dstpath));
      return {
        type: FILE_USEDBY_PROCESS_INTERACTION,
        records: nodeObjects
      };
    });

  // create dummy promise
  var ipcProv = Promise.resolve({
    type: IPC_INTERACTION,
    records: []
  });
  var ipcisenabled = document.getElementById("bx--checkbox-new").checked;
  var trueipcisenabled = document.getElementById("bx--checkbox-trueipc").checked;
  // process group based IPC
  if (ipcisenabled) {
    ipcProv = _post(TARGET_URL + "/provenance", {
      requestType: 'PROCESSPROV_IPC',
      params: {
        nodeName: node.nodeName,
        pid: node.pid,
        birthTime: node.birthTime,
        deathTime: node.deathTime,
        pgid: node.pgid
      }
    })
      .then((response) => {
        // reformatting
        const nodeObjects = response.data.records.map(e => new ProvNodes.ProcessNode(e.nodename,
          e.pid, e.ppid, fixWeirdBackendTimestamp(e.birthtime), fixWeirdBackendTimestamp(e.deathtime),
          e.pgid, e.execcmdline, e.inode, undefined, undefined, e.event, e.bytesread, e.byteswritten));
        return {
          type: IPC_INTERACTION,
          records: nodeObjects
        };
      });
    // "true" IPC, i.e. through pipes, sockets, etc.
  } else if (trueipcisenabled) {
    ipcProv = _post(TARGET_URL + "/provenance", {
      requestType: 'PROCESSPROV_TRUEIPC',
      params: {
        nodeName: node.nodeName,
        pid: node.pid,
        birthTime: node.birthTime
      }
    })
      .then((response) => {
        // reformatting
        const nodeObjects = response.data.records.map(e => new ProvNodes.ProcessNode(e.nodename,
          e.pid, e.ppid, fixWeirdBackendTimestamp(e.birthtime), fixWeirdBackendTimestamp(e.deathtime),
          e.pgid, e.execcmdline, e.inode, undefined, undefined, e.event, -1, -1, undefined, "true"));
        return {
          type: TRUEIPC_INTERACTION,
          records: nodeObjects
        };
      });
  }

  // create dummy promise
  var netProv = Promise.resolve({
    type: NET_INTERACTION,
    records: []
  });
  var netprovisenabled = document.getElementById("bx--checkbox-netprov").checked;
  // process group based IPC
  if (netprovisenabled) {
    netProv = _post(TARGET_URL + "/provenance", {
      requestType: 'PROCESSPROV_NET',
      params: {
        nodeName: node.nodeName,
        pid: node.pid,
        birthTime: node.birthTime,
        deathTime: node.deathTime,
        pgid: node.pgid
      }
    })
      .then((response) => {
        // reformatting
        const nodeObjects = response.data.records.map(e => new ProvNodes.ProcessNode(e.nodename,
          e.pid, e.ppid, fixWeirdBackendTimestamp(e.birthtime), fixWeirdBackendTimestamp(e.deathtime),
          e.pgid, e.execcmdline, e.inode, undefined, undefined, e.event, -1, -1, undefined, "true"));
        return {
          type: NET_INTERACTION,
          records: nodeObjects
        };
      });
  }

  // Fulfilled when all are done
  return Promise.all([fileProv, ipcProv, netProv])
    .then((provResults) => {
      return provResults;
    });
}

/**
 * Get the process provenance for the specified process node.
 *
 * Returns a promise fulfilled with an array of provenance
 * pseudo-objects.
 */
function fetchProvForInitialProcessNode(node) {
  const reqBody = {
    requestType: "PROCESSPROV_INITIAL",
    params: { processName: node.name }
  };

  return _post(TARGET_URL + "/provenance", reqBody)
    .then((response) => {
      // reformatting
      const nodeObjects = response.data.records.map(e => new ProvNodes.ProcessNode(e.nodename,
        e.pid, e.ppid, fixWeirdBackendTimestamp(e.birthtime), fixWeirdBackendTimestamp(e.deathtime),
        e.pgid, e.execcmdline, -1));
      return {
        type: PROCESS_USED_FILE_INTERACTION,
        records: nodeObjects
      };
    })
    .then((provNode) => {
      return [provNode];
    });
}

/**
 * Get any associated logs for the specified process.
 *
 * Returns a promise fulfilled with an array of log entries.
 */
export function fetchLogsForProcessNode(node) {
  const reqBody = {
    requestType: "PROCESSLOGS",
    params: {
      nodeName: node.nodeName,
      ppid: node.ppid,
      pid: node.pid,
      birthTime: node.birthTime,
      deathTime: node.deathTime
    }
  };

  return _post(TARGET_URL + "/provenance", reqBody)
    .then((response) => {
      return {
        type: PROCESS_LOGS,
        records: response.data.records
      };
    });
}

/**
 * Get any associated logs for the specified process.
 *
 * Returns a promise fulfilled with an array of log entries.
 */
export function fetchContentForFile(fileNode, procNode) {
  const reqBody = {
    requestType: "FILECOMMITID",
    params: {
      nodename: procNode.nodeName,
      path: fileNode.path,
      inode: fileNode.inode,
      eventtime: procNode.eventTime
    }
  };

  return _post(TARGET_URL + "/provenance", reqBody)
    .then((response) => {
      if (response.data.records.length > 1) {
        alert(`Found more than 1 commitid for ${JSON.stringify(procNode)}.
         That shouldn't happen... Taking the first one."`);
      } else if (response.data.records.length === 0) {
        return {
          type: FILECONTENT,
          filecontent: ""
        };
      }
      const contentReqBody = {
        requestType: "FILECONTENT",
        params: {
          commitid: response.data.records[0].commitid,
          path: fileNode.inode
        }
      };
      return _post(TARGET_URL + "/provenance/filecontent", contentReqBody).then((response) => {
        return {
          type: FILECONTENT,
          filecontent: response.data.filecontent
        }
      });
    });
}

/**
 * Calculate x position for a new node based on current
 * x position and relative location.
 */
export function calcXPos(x, d_x, relativeLocation) {
  console.log(`calcXPos: x ${x} d_x ${d_x} relativeLocation ${relativeLocation}`);

  if (relativeLocation === cardinal.west) {
    return x - d_x;
  } else if (relativeLocation === cardinal.east) {
    return x + d_x;
  } else {
    return x;
  }
}

/**
 * Calculate y position for a new node based on current
 * y position and relative location.
 */
export function calcYPos(y, d_y, relativeLocation) {
  console.log(`calcYPos: y ${y} d_y ${d_y} relativeLocation ${relativeLocation}`);

  if (relativeLocation === cardinal.sourth) {
    return y - d_y;
  } else if (relativeLocation === cardinal.north) {
    return y + d_y;
  } else {
    return y;
  }
}

/**
 * Calculate distance between nodes.
 */
export function calcYStepSize(nNodes) {
  return (nNodes === 1) ? 0 : (nNodes * 100) / (nNodes - 1);
}

/**
 * The DB2 REST endpoint returns weird timestamps.
 * Conversion:
 *   2018-07-26T23:14:24.577Z
 *    ->
 *   2018-07-26 23:14:24.577
 */
function fixWeirdBackendTimestamp(ts) {
  return ts
    .replace(/(\d+)T(\d+)/, "$1 $2")
    .replace(/(\d+)Z$/, "$1");
}
