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

const NODE_TYPE_INITIAL_FILE = "FILE_WITH_NAME";
const NODE_TYPE_FILE = "FILE_WITH_INODE";
const NODE_TYPE_INITIAL_PROCESS = "PROCESS_WITH_CMDLINE";
const NODE_TYPE_PROCESS = "PROCESS";

/**
 * Determines the interaction of the record. Records can either be 
 * from a FILEPROV_BY_PATH or PROCESSPROV_FS_ACCESSES and can
 * have read, write, rename, delete, and observed interactions.
 */
function getProcessFileInteraction(event, bytesRead, bytesWritten) {
  let desc = {
    read: false,
    write: false,
    observed: false,
    deleted: false,
    renamed: false
  };

  if (event === 'READ') {
    desc.read = true;
  } else if (event === 'WRITE' || event === 'CREATE') {
    desc.write = true;
  } else if (event === 'CLOSE') {
    if (0 < bytesRead) {
      desc.read = true;
    }
    if (0 < bytesWritten) {
      desc.write = true;
    }
    if (bytesRead <= 0 && bytesWritten <= 0) {
      desc.observed = true;
    }
  } else if (event === 'UNLINK') {
    desc.deleted = true;
  } else if (event === 'RENAME') {
    desc.renamed = true;
  }

  return desc;
}

/**
 * Determines the interaction of the record. Records can be
 * PROCESSPROV_TRUEIPC records and have read, write interactions.
 */
function getProcessProcessInteraction(event) {
  let desc = {
    read: false,
    write: false
  };

  if (event === 'read') {
    desc.read = true;
  } else if (event === 'write') {
    desc.write = true;
  }

  return desc;
}

class Node {

  constructor() {
    // this structure stores for each node, what type of interaction
    // it experienced
    this.desc = {
      read: false,
      write: false,
      observed: false,
      deleted: false,
      renamed: false
    };
    this.position = {
      x: 0,
      y: 0
    };
  }

  toCyNode() {
    throw new Error('Implementation required!');
  }

  getLabel() {
    throw new Error('Implementation required!');
  }

  isProvNode() {
    return true;
  }

  getDescString() {
    let result = '';
    if (this.desc.read) result += 'read';
    if (this.desc.write) result += 'write';
    if (this.desc.observed) result += 'observed';
    if (this.desc.deleted) result += 'deleted';
    if (this.desc.renamed) result += 'renamed';
    return result;
  }
}

class InitialFileNode extends Node {

  constructor(path) {
    super();

    this.path = path;
    this.inode = -1;
    this.style = {
      'shape': 'ellipse',
      'background-color': '#61bffc',
      'border-color': '#123456',
      'border-width': 8,
      'border-opacity': 0.5
    }
    this.type = NODE_TYPE_INITIAL_FILE;
    this.id = `${path} (inode -1)`;
  }

  toCyNode() {
    return {
      path: this.path,
      type: this.type,
      id: this.id
    }
  }

  getLabel() {
    return `${this.path} (inode ${this.inode})`
  }

}

class FileNode extends Node {

  constructor(path, inode, event = undefined, bytesRead = -1, bytesWritten = -1, dstpath = undefined) {
    super();

    this.path = path;
    this.inode = inode;
    this.dstpath = dstpath;
    this.style = {
      'shape': 'ellipse',
      'background-color': '#61bffc'
    }
    this.type = NODE_TYPE_FILE;
    this.id = `${path} (inode ${inode})`;

    if (event) {
      // determine this file nodes interaction
      this.desc = getProcessFileInteraction(event, bytesRead, bytesWritten);
    }

    this.version = "";
  }

  toCyNode() {
    return {
      path: this.path,
      inode: this.inode,
      type: this.type,
      id: this.id
    }
  }

  getLabel() {
    return `${this.path} (inode ${this.inode})`
  }

}

class InitialProcessNode extends Node {

  constructor(name) {
    super();

    this.name = name;
    this.inode = -1;
    this.style = {
      'shape': 'rectangle',
      'background-color': '#ff0000'
    }
    this.type = NODE_TYPE_INITIAL_PROCESS;
    // ID might not be unique but that's ok as
    // InitialProcessNodes should never be displayed
    // in the provenance graph
    this.id = `${name}`;
  }

  toCyNode() {
    return {
      name: this.name,
      id: this.id
    }

  }

  getLabel() {
    return `${this.name}`
  }

}

class ProcessNode extends Node {

  constructor(nodeName, pid, ppid, birthTime, deathTime, pgid, execCmdLine, inode,
    path = undefined, dstpath = undefined, event = undefined, bytesRead = -1, bytesWritten = -1,
    eventTime = undefined, isIPC = undefined, execcwd = undefined) {
    super();

    this.nodeName = nodeName;
    this.pid = pid;
    this.ppid = ppid;
    this.pgid = pgid;
    this.birthTime = birthTime;
    this.deathTime = deathTime;
    this.execCmdLine = execCmdLine;
    this.inode = inode;
    this.path = path;
    this.dstpath = dstpath;
    if (eventTime) {
      this.eventTime = eventTime;
    }
    if (execcwd) {
      this.execcwd = execcwd;
    }
    this.style = {
      'shape': 'rectangle',
      'background-color': '#ff0000'
    };
    this.type = NODE_TYPE_PROCESS;
    this.id = `${execCmdLine} - ${nodeName} pid ${pid} birth ${birthTime}`;

    if (event) {
      if (isIPC) {
        // determine this process nodes interaction with the other process
        this.desc = getProcessProcessInteraction(event);
      } else {
        // determine this process nodes interaction with files
        this.desc = getProcessFileInteraction(event, bytesRead, bytesWritten);
      }
      this.event = event;
    }
  }

  toCyNode() {
    return {
      nodeName: this.nodeName,
      pid: this.pid,
      ppid: this.ppid,
      pgid: this.pgid,
      birthTime: this.birthTime,
      deathTime: this.deathTime,
      execCmdLine: this.execCmdLine,
      eventTime: this.eventTime,
      type: this.type,
      id: this.id
    }
  }

  getLabel() {
    return `${this.execCmdLine} - ${this.nodeName} pid ${this.pid} birth ${this.birthTime}`;
  }

}

module.exports = {
  Node: Node,
  InitialFileNode: InitialFileNode,
  FileNode: FileNode,
  InitialProcessNode: InitialProcessNode,
  ProcessNode: ProcessNode,
  nodeTypeInitialFile: NODE_TYPE_INITIAL_FILE,
  nodeTypeFile: NODE_TYPE_FILE,
  nodeTypeInitialProcess: NODE_TYPE_INITIAL_PROCESS,
  nodeTypeProcess: NODE_TYPE_PROCESS
}
