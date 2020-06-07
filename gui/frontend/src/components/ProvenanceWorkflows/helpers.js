/********************************************************* {COPYRIGHT-TOP} ***
* Licensed Materials - Property of IBM
* MetaOcean
*
* (C) Copyright IBM Corp. 2017
*
* US Government Users Restricted Rights - Use, duplication, or
* disclosure restricted by GSA ADP Schedule Contract with IBM Corp.
********************************************************** {COPYRIGHT-END} **/
//helper functions for displaying provenance graph

import axios from 'axios';
import { BACKEND_ROOT_URL } from '../../constants'

var ProvNodes = require('./provNodes');

const TARGET_URL = `${BACKEND_ROOT_URL}`;

/**
 *  Retrieve the workflows from the database.
 */
function fetchWorkflows() {
  console.log(`fetchWorkflows`);

  const requestType = "WORKFLOWS"
  const params = {};
  const reqBody = {
    requestType: requestType
    , params: params
  };

  return _post(TARGET_URL + "/provenance", reqBody)
    .then((response) => {
      // reformatting
      return {
        type: 'prov_workflows'
        , records: response.data.records
      };
    })
    .then((workflows) => {
      return [workflows];
    });
}

/**
 * Retrieve the files written for the last stage of a specific
 * workflow.
 */
function fetchWorkflowOutputFiles(workflowId, starttime, finishtime) {
  console.log(`fetchWorkflowWrittenFiles`);

  const requestType = "WORKFLOW_OUTPUT_FILES"
  const params = {
    wid: workflowId,
    wstart: starttime,
    wfinish: finishtime
  };
  const reqBody = {
    requestType: requestType
    , params: params
  };

  return _post(TARGET_URL + "/provenance", reqBody)
    .then((response) => {
      // reformatting
      const nodeObjects = response.data.records.map(e => new ProvNodes.FileNode(e.path,
        e.inode));
      return {
        type: 'prov_workflow_output_files',
        records: nodeObjects
      };
    });
}

/**
 * Retrieve the (potential workflows) which wrote the specified output file.
 */
function fetchOutputFilesWorkflow(filePath, fileInode) {
  console.log(`fetchWorkflowWrittenFiles`);

  const requestType = "OUTPUT_FILES_WORKFLOW"
  const params = {
    path: filePath,
    inode: fileInode
  };
  const reqBody = {
    requestType: requestType
    , params: params
  };

  return _post(TARGET_URL + "/provenance", reqBody)
    .then((response) => {
      // reformatting
      const workflowObjects = response.data.records.map(e => {
        return {
          name: e.name,
          def1: e.definition1,
          def2: e.definition2
        }
      });
      return {
        type: 'prov_output_file_workflow',
        records: workflowObjects
      };
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
function convertTime(unixTimestamp) {
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

export default fetchWorkflows;
export {
  fetchWorkflowOutputFiles,
  fetchOutputFilesWorkflow,
  convertTime
};
