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

import React from 'react';

class Workflow {

  id;
  startTime;
  endTime;
  node;

  /** list of fileNode objects */
  inputs;

  /** list of fileNode objects */
  outputs;

  /** list of processes, which were part of this workflow */
  processes;

  constructor(id) {
    // stores the inputs
    this.id = id;
    this.inputs = [];
    this.outputs = [];
    this.processes = [];
  }

  /**
   * Returns the array of input files for this workflow
   */
  getInputs() {
    return { inputs: this.inputs };
  }

  /**
   * Returns the array of processes for this workflow
   */
  getProcesses() {
    return { processes: this.processes };
  }

  /**
   * Returns a pretty-printed version of this comparison.
   */
  toProcessListHtml() {
    this.processes.sort((a, b) => (a.birthTime > b.birthTime) ? 1 : -1)

    var table = this.processes.map((p) => {
      return (<tr>
        <td><font size="1">{p.execCmdLine}</font></td>
        <td><font size="1">{p.execcwd}</font></td>
        <td><font size="1">{p.nodeName}</font></td>
      </tr>)
    });

    return (
      <div>
        <table className="bx--data-table-v2 bx--data-table-v2--zebra">
          <thead>
            <tr>
              <th>Processes</th>
              <th>CWD</th>
              <th>Node</th>
            </tr>
          </thead>
          <tbody>
            {table}
          </tbody>
        </table>
      </div>
    );
  }
}

/**
 * Class to represent a summary for the comparison of two
 * workflows, including the different files, different scripts,
 * versions etc.
 */
class WorkflowComparison {

  /** list of input files in workflow 1 but not in workflow 2 */
  inputsInW1;

  /** list of input files in workflow 2 but not in workflow 1 */
  inputsInW2;

  /** list of output files in workflow 1 */
  outputsInW1;

  /** list of output files in workflow 2 */
  outputsInW2;

  constructor() {
    this.inputsInW1 = [];
    this.inputsInW2 = [];
    this.outputsInW1 = [];
    this.outputsInW2 = [];
    this.inputsInW1W2 = [];
  }

  /**
   * Compares two workflows and populates the different fields
   * of the WorkflowComparison.
   */
  static compare(workflow1, workflow2) {
    var comparison = new WorkflowComparison();

    var w1Inputs = workflow1.inputs;
    var w2Inputs = workflow2.inputs;

    // fill inputsInW1
    for (let i = 0; i < w1Inputs.length; i++) {
      let w2Contains = false;
      let w2ContainsDiffVersion = false;
      for (let j = 0; j < w2Inputs.length; j++) {
        if (w1Inputs[i].path === w2Inputs[j].path &&
          w1Inputs[i].inode === w2Inputs[j].inode &&
          w1Inputs[i].version === w2Inputs[j].version) {
          w2Contains = true;
        } else if (w1Inputs[i].path === w2Inputs[j].path &&
          w1Inputs[i].inode === w2Inputs[j].inode &&
          w1Inputs[i].version !== w2Inputs[j].version) {
          w2ContainsDiffVersion = true
        }
      }
      if (!w2Contains) {
        if (w2ContainsDiffVersion) {
          comparison.inputsInW1.push({ file: w1Inputs[i], versiondiff: true });
        } else {
          comparison.inputsInW1.push({ file: w1Inputs[i], versiondiff: false });
        }
      } else {
        comparison.inputsInW1W2.push(w1Inputs[i]);
      }
    }

    // fill inputsInW2
    for (let i = 0; i < w2Inputs.length; i++) {
      let w1Contains = false;
      let w1ContainsDiffVersion = false;
      for (let j = 0; j < w1Inputs.length; j++) {
        if (w2Inputs[i].path === w1Inputs[j].path &&
          w2Inputs[i].inode === w1Inputs[j].inode &&
          w2Inputs[i].version === w1Inputs[j].version) {
          w1Contains = true;
        } else if (w2Inputs[i].path === w1Inputs[j].path &&
          w2Inputs[i].inode === w1Inputs[j].inode &&
          w2Inputs[i].version !== w1Inputs[j].version) {
          w1ContainsDiffVersion = true;
        }
      }
      if (!w1Contains) {
        if (w1ContainsDiffVersion) {
          comparison.inputsInW2.push({ file: w2Inputs[i], versiondiff: true });
        } else {
          comparison.inputsInW2.push({ file: w2Inputs[i], versiondiff: false });
        }
      }
    }

    // fill outputsInW1
    for (let i = 0; i < workflow1.outputs.length; i++) {
      comparison.outputsInW1.push(workflow1.outputs[i]);
    }

    // fill outputsInW2
    for (let i = 0; i < workflow2.outputs.length; i++) {
      comparison.outputsInW2.push(workflow2.outputs[i]);
    }

    return comparison;
  }

  /**
   * Returns a pretty-printed version of this comparison.
   */
  toHtml() {
    var inputsTable = this.createInputsTable();
    var sharedInputsTable = this.createSharedInputsTable();
    var outputsTable = this.createOutputsTable();

    return (
      <div>
        <table className="bx--data-table-v2 bx--data-table-v2--zebra">
          <thead>
            <tr>
              <th>Unique inputs in W1</th>
              <th>Unique inputs in W2 </th>
            </tr>
          </thead>
          <tbody>
            {inputsTable}
          </tbody>
        </table>

        <table className="bx--data-table-v2 bx--data-table-v2--zebra">
          <thead>
            <tr>
              <th>Shared inputs in W1</th>
              <th>Shared inputs in W2 </th>
            </tr>
          </thead>
          <tbody>
            {sharedInputsTable}
          </tbody>
        </table>

        <table className="bx--data-table-v2 bx--data-table-v2--zebra">
          <thead>
            <tr>
              <th>Outputs in W1</th>
              <th>Outputs in W2 </th>
            </tr>
          </thead>
          <tbody>
            {outputsTable}
          </tbody>
        </table>
      </div>
    );
  }

  /**
   * Creates a table of the inputs for both workflows in this
   * comparison.
   */
  createInputsTable() {
    var tableContents = [];

    var maxLength = this.inputsInW1.length;
    if (this.inputsInW1.length < this.inputsInW2.length) {
      maxLength = this.inputsInW2.length;
    }

    this.inputsInW1.sort(function (a, b) {
      return a.file.path > b.file.path;
    });
    this.inputsInW2.sort(function (a, b) {
      return a.file.path > b.file.path;
    });

    for (let i = 0; i < maxLength; i++) {
      var w1File = "";
      var w2File = "";
      var w1Color = "black";
      var w2Color = "black";
      if (this.inputsInW1.length > i) {
        w1File = this.inputsInW1[i].file.path;
        if (this.inputsInW1[i].versiondiff) {
          w1Color = "red";
        }
      }
      if (this.inputsInW2.length > i) {
        w2File = this.inputsInW2[i].file.path;
        if (this.inputsInW2[i].versiondiff) {
          w2Color = "red";
        }
      }

      tableContents.push({
        file1: w1File,
        file2: w2File,
        color1: w1Color,
        color2: w2Color
      });
    }

    var table = tableContents.map((files) => {
      return (<tr>
        <td><font size="1" color={files.color1}>{files.file1}</font></td><td><font size="1" color={files.color2}>{files.file2}</font></td>
      </tr>)
    });

    return table;
  }

  /**
   * Creates a table of the inputs for both workflows in this
   * comparison.
   */
  createSharedInputsTable() {
    var tableContents = [];

    this.inputsInW1W2.sort(function (a, b) {
      return a.path > b.path;
    });

    for (let i = 0; i < this.inputsInW1W2.length; i++) {
      var w1File = "";
      w1File = this.inputsInW1W2[i].path;

      tableContents.push({
        file1: w1File
      });
    }

    var table = tableContents.map((files) => {
      return (<tr>
        <td><font size="1" color="green">{files.file1}</font></td><td><font size="1" color="green">{files.file1}</font></td>
      </tr>)
    });

    return table;
  }

  /**
   * Creates a table of the inputs for both workflows in this
   * comparison.
   */
  createOutputsTable() {
    var tableContents = [];

    var maxLength = this.outputsInW1.length;
    if (this.outputsInW1.length < this.outputsInW2.length) {
      maxLength = this.outputsInW2.length;
    }

    for (let i = 0; i < maxLength; i++) {
      var w1File = "";
      var w2File = "";
      if (this.outputsInW1.length > i) {
        w1File = this.outputsInW1[i].path;
      }
      if (this.outputsInW2.length > i) {
        w2File = this.outputsInW2[i].path;
      }

      tableContents.push({
        file1: w1File,
        file2: w2File
      });
    }

    var table = tableContents.map((files) => {
      return (<tr>
        <td><font size="1">{files.file1}</font></td><td><font size="1">{files.file2}</font></td>
      </tr>)
    });

    return table;
  }

}

export default Workflow;
export {
  WorkflowComparison
};
