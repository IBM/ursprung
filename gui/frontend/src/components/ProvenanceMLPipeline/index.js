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

import React, { Component } from 'react';
import { withRouter } from 'react-router-dom'
import Button from 'react-bootstrap/Button'
import Modal from 'react-bootstrap/Modal'
import Table from 'react-bootstrap/Table'
import Form from 'react-bootstrap/Form'
import { Container, Row, Col } from 'react-bootstrap';
import './provenance-ml-pipeline.css';
import logo from "./assets/5.gif"
import { fetchJobProcesses, fillWorkflow, fillWorkflowOutputs, fixWeirdBackendTimestamp } from './helpers';

var ProvNodes = require('../ProvenanceGraph/provNodes');
var Workflows = require('./workflows');

class ProvenanceMLPipeline extends Component {

  constructor(props) {
    super(props);
    this.state = {
      workflowItems: {},
      checkedBoxes: [],
      modalContent: <img src={logo} alt="loading..." />,
      rerunModalContent: <img src={logo} alt="loading..." />,
      showComparisonModal: false,
      showRerunModal: false
    };
    this.componentDidMount = this.componentDidMount.bind(this);
    this.onSelectionChanged = this.onSelectionChanged.bind(this);
    this.onCheckboxChanged = this.onCheckboxChanged.bind(this);
  }

  /**
   * On mount, fetch the workflows from the database and set
   * the internal state. The state is later used to display the
   * retrieved result in a table.
   */
  componentDidMount() {
    console.log(`Component mounted`);
  }

  /**
   * Change handler for radio buttons in the table. The handler
   * sets the selected value as the component's state.
   */
  onSelectionChanged(e) {
    this.setState({
      selectedWorkflow: e.currentTarget.value
    })
  }

  /**
   * Change handler for check boxes buttons in the table. The handler
   * adds the selected checkbox to the list of workflows to compare.
   */
  onCheckboxChanged(e) {
    const checked = this.state.checkedBoxes;
    let index;

    if (e.target.checked) {
      checked.push(e.target.value);
    } else {
      index = checked.indexOf(e.target.value);
      checked.splice(index, 1);
    }

    this.setState({ checkedBoxes: checked });
  }

  /**
   * Compares the selected workflows in terms of a variety of
   * properties (input files, software versions, etc.).
   */
  compareWorkflows() {
    const checked = this.state.checkedBoxes;
    if (checked.length < 2 || checked.length > 2) {
      alert(`Can only compare two workflows to each other`);
      return;
    }
    console.log(`Compare Workflows`);
    var workflowPromises = [];

    for (let i = 0; i < checked.length; i++) {
      let ids = checked[i].split(";");
      let workflowId = ids[0];
      const startTime = document.getElementById(workflowId + 'starttime').innerHTML;
      const endTime = document.getElementById(workflowId + 'endtime').innerHTML;
      // TODO the owner field is reused to store the nodename
      // the nodename should be stored explicitely in the table
      const owner = document.getElementById(workflowId + 'owner').innerHTML;
      let workflowPromise = fillWorkflow(workflowId, startTime, endTime, owner, undefined);
      workflowPromises.push(workflowPromise);
    }

    return Promise.all(workflowPromises).then((workflowsPerPromise) => {
      var workflowOutputPromises = [];
      workflowsPerPromise.forEach((workflow) => {
        let workflowOutputPromise = fillWorkflowOutputs(workflow);
        workflowOutputPromises.push(workflowOutputPromise);
      });
      return Promise.all(workflowOutputPromises);
    });
  }

  /**
   * Produces a list of commands which are required to
   * rerun the selected workflow.
   */
  rerunWorkflow() {
    const checked = this.state.checkedBoxes;
    if (checked.length !== 1) {
      alert(`Need to select a single workflow to rerun`);
      return;
    }
    console.log(`Rerun Workflow`);

    let ids = checked[0].split(";");
    let workflowId = ids[0];
    const startTime = document.getElementById(workflowId + 'starttime').innerHTML;
    const endTime = document.getElementById(workflowId + 'endtime').innerHTML;
    // TODO the owner field is reused to store the nodename
    // the nodename should be stored explicitely in the table
    const owner = document.getElementById(workflowId + 'owner').innerHTML;
    const cmdLine = document.getElementById(workflowId + 'name').innerHTML;
    let workflowPromise = fillWorkflow(workflowId, startTime, endTime, owner, cmdLine);

    return workflowPromise.then((workflow) => {
      return workflow
    });
  }

  render() {
    console.log(`Rendering displayProvenanceWorkflows`);

    if (Object.keys(this.state.workflowItems).length === 0) {
      console.log("No state set yet");
    } else {
      var tableElements = this.state.workflowItems.reduce((elements, item) => {
        elements.push(<tbody>
          {item.records.map((record) => {
            return (<tr>
              <td>{record.id}</td>
              <td id={record.id + 'name'}>{record.name}</td>
              <td id={record.id + 'starttime'}>{fixWeirdBackendTimestamp(record.starttime)}</td>
              <td id={record.id + 'endtime'}>{fixWeirdBackendTimestamp(record.endtime)}</td>
              <td id={record.id + 'owner'}>{record.owner}</td>
              <td>{record.exitcode}</td>
              <td><input id={record.id + 'compare'} type="checkbox"
                value={record.id + ';' + record.ppid + ';' + record.pgid}
                name="checkbox" onChange={this.onCheckboxChanged} /></td>
            </tr>)
          })}
        </tbody>);
        return elements;
      }, []);
    }

    return (
      <Container fluid>
        <Row>
          <Col>
            <Button className="navbutton" id="show_provenance_button"
              type="button" block onClick={(e) => {
                e.preventDefault();

                var mlWorkflowProcesses = [];
                const checked = this.state.checkedBoxes;

                for (let i = 0; i < checked.length; i++) {
                  let ids = checked[i].split(";");
                  let workflowId = ids[0];
                  let ppid = ids[1];
                  let pgid = ids[2];
                  let startTime = document.getElementById(workflowId + 'starttime').innerHTML;
                  let endTime = document.getElementById(workflowId + 'endtime').innerHTML;
                  let nodeName = document.getElementById(workflowId + 'owner').innerHTML;
                  let cmdLine = document.getElementById(workflowId + 'name').innerHTML;
                  let procNode = new ProvNodes.ProcessNode(nodeName, workflowId, ppid, startTime,
                    endTime, pgid, cmdLine, undefined);
                  mlWorkflowProcesses.push(procNode);
                }

                this.props.callbackToParent({
                  provStartPath: "",
                  workflowOutputFiles: [],
                  mlWorkflowProcess: mlWorkflowProcesses
                });
                this.props.history.push('/graph');
              }}>
              Show Provenance
            </Button>
            
            <Button className="navbutton" id="compare_workflows_button"
              type="button" block onClick={(e) => {
                e.preventDefault();
                this.setState({ modalContent: "Waiting...", showComparisonModal: true });

                var workflows = [];
                this.compareWorkflows().then((workflowsPerPromise) => {
                  workflowsPerPromise.forEach((workflow) => {
                    workflows.push(workflow);
                  });
                  // this should not happen as long as we have the condition in compareWorkflows
                  if (workflows.length !== 2) {
                    alert(`Expected 2 workflows in comparison, instead received ${workflows.length}.
                              Won't compare results.`);
                    return;
                  }
                  var comparison = Workflows.WorkflowComparison.compare(workflows[0], workflows[1]);
                  this.setState({ modalContent: comparison.toHtml() });
                });
              }}>
              Compare Workflows
            </Button>
            
            <Button className="navbutton" id="rerun_workflow_button"
              type="button" block onClick={(e) => {
                e.preventDefault();
                this.setState({ rerunModalContent: "Waiting...", showRerunModal: true });

                this.rerunWorkflow().then((w) => {
                  this.setState({ rerunModalContent: w.toProcessListHtml() });
                });
              }} >
              Rerun Workflow
            </Button>

            <Modal show={this.state.showRerunModal}>
              <Modal.Header closeButton>
                <Modal.Title>Comparison Summary</Modal.Title>
              </Modal.Header>

              <Modal.Body>
                <p>{this.state.modalContent}</p>
              </Modal.Body>

              <Modal.Footer>
                <Button variant="secondary" onClick={(e) => {
                  e.preventDefault();
                  this.setState({ modalContent: "Waiting...", showModalContent: false });
                }}>Close</Button>
              </Modal.Footer>
            </Modal>

            <Modal show={this.state.showRerunModal}>
              <Modal.Header closeButton>
                <Modal.Title>Process list to rerun workflow</Modal.Title>
              </Modal.Header>

              <Modal.Body>
                <p>{this.state.rerunModalContent}</p>
              </Modal.Body>

              <Modal.Footer>
                <Button variant="secondary" onClick={(e) => {
                  e.preventDefault();
                  this.setState({ rerunModalContent: "Waiting...", showRerunModal: false });
                }}>Close</Button>
              </Modal.Footer>
            </Modal>
          </Col>

          <Col xs={9}>
            <Table striped bordered hover className="mljobtable">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Name</th>
                  <th>Starttime (UTC)</th>
                  <th>Finishtime (UTC)</th>
                  <th>Owner</th>
                  <th>Exit Code</th>
                  <th>Compare</th>
                </tr>
              </thead>
              {tableElements}
            </Table>
          </Col>

          <Col>
            <Form className="mljobform">
              <Form.Group>
                <Form.Label>Job Description</Form.Label>
                <Form.Control type="input" id="job_description" placeholder="Enter job information" />
              </Form.Group>

              <Button variant="primary" type="button" id="btn-find-jobs" onClick={(e) => {
                // get initial filename
                var jobDescription = document.getElementById('job_description').value;
                console.log(`Searching jobs: ${jobDescription}`);
                fetchJobProcesses(jobDescription)
                  .then((workflows) => {
                    console.log(`Query successful: ${JSON.stringify(workflows)}`);
                    this.setState({ workflowItems: workflows });
                  });
              }}>
                Find Jobs
              </Button>
            </Form>
          </Col>
        </Row>
      </Container>
    );
  }
}

export default withRouter(ProvenanceMLPipeline);
