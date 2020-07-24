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
import Table from 'react-bootstrap/Table';
import Button from 'react-bootstrap/Button';
import { Container, Row, Col } from 'react-bootstrap';

import fetchWorkflows from './helpers';
import { fetchWorkflowOutputFiles, convertTime } from './helpers';

class ProvenanceWorkflows extends Component {

  constructor(props) {
    super(props);
    this.state = {
      workflowItems: {},
      checkedBoxes: []
    };
    this.componentDidMount = this.componentDidMount.bind(this);
    this.onSelectionChanged = this.onSelectionChanged.bind(this);
  }

  /**
   * On mount, fetch the workflows from the database and set
   * the internal state. The state is later used to display the
   * retrieved result in a table.
   */
  componentDidMount() {
    fetchWorkflows()
      .then((workflows) => {
        console.log(`Query successful`);
        this.setState({ workflowItems: workflows });
      });
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

  render() {
    if (Object.keys(this.state.workflowItems).length === 0) {
      console.log("No state set yet");
    } else {
      var tableElements = this.state.workflowItems.reduce((elements, item) => {
        elements.push(<tbody>
          {item.records.map((record) => {
            return (<tr>
              <td><input id={record.id} type="radio" value={record.id} name="radio-button"
                tabIndex="0" onChange={this.onSelectionChanged} /></td>
              <td>{record.id}</td>
              <td>{record.name}</td>
              <td id={record.id + 'starttime'}>{convertTime(record.starttime)}</td>
              <td id={record.id + 'endtime'}>{convertTime(record.endtime)}</td>
              <td id={record.id + 'owner'}>{record.owner}</td>
              <td>{record.exitcode}</td>
            </tr>)
          })}
        </tbody>);
        return elements;
      }, []);
    }

    return (
      <div id="provenance-workflows-panel">
        <Container fluid>
          <Row>
            <Col xs={11}>
              <Table striped bordered hover>
                <thead>
                  <tr>
                    <th></th>
                    <th>ID</th>
                    <th>Name</th>
                    <th>Starttime (UTC)</th>
                    <th>Finishtime (UTC)</th>
                    <th>Owner</th>
                    <th>Exit Code</th>
                  </tr>
                </thead>
                {tableElements}
              </Table>
            </Col>
            <Col>
              <Button variant="primary" size="sm" id="show-provenance-button" type="button" onClick={(e) => {
                e.preventDefault();

                // fetch the output files from the selected workflow
                const startTime = document.getElementById(this.state.selectedWorkflow + 'starttime').innerHTML;
                const endTime = document.getElementById(this.state.selectedWorkflow + 'endtime').innerHTML;

                console.log(`Starttime: ${startTime} - Endtime: ${endTime}`);
                fetchWorkflowOutputFiles(this.state.selectedWorkflow, startTime, endTime)
                  .then((workflows) => {
                    console.log(`Query successful: ${JSON.stringify(workflows.records)}`);
                    if (workflows.records.length > 0) {
                      // return the selected workflow ID and the corresponding workflow
                      // output files to the parent component
                      this.props.callbackToParent({
                        provStartPath: this.state.selectedWorkflow,
                        workflowOutputFiles: workflows.records
                      });
                      this.props.history.push('/graph');
                    } else {
                      alert("No provenance found for selected workflow. Please check the database manually.")
                    }
                  });
              }} >
                Show Provenance
              </Button>
            </Col>
          </Row>
        </Container >
      </div >
    );
  }
}

export default withRouter(ProvenanceWorkflows);
