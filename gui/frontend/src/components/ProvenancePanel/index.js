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
import { Tabs, Tab } from 'react-bootstrap';
import ProvenanceWorkflows from '../ProvenanceWorkflows';
import ProvenanceMLPipeline from '../ProvenanceMLPipeline';
import ProvenanceGraph from '../ProvenanceGraph';
import './provenance-panel.css';

class ProvenancePanel extends Component {

  constructor(props) {
    super(props);
    this.componentDidMount = this.componentDidMount.bind(this);
    this.state = {
      provenanceStartPath: "/path/to/file",
      workflowOutputFiles: []
    };
  }

  componentDidMount() {}

  parentCallback = (childData) => {
    this.setState({
      provenanceStartPath: childData.provStartPath,
      workflowOutputFiles: childData.workflowOutputFiles,
      mlWorkflowProcess: childData.mlWorkflowProcess
    });
  }

  render() {
    return (
      <Tabs defaultActiveKey="graph" id="provenance-panel-tabs">
        <Tab eventKey="graph" title="Provenance Graph">
          <ProvenanceGraph callbackToParent={this.parentCallback}
            provStartPath={this.state.provenanceStartPath} workflowOutFiles={this.state.workflowOutputFiles}
            mlWorkflowProcess={this.state.mlWorkflowProcess} />
        </Tab>
        <Tab eventKey="workflow" title="Workflows">
          <ProvenanceWorkflows callbackToParent={this.parentCallback} />
        </Tab>
        <Tab eventKey="mlpipeline" title="ML Pipelines">
          <ProvenanceMLPipeline callbackToParent={this.parentCallback} />
        </Tab>
      </Tabs>
    );
  }
}

export default ProvenancePanel;
