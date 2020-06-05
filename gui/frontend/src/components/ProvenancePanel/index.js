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
import { Tab, Tabs } from 'carbon-components-react';

class ProvenancePanel extends Component {
  constructor(props) {
    super(props);
    this.componentDidMount = this.componentDidMount.bind(this);
    this.state = {
      provenanceStartPath: "/path/to/file",
      workflowOutputFiles: []
    };
  }

  componentDidMount() {
  }

  parentCallback = (childData) => {
    this.setState({
      provenanceStartPath: childData.provStartPath,
      workflowOutputFiles: childData.workflowOutputFiles,
      mlWorkflowProcess: childData.mlWorkflowProcess
    });
  }

  render() {
    console.log(`Rendering displayProvenance`);

    return (
      <div id="provenance_workflows_panel_tabs">
        <Tabs>
          <Tab label="Provenance" id="provenance_panel_graph2">
            <div>Hello Provenance!</div>
          </Tab>
          <Tab label="Workflows" id="provenance_panel_workflows">
            <div>Hello Workflows!</div>
          </Tab>
          <Tab label="ML Pipelines" id="provenance_panel_mlpipelines">
            <div>Hello Pipelines!</div>
          </Tab>
        </Tabs>
      </div>
    );
  }
}

export default ProvenancePanel;
