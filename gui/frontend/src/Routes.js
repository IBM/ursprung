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
import { Route, Switch } from "react-router-dom";
import LandingPage from "./components/LandingPage";
import ProvenanceWorkflows from './components/ProvenanceWorkflows';
import ProvenanceMLPipeline from './components/ProvenanceMLPipeline';
import ProvenanceGraph from './components/ProvenanceGraph';

class Routes extends Component {

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
    <Switch>
      <Route exact path="/">
        <LandingPage />
      </Route>
      <Route exact path="/graph">
       <ProvenanceGraph callbackToParent={this.parentCallback}
            provStartPath={this.state.provenanceStartPath} workflowOutFiles={this.state.workflowOutputFiles}
            mlWorkflowProcess={this.state.mlWorkflowProcess} />
      </Route>
      <Route exact path="/workflows">
        <ProvenanceWorkflows callbackToParent={this.parentCallback} />
      </Route>
      <Route exact path="/pipelines">
        <ProvenanceMLPipeline callbackToParent={this.parentCallback} />
      </Route>
    </Switch>
    );
  }
}

export default Routes;
