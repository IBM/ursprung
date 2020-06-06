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
import { Tabs, Tab } from "carbon-components-react";

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

    const props = {
      tabs: {
        selected: 0,
        triggerHref: "#",
        role: "navigation",
      },
      tab: {
        href: "#",
        role: "presentation",
        tabIndex: 0,
      },
    };

    return (
      <div className="bx--grid bx--grid--full-width ppanel">
        <div className="bx--row ppanel__banner">
          <div className="bx--col-lg-16">
            <h1 className="ppanel__heading">
              Provenance Explorer
            </h1>
          </div>
        </div>
        <div className="bx--row ppanel__r2">
          <div className="bx--col bx--no-gutter">
            <Tabs {...props.tabs} aria-label="Tab navigation">
              <Tab {...props.tab} label="About">
                <div className="bx--grid bx--grid--no-gutter bx--grid--full-width">
                  <div className="bx--row ppanel__tab-content">
                    <div className="bx--col-lg-16">
                      blabla
                    </div>
                  </div>
                </div>
              </Tab>
              <Tab {...props.tab} label="Design">
                <div className="bx--grid bx--grid--no-gutter bx--grid--full-width">
                  <div className="bx--row ppanel__tab-content">
                    <div className="bx--col-lg-16">
                      Rapidly build beautiful and accessible experiences. The Carbon kit
                      contains all resources you need to get started.
                    </div>
                  </div>
                </div>
              </Tab>
              <Tab {...props.tab} label="Develop">
                <div className="bx--grid bx--grid--no-gutter bx--grid--full-width">
                  <div className="bx--row ppanel__tab-content">
                    <div className="bx--col-lg-16">
                      Carbon provides styles and components in Vanilla, React, Angular,
                      and Vue for anyone building on the web.
                    </div>
                  </div>
                </div>
              </Tab>
            </Tabs>
          </div>
        </div>
      </div>
    );
  }
}

export default ProvenancePanel;
