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
import cytoscape from 'cytoscape';
import popper from 'cytoscape-popper';
import tippy from 'tippy.js'
import $ from "jquery";
import { Container, Row, Col } from 'react-bootstrap';
import { Form, Button, Table, Modal } from 'react-bootstrap';
import './provenance-graph.css';

var ProvNodes = require('./provNodes');
var helpers = require('./helpers');
var mlPipelineHelpers = require('../ProvenanceMLPipeline/helpers');
var workflowHelpers = require('../ProvenanceWorkflows/helpers');
var Filter = require('./filter');

const plainEdgeStyle = {
  'line-color': '#61bffc',
  'target-arrow-color': '#61bffc',
  'transition-property': 'line-color, target-arrow-color',
  'transition-duration': '1s'
};
const renameEdgeStyle = {
  'line-color': '#ff8e00',
  'target-arrow-color': '#ff8e00',
  'transition-property': 'line-color, target-arrow-color',
  'transition-duration': '1s',
  'opacity': '0.25'
};

cytoscape.use(popper);

class ProvenanceGraph extends Component {

  /**
   * Constructs an empty cytoscape graph and defines the basic styles
   * for nodes and edges.
   */
  static buildEmptyCytoscapeGraph() {
    return cytoscape({
      container: document.getElementById('cy2'),

      boxSelectionEnabled: false,
      autounselectify: true,

      style: cytoscape.stylesheet()
        .selector('node')
        .css({
          'height': 80,
          'width': 80,
          'transition-property': 'background-color',
          'transition-duration': '1s',
          'text-valign': 'center',
          'text-align': 'center',
        })
        .selector('edge')
        .css({
          'width': 6,
          'target-arrow-shape': 'triangle',
          'curve-style': 'bezier'
        }),
    });
  }

  /**
   * Constructor. Sets the initial state of the component and defines
   * the different graph layouts.
   */
  constructor(props) {
    super(props);

    this.state = {
      workflowGraphRendered: false,
      filters: new Map(),
      filtersUpdated: false,
      isOpen: false,
      isRerunOpen: false,
      producedByWorkflow: false,
      processes: new Map(),
      selectedFileNode: null,
      workflowObj: null,
      filecontent: ""
    };

    this.onSelectionChanged = this.onSelectionChanged.bind(this);
    this.onProcessSelectionChanged = this.onProcessSelectionChanged.bind(this);
    this.closeModal = this.closeModal.bind(this);
    this.closeRerunModal = this.closeRerunModal.bind(this);
  }

  /**
   * Change handler for radio buttons in the filter table.
   * sets the selected value as the component's state.
   */
  onSelectionChanged(e) {
    this.setState({
      selectedFilter: e.currentTarget.value
    })
  }

  /**
   * Change handler for radio buttons in the process selection modal.
   * Uses the selected value to get the content of the corresponding
   * file version for the write event of the selected process.
   */
  onProcessSelectionChanged(e) {
    var key = e.currentTarget.value
    var procNode = this.state.processes.get(key);
    helpers.fetchContentForFile(this.state.selectedFileNode, procNode).then((result) => {
      console.log(`Content: ${JSON.stringify(result.filecontent)}`);
      // TODO why is result.filecontent an array?
      var lines = result.filecontent.split("\n");
      var formattedFileContent = [];
      for (var i = 0; i < lines.length; i++) {
        if (lines[i].startsWith("-")) {
          formattedFileContent.push(<span style={{ color: 'red' }}>{lines[i]}<br /></span>);
        } else if (lines[i].startsWith("+")) {
          formattedFileContent.push(<span style={{ color: 'green' }}>{lines[i]}<br /></span>);
        }
      }
      this.setState({
        filecontent: formattedFileContent
      });
    });
  }

  /**
   * Component mount handler.
   */
  componentDidMount() {
    if (this.props.workflowOutFiles.length > 0 && !this.state.workflowGraphRendered) {
      console.log(`Got ${this.props.workflowOutFiles.length} workflow output files: ${JSON.stringify(this.props.workflowOutFiles)}`);
      this.startWorkflowProvenanceGraph();
    } else if (this.props.mlWorkflowProcess !== undefined && !this.state.workflowGraphRendered) {
      console.log(`${JSON.stringify(this.props.mlWorkflowProcess)}`);
      this.startMLProvenanceGraph();
    }
  }

  /**
   * Component update handle when the component is reloaded
   * and is receiving new properties from the parent.
   */
  componentWillReceiveProps(nextProps) {
    // Any time the workflowOutFiles change, reset the state so
    // we can render the new workflow graph
    console.log(`DisplayProvenanceGraph props change: ${nextProps.workflowOutFiles} - ${this.props.workflowOutFiles}`);

    // This (seems to be) comparing object references rather then the objects itself.
    // This is what we want as we want to redisplay the graph each time a user
    // selects a workflow in the Workflows tab and presses 'Display Provenance',
    // even if the selected workflow is the one that was previously selected.
    if (nextProps.workflowOutFiles !== this.props.workflowOutFiles) {
      console.log(`DisplayProvenanceGraph props have changed`);
      this.setState({
        workflowGraphRendered: false
      });
    }
  }

  /**
   * Mouseover handler for nodes. This uses tippy to
   * display the node label.
   */
  handleMouseOverEvent(evt) {
    if (helpers.isProvNode(evt.target)) {
      const nodeObj = helpers.toProvNodeObject(evt.target);
      let ref = evt.target.popperRef();

      var tip = tippy(ref, {
        html: (() => {
          let content = document.createElement('div');
          content.innerHTML = nodeObj.getLabel();
          return content;
        })(),
        trigger: 'manual'
      }).tooltips[0];

      tip.show();
    }
  }

  /**
   * Onclick handler for nodes. Depending on the clicked node, this
   * will send a request to the backend to query the dependencies
   * of that node.
   * 
   * This just calls fetchCyNodeProvAndUpdateGraph.
   */
  handleMouseClickEvent(evt) {
    if (helpers.isProvNode(evt.target)) {
      const nodeObj = helpers.toProvNodeObject(evt.target);
      nodeObj.position.x = evt.target.position('x');
      nodeObj.position.y = evt.target.position('y');

      return this.fetchNodeProvAndUpdateGraph(nodeObj);
    }

    console.log(`fetchProvAndUpdateGraph: Warning, got unexpected evt`);
    return null;
  }

  /**
   * Onrightclick handler for nodes. For process nodes, this will
   * retrieve any associated logs. For file nodes, this will show
   * a list of process who modified the files and allow to retrieve
   * the content at different points in time if the content is
   * tracked.
   */
  handleMouseRightClickEvent(evt) {
    if (helpers.isProvNode(evt.target)) {
      const nodeObj = helpers.toProvNodeObject(evt.target);
      if (nodeObj.type === ProvNodes.NODE_TYPE_PROCESS) {
        // retrieve any associated warning or error logs and display as tooltip
        let ref = evt.target.popperRef();
        helpers.fetchLogsForProcessNode(nodeObj)
          .then((procLogs) => {
            let tooltipContent = "<table>";
            procLogs.records.forEach((r) => {
              tooltipContent += "<tr>";
              tooltipContent += "<td style=\"padding-right: 10px;\">" + r.loglevel + "</td>";
              tooltipContent += "<td>" + r.logmsg + "</td>";
              tooltipContent += "</tr>";
            });
            tooltipContent += "</table>";

            var tip = tippy(ref, {
              html: (() => {
                let content = document.createElement('div');
                content.innerHTML = tooltipContent;
                return content;
              })(),
              trigger: 'manual'
            }).tooltips[0];

            tip.show();
          });
      } else if (nodeObj.type === ProvNodes.NODE_TYPE_FILE || nodeObj.type === ProvNodes.NODE_TYPE_INITIAL_FILE) {
        helpers.fetchProv(nodeObj).then((procNodes) => {
          let procNodesList = procNodes[0].records;
          procNodesList.sort(function (a, b) {
            return new Date(a.eventTime) - new Date(b.eventTime);
          });
          procNodesList.forEach((procNode) => {
            if ((procNode.desc.write === true && procNode.event === "CLOSE") || procNode.desc.renamed === true) {
              this.state.processes.set(procNode.pid.toString(), procNode);
            }
          });

          // display the modal for process selection
          this.setState({ isOpen: true, selectedFileNode: nodeObj });
        });
      }
    }
  }

  /**
   * Hold handler for nodes. This only works for file
   * nodes and will retrieve the list of commands required
   * to reproduce this file
   */
  handleHoldEvent(evt) {
    if (helpers.isProvNode(evt.target)) {
      const nodeObj = helpers.toProvNodeObject(evt.target);
      if (nodeObj.type === ProvNodes.NODE_TYPE_FILE || nodeObj.type === ProvNodes.NODE_TYPE_INITIAL_FILE) {
        let findWorkflowPromise = workflowHelpers.fetchOutputFilesWorkflow(nodeObj.path, nodeObj.inode);

        return findWorkflowPromise.then((workflows) => {
          if (workflows.records.length === 0) {
            // no workflow found, collect all dependent processes
            let workflowPromise = mlPipelineHelpers.fillWorkflowForFile(nodeObj);

            return workflowPromise.then((workflow) => {
              this.setState({ isRerunOpen: true, workflowObj: workflow });
            });
          } else if (workflows.records.length === 1) {
            // workflow found, display it
            this.setState({ isRerunOpen: true, producedByWorkflow: true, workflowObj: workflows.records[0] });
          } else {
            alert(`Found ${workflows.length} workflows for single file??`);
          }
        })
      }
    }
  }

  /**
   * Update state to prevent process selection
   * modal from being rendered.
   */
  closeModal() {
    this.setState({
      isOpen: false,
      processes: new Map(),
      filecontent: ""
    });
  }

  /**
   * Update state to prevent rerun
   * modal from being rendered.
   */
  closeRerunModal() {
    this.setState({
      isRerunOpen: false,
      workflowObj: null,
      producedByWorkflow: false
    });
  }

  /**
   * Wipe any existing graphs and start a new empty graph.
   * Returns the result of fetchCyNodeProvAndUpdateGraph on path.
   */
  startProvenanceGraph(val, startingPoint) {
    if (!val) {
      alert("Must provide a value for the text field.");
      return;
    }

    this.cy = ProvenanceGraph.buildEmptyCytoscapeGraph();

    // Monitor events
    // By default the CBs are called in the context of the event.target.
    // Override that with binding to 'this' so we have access to our helpers.
    this.cy.on('mouseover', 'node', this.handleMouseOverEvent.bind(this));
    this.cy.on('tap', 'node', this.handleMouseClickEvent.bind(this));
    this.cy.on('cxttap', 'node', this.handleMouseRightClickEvent.bind(this));
    this.cy.on('taphold', 'node', this.handleHoldEvent.bind(this));

    let queryNode = null;
    if (startingPoint === 'file') {
      queryNode = new ProvNodes.InitialFileNode(val);
    } else if (startingPoint === 'process') {
      queryNode = new ProvNodes.InitialProcessNode(val);
    } else {
      console.log(`ERROR: unexpected starting point ${startingPoint} for provenance query.`);
    }

    console.log(`Calling fetchCyNodeProvAndUpdateGraph with ${JSON.stringify(queryNode)}`);
    this.fetchNodeProvAndUpdateGraph(queryNode);
  }

  /**
   * Wipe and start the graph using the initial nodes that
   * are provided by the workflow output files.
   */
  startWorkflowProvenanceGraph() {
    console.log(`startWorkflowProvenanceGraph`);
    // initialize an empty cy graph
    this.cy = ProvenanceGraph.buildEmptyCytoscapeGraph();
    this.cy.on('mouseover', 'node', this.handleMouseOverEvent.bind(this));
    this.cy.on('tap', 'node', this.handleMouseClickEvent.bind(this));
    this.cy.on('cxttap', 'node', this.handleMouseRightClickEvent.bind(this));
    this.cy.on('taphold', 'node', this.handleHoldEvent.bind(this));

    // create dummy node to get InitialFileNode style
    let dummy = new ProvNodes.InitialFileNode('/');

    var yPos = 250;
    this.props.workflowOutFiles.forEach((node) => {
      this.cy.add([{
        group: "nodes",
        data: node.toCyNode(),
        position: { x: 500, y: yPos },
        style: dummy.style
      }]);
      yPos = yPos + 150;
    });

    // Set the workflowOutfiles props to empty to indicate that
    // we've consumed the initial nodes.
    this.setState({
      workflowGraphRendered: true
    });

    return;
  }

  /**
   * Wipe and start the graph using the initial process node that
   * is provided by the mlWorkflowProcess.
   */
  startMLProvenanceGraph() {
    console.log(`startMLProvenanceGraph`);
    // initialize an empty cy graph
    this.cy = ProvenanceGraph.buildEmptyCytoscapeGraph();
    this.cy.on('mouseover', 'node', this.handleMouseOverEvent.bind(this));
    this.cy.on('tap', 'node', this.handleMouseClickEvent.bind(this));
    this.cy.on('cxttap', 'node', this.handleMouseRightClickEvent.bind(this));
    this.cy.on('taphold', 'node', this.handleHoldEvent.bind(this));

    // create dummy node to get InitialProcessNode style
    let dummy = new ProvNodes.InitialProcessNode('proc');

    var yPos = 250
    this.props.mlWorkflowProcess.forEach((procNode) => {
      this.cy.add([{
        group: "nodes",
        data: procNode.toCyNode(),
        position: { x: 500, y: yPos },
        style: dummy.style
      }]);
      yPos = yPos + 250;
    })

    // Set the workflowOutfiles props to empty to indicate that
    // we've consumed the initial nodes.
    this.setState({
      workflowGraphRendered: true
    })

    return;
  }

  /**
   * Fetch provenance info for this cyNode and update graph.
   *
   * The target must have identifying information stored in it.
   * We retrieve this information and query the backend for
   * provenance information.
   * 
   * Returns promise with all the nodes we added.
   */
  fetchNodeProvAndUpdateGraph(node) {
    return helpers.fetchProv(node)
      .then((provInfo) => {
        console.log(`Query successful: ${JSON.stringify(provInfo)}`);
        return this.updateGraphWithProvInfo(node, provInfo);
      });
  }

  /**
   * Update the graph with the new provenance data in provInfo,
   * which was retrieved based on the specified node.
   * 
   * Returns array of all the cy-nodes added.
   */
  updateGraphWithProvInfo(node, provInfo) {
    helpers.assert(provInfo, `updateGraphWithProvInfo: Error, undefined provInfo`);
    helpers.assert(provInfo.length, `updateGraphWithProvInfo: Error, nothing in the provInfo array`);

    if (node.type === ProvNodes.NODE_TYPE_INITIAL_FILE || node.type === ProvNodes.NODE_TYPE_INITIAL_PROCESS) {
      // the graph is empty and we're adding the first node
      console.log(`updateGraphWithProvInfo: Placing initial node`);
      return [this.placeInitialNode(node, provInfo)]; // must return an array
    } else {
      // add received provInfo to existing graph
      let provNodesWithVisInfo = this.addVisInfo(node, provInfo);
      console.log(`Got ${provNodesWithVisInfo.length} provNodes: ${JSON.stringify(provNodesWithVisInfo.map((pn) => pn.node))}`);
      let nodesWeAdded = this.addProvNodes(node, provNodesWithVisInfo);

      // Make sure we can see all the nodes.
      //this.cy.fit(null, 100);

      /* Return the cy-nodes we added. */
      return nodesWeAdded.map((n) => {
        return this.cy.getElementById(n.id);
      });
    }
  }

  /**
   * Performs the initial placement of a provenance node
   * on an empty provenance panel.
   */
  placeInitialNode(node, provInfo) {
    console.log(`placeInitialNode ${JSON.stringify(node)} with provInfo
                 ${JSON.stringify(provInfo)}`);

    // Ensure we got some info on the file.
    const initialNodes = provInfo.filter((e) => {
      return e.type === helpers.PROCESS_USED_FILE_INTERACTION;
    });

    if (!initialNodes.length || initialNodes[0].records.length < 1) {
      console.log(`Warning, found no provenance for ${JSON.stringify(node)}`);
      alert(`No provenance found for ${node.path}`);
      var tmp = this.cy.elements().remove;
      return null;
    }

    helpers.assert(initialNodes.length === 1,
      `Error, unexpected provInfo: too many prov_processesThatInteractedWithFile
         in ${JSON.stringify(provInfo)}`);
    const provNodes = initialNodes[0].records;

    // Construct initial graph: just show the initial nodes
    const xPos = document.getElementById("cy2").offsetWidth - 250;
    let yPos = 500;

    let labels = [];
    provNodes.forEach((r) => {
      let nodeToDisplay = null;

      if (node.type === ProvNodes.NODE_TYPE_INITIAL_FILE) {
        nodeToDisplay = new ProvNodes.FileNode(node.path, r.inode);
      } else if (node.type === ProvNodes.NODE_TYPE_INITIAL_PROCESS) {
        nodeToDisplay = r;
      }

      this.cy.add([{
        group: "nodes",
        data: nodeToDisplay.toCyNode(),
        position: { x: xPos, y: yPos },
        // we use the style of the initial file node here
        style: node.style
      }]);

      yPos = yPos + 150;
      labels.push(nodeToDisplay.getLabel());
    });

    return labels;
  }

  /**
   * Computes the relative location for each node
   * in the retrieved dataset, i.e. whether it should
   * be plotted left or right from the query node.
   */
  addVisInfo(node, provInfo) {
    let labels = new Set();
    let provNodesWithVisInfo = [];

    provInfo.forEach((prov) => {
      prov.records.forEach((r) => {
        let dstNode;
        let relativeLocation;
        let bidirectional = false;

        // check if any filters apply to the current node
        for (var [key, value] of this.state.filters) {
          if (value.evaluate(r)) {
            return;
          }
        }

        if (prov.type === helpers.IPC_INTERACTION) {
          console.log(`Building process node from record: ${JSON.stringify(r, null, 2)}`);

          // TODO Could be west or east, this is a "communication group".
          relativeLocation = helpers.cardinal.west;
          bidirectional = true;
        } else if (prov.type === helpers.TRUEIPC_INTERACTION) {
          console.log(`Building true process node from record: ${JSON.stringify(r, null, 2)}`);

          if (r.desc.read) {
            relativeLocation = helpers.cardinal.east;
          } else if (r.desc.write) {
            relativeLocation = helpers.cardinal.west;
          }
          bidirectional = false;
        } else if (prov.type === helpers.NET_INTERACTION) {
          console.log(`Building network process node from record: ${JSON.stringify(r, null, 2)}`);

          if (r.desc.read) {
            relativeLocation = helpers.cardinal.east;
          } else if (r.desc.write) {
            relativeLocation = helpers.cardinal.west;
          }
          bidirectional = false;
        } else if (prov.type === helpers.PROCESS_USED_FILE_INTERACTION) {
          console.log(`Building process node from record: ${JSON.stringify(r, null, 2)}`);

          if (r.desc.renamed) {
            // if the op was a rename, we have to determine, whether the src or dst file was clicked
            if (node.path === r.path) {
              // the search node is equal to the src so plot rename process east
              relativeLocation = helpers.cardinal.east;
            } else if (node.path === r.dstpath) {
              // the search node is equal to the dst so plot rename process west
              relativeLocation = helpers.cardinal.west;
            } else {
              helpers.assert(false, `Search node path ${node.path} did not match either src ${r.path} or
                dst ${r.dstpath} of rename operation?`);
            }
          } else {
            relativeLocation = helpers.interactionDescToCardinal(r, false);
          }
          bidirectional = false;
        } else if (prov.type === helpers.FILE_USEDBY_PROCESS_INTERACTION) {
          console.log(`Building file node from record: ${JSON.stringify(r, null, 2)}`);

          if (r.desc.renamed) {
            // If this is a rename op, r by default contains the src node as
            // the file nodes in the query result are always created using
            // the path attribute and never the dstpath attribute. Hence, we
            // create the additional destination node of the rename operation
            // and set the relative location of the src node to west.
            dstNode = new ProvNodes.FileNode(r.dstpath, r.inode);
            dstNode.desc = { renamed: true };
            relativeLocation = helpers.cardinal.west;
          } else {
            relativeLocation = helpers.interactionDescToCardinal(r, true);
          }
          bidirectional = false;
        } else {
          helpers.assert(false, `Error, unexpected prov type ${prov.type}`);
        }

        // only keep the unique provNodes
        let label = r.getLabel() + r.getDescString();
        if (!labels.has(label)) {
          provNodesWithVisInfo.push({
            node: r.toCyNode(),
            nodeObj: r,
            style: r.style,
            relativeLocation: relativeLocation,
            bidirectional: bidirectional
          });
          labels.add(label);
        } else {
          console.log(`Discarding duplicate prov node in this query: ${label}`);
        }

        // check if we have to display the dst node from a rename event
        // and perform the same duplicate elimination as above
        if (dstNode != null) {
          label = dstNode.getLabel() + dstNode.getDescString();
          if (!labels.has(label)) {
            provNodesWithVisInfo.push({
              node: dstNode.toCyNode(),
              nodeObj: dstNode,
              style: dstNode.style,
              // dst node is always east
              relativeLocation: helpers.cardinal.east,
              bidirectional: bidirectional
            });
            labels.add(label);
          } else {
            console.log(`Discarding duplicate prov node in this query: ${label}`);
          }
        }
      });
    });

    return provNodesWithVisInfo;
  }

  /**
   * Adds a set of provenance nodes to the existing
   * provenance graph.
   */
  addProvNodes(node, provNodesWithVisInfo) {
    const nNodes = {
      west: provNodesWithVisInfo.filter((e) => {
        return e.relativeLocation === helpers.cardinal.west
      }).length,
      east: provNodesWithVisInfo.filter((e) => {
        return e.relativeLocation === helpers.cardinal.east
      }).length
    };

    const yStepSizes = {
      west: helpers.calcYStepSize(nNodes.west),
      east: helpers.calcYStepSize(nNodes.east)
    };

    const yLowestPos = {
      // Half of nodes are below, half above
      west: node.position.y - 0.5 * (nNodes.west * yStepSizes.west),
      east: node.position.y - 0.5 * (nNodes.east * yStepSizes.east)
    }

    const yIxs = {
      west: 0,
      east: 0
    };

    // place each node
    console.log(`Placing ${nNodes.west} nodes to the west and ${nNodes.east} nodes to the east`);
    console.log(`yStepSizes ${JSON.stringify(yStepSizes)} yLowestPos ${JSON.stringify(yLowestPos)}`);

    let nodesWeAdded = [];

    provNodesWithVisInfo.forEach((elt) => {
      console.log(`Node ID: ${elt.node.id}`);
      if (!this.cy.getElementById(elt.node.id).length) {
        const d_x = 350;
        const xPos = helpers.calcXPos(node.position.x, d_x, elt.relativeLocation);
        const yPos = yLowestPos[elt.relativeLocation]
          + (yIxs[elt.relativeLocation] * yStepSizes[elt.relativeLocation]);

        // Add the node.
        console.log(`Adding node ${JSON.stringify(elt.node)}`);
        console.log(`Placing node at <${xPos}, ${yPos}> (curr <${node.position.x}, ${node.position.y}>)`);
        this.cy.add([
          {
            group: "nodes",
            data: elt.node,
            position: { x: xPos, y: yPos },
            style: elt.style
          }
        ]);
        nodesWeAdded.push(elt.node);
        // update indices, only if we are actually adding the node
        yIxs[elt.relativeLocation]++;
      } else {
        console.log(`Not adding already-present prov node ${elt.node.id}`);
      }

      // Add the edge.
      let sourceId, targetId;
      if (elt.bidirectional) { // TODO This probably doesn't work.
        // Make sure we find the same edge regardless of direction.
        const id1 = elt.node.id;
        const id2 = node.getLabel();
        if (id1 < id2) {
          sourceId = id1;
          targetId = id2;
        } else {
          sourceId = id2;
          targetId = id1;
        }
      } else {
        if (elt.relativeLocation === helpers.cardinal.west) {
          sourceId = elt.node.id;
          targetId = node.getLabel();
        } else {
          sourceId = node.getLabel();
          targetId = elt.node.id;
        }
      }
      const edgeConnectorCode = elt.bidirectional ? '<->' : '->';
      const edgeId = `${sourceId}${edgeConnectorCode}${targetId}`;
      if (!this.cy.getElementById(edgeId).length) {
        console.log(`Drawing edge ${edgeId}`);
        let edgeStyle = plainEdgeStyle;
        if (elt.nodeObj.desc.renamed) {
          edgeStyle = renameEdgeStyle;
        }
        this.cy.add([
          {
            group: "edges",
            data: { id: edgeId, source: sourceId, target: targetId },
            style: edgeStyle
          }
        ]);
      } else {
        console.log(`Not adding already-present edge ${edgeId}`);
      }
    });

    return nodesWeAdded;
  }

  /**
   * Implementation of the react.js Component interface. This
   * requires the implementation of the {@code render} function.
   */
  render() {
    console.log(`Rendering displayProvenanceGraph with ${this.props.provStartPath}`);

    const cy_style = {
      height: '1000px',
      width: '100%',
    };

    const content_style = {
      whiteSpace: 'pre-line'
    }

    if (this.state.filters.size === 0 || !this.state.filtersUpdated) {
      console.log("No filters configured");
    } else {
      var tableElements = [];
      for (var [key, value] of this.state.filters) {
        tableElements.push(<tr>
          <td>{value.toString()}</td>
          <td><input id={value.toString()} class="bx--radio-button" type="radio" value={value.toString()} name="radio-button" tabindex="0" onChange={this.onSelectionChanged} /></td>
        </tr>);
      }
    }

    if (this.state.processes.size === 0) {
      console.log("No processes selected");
    } else {
      var processSelectionElements = [];
      for (var [key, value] of this.state.processes) {
        processSelectionElements.push(
          <div className="bx--radio-button-wrapper">
            <input id={key.toString()} className="bx--radio-button" type="radio" value={key.toString()} name="radio-button--vertical" tabindex="0" onChange={this.onProcessSelectionChanged} />
            <label htmlFor={key.toString()} className="bx--radio-button__label">
              <span className="bx--radio-button__appearance"></span>
              <span className="bx--radio-button__label-text">{key.toString() + " " + value.execCmdLine + " " + value.eventTime}</span>
            </label>
          </div>);
      }
    }

    console.log(`State before render: ${JSON.stringify(this.state)}`);
    if (this.state.workflowObj != null) {
      var processRerunElements = [];
      if (this.state.producedByWorkflow) {
        processRerunElements.push(
          <div>
            <table className="bx--data-table-v2 bx--data-table-v2--zebra">
              <thead>
                <tr>
                  <th>File produced by workflow</th>
                </tr>
              </thead>
              <tbody>
                <tr><td>{this.state.workflowObj.name}</td></tr>
                <tr><td>{this.state.workflowObj.def1}</td></tr>
                <tr><td>{this.state.workflowObj.def2}</td></tr>
              </tbody>
            </table>
          </div>
        )
      } else {
        processRerunElements = this.state.workflowObj.toProcessListHtml();
      }
    }

    return (
      <div id="provenance_panel">
        <Container fluid>
          <Row>
            <Col>
              <Form className="searchnodeform">
                <Form.Group>
                  <legend>File/Process Search</legend>
                  <Form.Control type="input" id="prov_path2" placeholder={this.props.provStartPath} />
                </Form.Group>
                <Button className="navbutton2" variant="primary" type="button" id="btn-find-prov" onClick={(e) => {
                    e.preventDefault();
                    // get initial filename
                    var filename = document.getElementById('prov_path2').value;
                    console.log(`Starting provenance graph for file ${filename}`);
                    this.prom_cyNodesAddedLastTime = this.startProvenanceGraph(filename, 'file');
                  }} >
                  Find File
                </Button>
                <Button className="navbutton2" variant="primary" type="button" id="btn-find-prov-proc" onClick={(e) => {
                    e.preventDefault();
                    // get initial process name
                    var processname = document.getElementById('prov_path2').value;
                    console.log(`Starting provenance graph for processes ${processname}`);
                    this.prom_cyNodesAddedLastTime = this.startProvenanceGraph(processname, 'process');
                  }} >
                  Find Process
                </Button>

                <Form.Group>
                  <legend>Provenance Classes</legend>
                </Form.Group>
                <Form.Check type="checkbox" id="bx--checkbox-new" name="enable_ipc_prov"
                  value="true" label="Enable IPC Provenance" />
                <Form.Check type="checkbox" id="bx--checkbox-trueipc" name="enable_trueipc_prov"
                  value="true" label="Enable True IPC Provenance" />
                <Form.Check type="checkbox" id="bx--checkbox-netprov" name="enable_netipc_prov"
                  value="true" label="Enable Network Provenance" />

                <Form.Group>
                  <legend>Graph Filters</legend>
                  <Form.Control type="input" id="prov_filter" placeholder="New filter" />
                </Form.Group>
                <Button className="navbutton2" variant="primary" type="button" id="btn-find-prov" onClick={(e) => {
                    // get initial filename
                    var filterVal = document.getElementById('prov_filter').value;
                    if (!filterVal.includes(":")) {
                      alert("Filter format is attribute:filter-value");
                      return;
                    }
                    var filter = new Filter.Filter(filterVal.split(":")[0], filterVal.split(":")[1]);
                    this.state.filters.set(filter.toString(), filter);
                    this.setState({ filtersUpdated: true });
                    e.preventDefault();
                    console.log(`${JSON.stringify(this.state.filters)}`);
                  }} >
                  Add Filter
                </Button>

                <Table striped bordered hover>
                  <thead>
                    <tr>
                      <th>Configured Filters</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    {tableElements}
                  </tbody>
                </Table>
                <Button variant="primary" type="button" id="btn-find-prov" onClick={(e) => {
                    // get initial filename
                    var filterName = this.state.selectedFilter;
                    this.state.filters.delete(filterName);
                    this.setState({ filtersUpdated: true });
                    e.preventDefault();
                    console.log(`${JSON.stringify(this.state.filters)}`);
                  }} >
                  Delete Filter
                </Button>
              </Form>

              <Modal show={this.state.isOpen}>
                <Modal.Header>
                  <Modal.Title>Processes which updated file:</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                  <p>{processSelectionElements}</p>
                  <div>
                    <br />
                    <h4>Content:</h4>
                    <span style={content_style}>{this.state.filecontent}</span>
                  </div>
                </Modal.Body>
                <Modal.Footer>
                  <Button variant="secondary" onClick={(e) => {
                    e.preventDefault();
                    this.setState({ modalContent: "Waiting...", isOpen: false });
                  }}>Close</Button>
                </Modal.Footer>
              </Modal>

              <Modal show={this.state.isRerunOpen}>
                <Modal.Header>
                  <Modal.Title>Processes which updated file:</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                  <p>{processRerunElements}</p>
                </Modal.Body>
                <Modal.Footer>
                  <Button variant="secondary" onClick={(e) => {
                    e.preventDefault();
                    this.setState({ modalContent: "Waiting...", isRerunOpen: false });
                  }}>Close</Button>
                </Modal.Footer>
              </Modal>
            </Col>
            <Col xs={9}>
              <div style={cy_style} id="cy2" />
            </Col>
          </Row>
        </Container>
      </div>
    );
  }

}

export default ProvenanceGraph;
