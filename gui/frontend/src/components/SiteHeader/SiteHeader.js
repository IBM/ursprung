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
import { LinkContainer } from "react-router-bootstrap";
import { Navbar, Nav } from 'react-bootstrap';
import { Link } from "react-router-dom";
import Breadcrumb from 'react-bootstrap/Breadcrumb';
import './site-header.css';

const SiteHeader = () => (
  <div>
    <Navbar variant="dark" bg="dark">
      <Navbar.Brand><Link to="/">Ursprung UI</Link></Navbar.Brand>
      <Navbar.Toggle aria-controls="basic-navbar-nav" />
      <Nav pullRight>
        <LinkContainer to="/graph">
          <Nav.Link>Graph Viewer</Nav.Link>
        </LinkContainer>
        <LinkContainer to="/workflows">
          <Nav.Link>Workflows</Nav.Link>
        </LinkContainer>
        <LinkContainer to="/pipelines">
          <Nav.Link>ML Pipelines</Nav.Link>
        </LinkContainer>
      </Nav>
    </Navbar>
    <Breadcrumb></Breadcrumb>
  </div>
);

export default SiteHeader;