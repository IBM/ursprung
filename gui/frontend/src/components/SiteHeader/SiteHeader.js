import React from 'react';
import Navbar from 'react-bootstrap/Navbar';
import Jumbotron from 'react-bootstrap/Jumbotron'
import './site-header.css';

const SiteHeader = () => (
  <div>
    <Navbar variant="dark" bg="dark">
      <Navbar.Brand href="#home">Ursprung GUI</Navbar.Brand>
      <Navbar.Toggle aria-controls="basic-navbar-nav" />
    </Navbar>
    <Jumbotron>
      <h1 className="header">Welcome to the Ursprung UI!</h1>
    </Jumbotron>
  </div>
);

export default SiteHeader;