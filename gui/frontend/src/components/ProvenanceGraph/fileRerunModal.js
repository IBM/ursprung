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
import PropTypes from 'prop-types';

class FileRerunModal extends React.Component {
  render() {
    // Return empty div if show is false
    if (!this.props.show) {
      return <div></div>
    }

    // The gray background
    const backdropStyle = {
      position: 'fixed',
      top: 0,
      bottom: 0,
      left: 0,
      right: 0,
      backgroundColor: 'rgba(0,0,0,0.3)',
      padding: 50
    };

    // The modal "window"
    const modalStyle = {
      maxWidth: 700,
      minHeight: 350,
      margin: '0 auto',
      padding: 30
    };

    return (
      <div style={backdropStyle}>
        <div className="bx--modal-container" style={modalStyle}>
          <div className="bx--modal-header">
            <p className="bx--modal-header__heading bx--type-beta" id="file-rerun-modal-heading">Processes to Reproduce File</p>
            <button className="bx--modal-close" type="button" data-modal-close aria-label="close modal" onClick={(e) => {
              this.props.onClose();
            }}>
              <svg focusable="false" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg" className="bx--modal-close__icon" width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">
                <path d="M12 4.7l-.7-.7L8 7.3 4.7 4l-.7.7L7.3 8 4 11.3l.7.7L8 8.7l3.3 3.3.7-.7L8.7 8z"></path>
              </svg>
            </button>
          </div>

          <div className="bx--modal-content">
            {this.props.children}
          </div>
        </div>
      </div>
    );
  }
}

FileRerunModal.propTypes = {
  onClose: PropTypes.func.isRequired,
  show: PropTypes.bool,
  children: PropTypes.node
};

export default FileRerunModal;
