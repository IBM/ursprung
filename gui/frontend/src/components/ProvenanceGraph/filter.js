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

export class Filter {
  
  /** 
   * The attribute of a provenance graph node on which the filter should be
   * evaluated.
   */
  attribute;
  
  /** The actual filter value. */
  value;
  
  constructor(attribute, value) {
    this.attribute = attribute;
    this.value = value;
  }
  
  /**
   * Evaluate this filter on the passed node.
   */
  evaluate(node) {
    if (node[this.attribute] === undefined) {
      // object doesn't contain filter attribute so we
      // just return false
      return false;
    }
    
    return node[this.attribute].includes(this.value);
  }
  
  /**
   * Returns a string representation of this filter
   */
  toString() {
    return this.attribute + ":" + this.value;
  }
}
