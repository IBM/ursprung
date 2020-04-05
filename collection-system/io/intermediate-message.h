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

#ifndef CONSUMER_INTERMEDIATE_MESSAGE_H_
#define CONSUMER_INTERMEDIATE_MESSAGE_H_

#include <string>

enum ConsumerSource {
  CS_PROV_GPFS = 0,
  CS_PROV_AUDITD = 1
};

enum ConsumerDestination {
  CD_DB2 = 0,
  CD_POSTGRES = 1
};

/**
 * Abstract class to represent a message between provenance
 * source (e.g. auditd) and destination (e.g. a relational DB).
 * Subclasses should override all virtual methods and offer
 * the indicated constructors.
 */
class IntermediateMessage {
protected:
  /**
   * Converts the specified string into a varchar formatted string
   * for insertion into a DB table.
   */
  std::string format_as_varchar(const std::string &str, int limit = -1) const;

public:
  IntermediateMessage(ConsumerSource csrc, const std::string &msgin);
  virtual ~IntermediateMessage() {};

  /**
   * Get a unique ID for this intermediate message.
   */
  virtual std::string get_id() const = 0;

  /**
   * Return a message formatted for a consumer destination of type cdest.
   * If cdest requires delimited fields, specify the delim parameter.
   */
  virtual std::string normalize(ConsumerDestination cdest, std::string delim = ",") const = 0;

  /**
   * Get the value for the specified message field.
   * If the field doesn't exist, this function returns
   * a nullptr.
   */
  virtual std::string get_value(std::string field) const = 0;
};

#endif /* CONSUMER_INTERMEDIATE_MESSAGE_H_ */
