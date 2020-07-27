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

#include <sstream>
#include <iomanip>
#include <openssl/md5.h>

#include "rule-engine.h"
#include "logger.h"

const std::string RULE_DELIM = "->";
const char DELIM = ';';

/*------------------------------
 * RuleEngine
 *------------------------------*/

RuleEngine::RuleEngine(std::string rules_file) {
  std::ifstream in_file(rules_file);
  std::string line;

  while (std::getline(in_file, line)) {
    // Skip whitespace lines.
    if (line.find_first_not_of(' ') == std::string::npos) {
      continue;
    }
    // Skip comment lines (#, ;).
    if (line.at(0) == '#' || line.at(0) == ';') {
      continue;
    }

    this->add_rule(line);
  }
}

int RuleEngine::add_rule(std::string rule) {
  std::unique_ptr<Rule> r = std::make_unique<Rule>();

  // compute rule ID
  unsigned char rule_id[MD5_DIGEST_LENGTH];
  MD5((unsigned char*) rule.c_str(), rule.size(), rule_id);
  std::ostringstream md5_ss;
  md5_ss << std::hex << std::setfill('0');
  for (long long c : rule_id) {
    md5_ss << std::setw(2) << (long long) c;
  }
  r->set_rule_id(md5_ss.str());

  // add conditions
  size_t pos = rule.find(RULE_DELIM);
  std::string condition_expr = rule.substr(0, pos);
  r->add_condition(condition_expr);

  // add actions
  std::string actions = rule.substr(pos + 2, rule.length());
  std::stringstream actions_ss(actions);
  std::string action;
  while (getline(actions_ss, action, DELIM)) {
    if (r->add_action(action) != NO_ERROR) {
      LOGGER_LOG_ERROR("Problems while adding action " << action << " to rule engine. "
          << "Ignoring action.");
      return ERROR_NO_RETRY;
    }
  }

  rules.push_back(std::move(r));
  return NO_ERROR;
}

std::vector<uint32_t> RuleEngine::evaluate_conditions(evt_t msg) {
  std::string value;
  std::vector<uint32_t> idx;

  for (uint32_t i = 0; i < rules.size(); i++) {
    if (rules[i]->eval_condition_expr(msg)) {
      idx.push_back(i);
    }
  }

  return idx;
}

int RuleEngine::run_actions(std::vector<uint32_t> rule_ids, evt_t msg) {
  for (uint32_t id : rule_ids) {
    rules[id]->run_actions(msg);
  }

  return NO_ERROR;
}

int RuleEngine::shutdown() {
  for (size_t i = 0; i < rules.size(); i++) {
    rules[i]->remove_actions();
  }
  return NO_ERROR;
}

/*------------------------------
 * Rule
 *------------------------------*/

int Rule::add_condition(std::string condition) {
  // lex and parse are called in the ConditionExpr constructor
  condition_expr = std::make_unique<ConditionExpr>(condition);
  return NO_ERROR;
}

int Rule::add_action(std::string action) {
  try {
    std::unique_ptr<Action> a = Action::parse_action(action);
    a->set_rule_id(this->rule_id);
    a->start_action_consumers(a->get_num_consumer_threads());
    actions.push_back(std::move(a));
    return NO_ERROR;
  } catch (const std::invalid_argument &e) {
    return ERROR_NO_RETRY;
  } catch (const DBConnectionException &e) {
    return ERROR_NO_RETRY;
  }
}

bool Rule::eval_condition_expr(evt_t msg) const {
  return condition_expr->eval(*msg);
}

void Rule::run_actions(evt_t msg) const {
  for (size_t i = 0; i < actions.size(); i++) {
    actions[i]->get_action_queue()->push(msg);
  }
}

void Rule::remove_actions() {
  for (size_t i = 0; i < actions.size(); i++) {
    actions[i]->stop_action_consumers();
  }
  actions.clear();
}

std::vector<std::string> Rule::get_action_types() {
  std::vector<std::string> types;
  for (size_t i = 0; i < actions.size(); i++) {
    types.push_back(actions[i]->get_type());
  }
  return types;
}
