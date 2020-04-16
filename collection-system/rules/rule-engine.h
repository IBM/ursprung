/********************************************************* {COPYRIGHT-TOP} ***
* IBM Confidential
* OCO Source Materials
* MetaOcean
*
* (C) Copyright IBM Corp. 2017  All Rights Reserved.
*
* The source code for this program is not published or otherwise
* divested of its trade secrets, irrespective of what has been
* deposited with the U.S. Copyright Office.
********************************************************* {COPYRIGHT-END} **/

#ifndef RULE_ENGINE_H
#define RULE_ENGINE_H

#include <memory>
#include <iostream>
#include <string>
#include <vector>
#include <regex>
#include "action.h"
#include "condition.h"

class Rule;
typedef std::vector<std::unique_ptr<Rule>> Rules;
typedef std::vector<std::unique_ptr<Action>> Actions;

/**
 * Rules consist of a set of conditions and actions. Conditions
 * and actions are independent, i.e. if a rule has been determined
 * to apply, all corresponding actions will be executed.
 */
class Rule {
private:
  Actions actions;
  std::string rule_id;
  std::unique_ptr<ConditionExpr> condition_expr;

public:
  /*
   * Parses the condition expression defined in the provided string
   * and adds them to the rule.
   */
  int add_condition(std::string condition);
  /*
   * Parses the action defined in the provided string, adds the
   * action to the list of actions of this rule and starts the
   * action consumer(s).
   */
  int add_action(std::string action);
  /*
   * Removes all actions for this rule and stops all corresponding
   * action consumers.
   */
  void remove_actions();
  bool eval_condition_expr(std::shared_ptr<IntermediateMessage> msg) const;
  void run_actions(std::shared_ptr<IntermediateMessage> msg) const;

  void set_rule_id(std::string rid) { rule_id = rid; }
  std::string get_rule_id() { return rule_id; }
};

/**
 * A RuleEngine has a set of rules. Rules can be evaluated against an incoming
 * event and if a rule has been determined to apply, the corresponding actions
 * of that rule are executed.
 */
class RuleEngine {
private:
  Rules rules;

public:
  /*
   * The constructor takes a path to a rules file as an argument.
   * It parses the rules file (ignoring empty lines and lines
   * preceeded with a '#') and adds each rule to the engine's
   * list of rules.
   */
  RuleEngine(std::string rules_file);

  /*
   * Evaluates all rules of this engine against the incoming message
   * and determines the indexes of every rules whose conditions match
   * the incoming message.
   */
  std::vector<uint32_t> evaluate_conditions(std::shared_ptr<IntermediateMessage> msg);
  /* Runs the actions for the rules at the specified indexes. */
  int run_actions(std::vector<uint32_t> rule_ids, std::shared_ptr<IntermediateMessage> msg);
  int shutdown();

  int add_rule(std::string rule);
  bool has_rules() const { return (rules.size() > 0) ? true : false; }
};

#endif
