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

#include <iostream>
#include <vector>
#include <sstream>
#include <boost/regex.hpp>
#include <cctype>

#include "condition.h"
#include "logger.h"
#include "error.h"

/*
 * Condition operators can be +,>,< for arithmetic comparisons
 * while @ specifies a regex match on the rvalue
 */
const boost::regex FIND_OP_REGEX = boost::regex("(=|>|<|@)");

/*------------------------------
 * Condition
 *------------------------------*/

Condition::Condition(std::string condition) {
  boost::smatch match;

  if (boost::regex_search(condition, match, FIND_OP_REGEX)) {
    int pos = match.position();

    field_name = condition.substr(0, pos);
    op = condition.substr(pos, match.length());
    rvalue = condition.substr(pos + match.length());
    rvalue = boost::regex_replace(rvalue, boost::regex("\\["), "(");
    rvalue = boost::regex_replace(rvalue, boost::regex("\\]"), ")");
  } else {
    LOG_WARN("No operator found in condition " << condition << "Valid operators are =, >, <, and @");
  }
}

bool Condition::evaluate(std::string val) const {
  if (op == ">")
    return std::stod(val) > std::stod(rvalue);
  if (op == "<")
    return std::stod(val) < std::stod(rvalue);
  if (op == "=")
    return std::stod(val) == std::stod(rvalue);
  if (op == "@")
    return boost::regex_match(val, boost::regex(rvalue));

  return false;
}

/*------------------------------
 * ConditionExpr
 *------------------------------*/

void ConditionExpr::lex() {
  std::string::size_type expr_size = expression.size();
  for (std::string::size_type i = 0; i < expr_size; i++) {
    if (isspace(expression[i])) {
      continue;
    } else if (expression[i] == '(') {
      tokens.push_back(new Token(Token::TokenType::LPAREN, "(", NULL));
    } else if (expression[i] == ')') {
      tokens.push_back(new Token(Token::TokenType::RPAREN, "(", NULL));
    } else if (expression[i] == '&') {
      tokens.push_back(new Token(Token::TokenType::AND, "&&", NULL));
      // increment i for the second '&'
      i++;
    } else if (expression[i] == '|') {
      tokens.push_back(new Token(Token::TokenType::OR, "||", NULL));
      // increment i for the second '|'
      i++;
    } else {
      // parse the condition
      std::stringstream cond_str;
      while (expression[i] != '(' && expression[i] != ')'
          && expression[i] != '&' && expression[i] != '|'
          && !isspace(expression[i]) && i < expr_size) {
        cond_str << expression[i];
        i++;
      }
      // decrement back to the next control character
      i--;
      Condition *cond = new Condition(cond_str.str());
      tokens.push_back(new Token(Token::TokenType::COND, "", cond));
    }
  }
}

void ConditionExpr::parse() {
  unsigned int curr_token_idx = 0;
  ast_root = expr(tokens, curr_token_idx);
}

bool ConditionExpr::eval(const IntermediateMessage &msg) {
  return eval_rec(ast_root, msg);
}

Node* ConditionExpr::factor(std::vector<Token*> tokens, unsigned int &curr_token_idx) const {
  if (tokens[curr_token_idx]->get_type() == Token::TokenType::COND) {
    CondNode *n = new CondNode(new Condition(*tokens[curr_token_idx]->get_cond()));
    curr_token_idx++;
    return n;
  } else if (tokens[curr_token_idx]->get_type() == Token::TokenType::LPAREN) {
    curr_token_idx++;
    Node *n = expr(tokens, curr_token_idx);
    if (!(tokens[curr_token_idx]->get_type() == Token::TokenType::RPAREN)) {
      std::cout << "Missing ) in expression at " << curr_token_idx << std::endl;
      throw std::invalid_argument("Invalid expression!");
    }
    curr_token_idx++;
    return n;
  } else {
    std::cout << "Invalid token " << tokens[curr_token_idx]->get_type()
        << " in expression at " << curr_token_idx << std::endl;
    throw std::invalid_argument("Invalid expression!");
  }
}

Node* ConditionExpr::term(std::vector<Token*> tokens, unsigned int &curr_token_idx) const {
  Node *latest = factor(tokens, curr_token_idx);

  while (curr_token_idx < tokens.size()
      && tokens[curr_token_idx]->get_type() == Token::TokenType::AND) {
    OpNode *op = new OpNode(tokens[curr_token_idx]->get_val());
    op->set_lchild(latest);
    curr_token_idx++;
    op->set_rchild(factor(tokens, curr_token_idx));
    latest = op;
  }

  return latest;
}

Node* ConditionExpr::expr(std::vector<Token*> tokens, unsigned int &curr_token_idx) const {
  Node *latest = term(tokens, curr_token_idx);

  while (curr_token_idx < tokens.size()
      && tokens[curr_token_idx]->get_type() == Token::TokenType::OR) {
    OpNode *op = new OpNode(tokens[curr_token_idx]->get_val());
    op->set_lchild(latest);
    curr_token_idx++;
    op->set_rchild(term(tokens, curr_token_idx));
    latest = op;
  }

  return latest;
}

bool ConditionExpr::eval_rec(Node *node, const IntermediateMessage &msg) const {
  if (!node->is_leaf()) {
    OpNode *op = (OpNode*) node;
    if (op->get_op() == "&&") {
      return eval_rec(op->get_lchild(), msg) && eval_rec(op->get_rchild(), msg);
    } else if (op->get_op() == "||") {
      return eval_rec(op->get_lchild(), msg) || eval_rec(op->get_rchild(), msg);
    } else {
      LOG_ERROR("Op not recognized");
      return false;
    }
  } else {
    Condition *c = ((CondNode*) node)->get_cond();
    std::string val = msg.get_value(c->get_field_name());
    if (!val.empty()) {
      return c->evaluate(val);
    } else {
      LOG_ERROR("Field " << c->get_field_name() << " not part of message." << "Ignoring rule.");
      return false;
    }
  }
}
