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

    fieldName = condition.substr(0, pos);
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
  std::string::size_type exprSize = expression.size();
  for (std::string::size_type i = 0; i < exprSize; i++) {
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
      std::stringstream condStr;
      while (expression[i] != '(' && expression[i] != ')'
          && expression[i] != '&' && expression[i] != '|'
          && !isspace(expression[i]) && i < exprSize) {
        condStr << expression[i];
        i++;
      }
      // decrement back to the next control character
      i--;
      Condition *cond = new Condition(condStr.str());
      tokens.push_back(new Token(Token::TokenType::COND, "", cond));
    }
  }
}

void ConditionExpr::parse() {
  unsigned int currTokenIdx = 0;
  astRoot = expr(tokens, currTokenIdx);
}

bool ConditionExpr::eval(const IntermediateMessage &msg) {
  return evalRec(astRoot, msg);
}

Node* ConditionExpr::factor(std::vector<Token*> tokens, unsigned int &currTokenIdx) const {
  if (tokens[currTokenIdx]->getType() == Token::TokenType::COND) {
    CondNode *n = new CondNode(new Condition(*tokens[currTokenIdx]->getCond()));
    currTokenIdx++;
    return n;
  } else if (tokens[currTokenIdx]->getType() == Token::TokenType::LPAREN) {
    currTokenIdx++;
    Node *n = expr(tokens, currTokenIdx);
    if (!(tokens[currTokenIdx]->getType() == Token::TokenType::RPAREN)) {
      std::cout << "Missing ) in expression at " << currTokenIdx << std::endl;
      throw std::invalid_argument("Invalid expression!");
    }
    currTokenIdx++;
    return n;
  } else {
    std::cout << "Invalid token " << tokens[currTokenIdx]->getType()
        << " in expression at " << currTokenIdx << std::endl;
    throw std::invalid_argument("Invalid expression!");
  }
}

Node* ConditionExpr::term(std::vector<Token*> tokens, unsigned int &currTokenIdx) const {
  Node *latest = factor(tokens, currTokenIdx);

  while (currTokenIdx < tokens.size()
      && tokens[currTokenIdx]->getType() == Token::TokenType::AND) {
    OpNode *op = new OpNode(tokens[currTokenIdx]->getVal());
    op->setLChild(latest);
    currTokenIdx++;
    op->setRChild(factor(tokens, currTokenIdx));
    latest = op;
  }

  return latest;
}

Node* ConditionExpr::expr(std::vector<Token*> tokens, unsigned int &currTokenIdx) const {
  Node *latest = term(tokens, currTokenIdx);

  while (currTokenIdx < tokens.size()
      && tokens[currTokenIdx]->getType() == Token::TokenType::OR) {
    OpNode *op = new OpNode(tokens[currTokenIdx]->getVal());
    op->setLChild(latest);
    currTokenIdx++;
    op->setRChild(term(tokens, currTokenIdx));
    latest = op;
  }

  return latest;
}

bool ConditionExpr::evalRec(Node *node, const IntermediateMessage &msg) const {
  if (!node->isLeaf()) {
    OpNode *op = (OpNode*) node;
    if (op->getOp() == "&&") {
      return evalRec(op->getLChild(), msg) && evalRec(op->getRChild(), msg);
    } else if (op->getOp() == "||") {
      return evalRec(op->getLChild(), msg) || evalRec(op->getRChild(), msg);
    } else {
      LOG_ERROR("Op not recognized");
      return false;
    }
  } else {
    Condition *c = ((CondNode*) node)->getCond();
    std::string val = msg.get_value(c->getFieldName());
    if (!val.empty()) {
      return c->evaluate(val);
    } else {
      LOG_ERROR("Field " << c->getFieldName() << " not part of message." << "Ignoring rule.");
      return false;
    }
  }
}
