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

#ifndef RULES_CONDITION_H_
#define RULES_CONDITION_H_

#include <string>
#include <vector>
#include <map>

#include "intermediate-message.h"

/**
 * A condition consists of an fieldname on which the condition
 * should be evaluated, an operator and a value to compare to.
 *
 * The operator can be either an arithmetic comparison of the fieldname
 * to a number value (>,<,=) or a regex matching comparison on a string (@).
 */
class Condition {
private:
  std::string fieldName;
  std::string op;
  std::string rvalue;

public:
  Condition(std::string condition);
  ~Condition() {}
  Condition(const Condition &other) {
    fieldName = other.getFieldName();
    op = other.getOp();
    rvalue = other.getRValue();
  }
  Condition& operator=(const Condition &other) {
    this->fieldName = other.getFieldName();
    this->op = other.getOp();
    this->rvalue = other.getRValue();
    return *this;
  }

  bool evaluate(std::string val) const;
  std::string getFieldName() const { return fieldName; }
  std::string getOp() const { return op; }
  std::string getRValue() const { return rvalue; }
  std::string str() const { return fieldName + op + rvalue; }
};

class Node {
public:
  virtual ~Node() {}
  virtual std::string str() = 0;
  virtual bool const isLeaf() = 0;
};

class OpNode: public Node {
public:
  OpNode(std::string o) : lChild { NULL }, rChild { NULL }, op { o } {}
  ~OpNode() {
    if (lChild) delete lChild;
    if (rChild) delete rChild;
  }
  OpNode(const OpNode &other) = delete;
  OpNode& operator=(const OpNode &other) = delete;

  void setLChild(Node *l) { lChild = l; }
  void setRChild(Node *r) { rChild = r; }
  Node* getLChild() const { return lChild; }
  Node* getRChild() const { return rChild; }
  std::string getOp() const { return op; }
  virtual std::string str() override { return this->op; }
  virtual bool const isLeaf() override { return false; }

private:
  Node *lChild;
  Node *rChild;
  std::string op;
};

class CondNode: public Node {
public:
  CondNode(Condition *c) : cond { c } {}
  ~CondNode() {
    if (cond) delete cond;
  }
  CondNode(const CondNode &other) = delete;
  CondNode& operator=(const CondNode &other) = delete;

  Condition* getCond() const { return cond; }
  virtual std::string str() override { return cond->str(); }
  virtual bool const isLeaf() override { return true; }

private:
  Condition *cond;
};

class Token {
public:
  enum TokenType {
    LPAREN,
    RPAREN,
    AND,
    OR,
    COND
  };

  Token(TokenType t, std::string v, Condition *c) : type { t }, val { v }, cond { c } {}
  ~Token() {
    if (cond) delete cond;
  }
  Token(const Token &other) = delete;
  Token& operator=(const Token &other) = delete;

  TokenType getType() const { return type; }
  std::string getVal() const { return val; }
  Condition* getCond() const { return cond; }

private:
  TokenType type;
  std::string val;
  Condition *cond;
};

/**
 * Represents a condition expression which consists of a series
 * of boolean conditions (var >/</=/@ value). Those conditions
 * can be concatenated using && and || operators and parentheses
 * to indicated precedence.
 */
class ConditionExpr {
private:
  std::string expression;
  Node *astRoot;
  std::vector<Token*> tokens;

  Node* expr(std::vector<Token*> tokens, unsigned int &currTokenIdx) const;
  Node* term(std::vector<Token*> tokens, unsigned int &currTokenIdx) const;
  Node* factor(std::vector<Token*> tokens, unsigned int &currTokenIdx) const;
  bool evalRec(Node *node, const IntermediateMessage &msg) const;

public:
  ConditionExpr(std::string e) :
      expression { e }, astRoot { NULL } {
  }
  ~ConditionExpr() {
    if (astRoot)
      delete astRoot;
    for (unsigned int i = 0; i < tokens.size(); i++) {
      delete tokens[i];
    }
  }
  ConditionExpr(const ConditionExpr &other) = delete;
  ConditionExpr& operator=(const ConditionExpr &other) = delete;

  void lex();
  void parse();
  bool eval(const IntermediateMessage &msg);
};

#endif /* RULES_CONDITION_H_ */
