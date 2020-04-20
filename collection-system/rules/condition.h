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
#include "event.h"

/**
 * A condition consists of an fieldname on which the condition
 * should be evaluated, an operator and a value to compare to.
 *
 * The operator can be either an arithmetic comparison of the fieldname
 * to a number value (>,<,=) or a regex matching comparison on a string (@).
 */
class Condition {
private:
  std::string field_name;
  std::string op;
  std::string rvalue;

public:
  Condition(std::string condition);
  ~Condition() {}
  Condition(const Condition &other) {
    field_name = other.get_field_name();
    op = other.get_op();
    rvalue = other.get_rvalue();
  }
  Condition& operator=(const Condition &other) {
    this->field_name = other.get_field_name();
    this->op = other.get_op();
    this->rvalue = other.get_rvalue();
    return *this;
  }

  bool evaluate(std::string val) const;
  std::string get_field_name() const { return field_name; }
  std::string get_op() const { return op; }
  std::string get_rvalue() const { return rvalue; }
  std::string str() const { return field_name + op + rvalue; }
};

class Node {
public:
  virtual ~Node() {}
  virtual std::string str() = 0;
  virtual bool const is_leaf() = 0;
};

class OpNode: public Node {
public:
  OpNode(std::string o) : l_child { NULL }, r_child { NULL }, op { o } {}
  ~OpNode() {
    if (l_child) delete l_child;
    if (r_child) delete r_child;
  }
  OpNode(const OpNode &other) = delete;
  OpNode& operator=(const OpNode &other) = delete;

  void set_lchild(Node *l) { l_child = l; }
  void set_rchild(Node *r) { r_child = r; }
  Node* get_lchild() const { return l_child; }
  Node* get_rchild() const { return r_child; }
  std::string get_op() const { return op; }
  virtual std::string str() override { return this->op; }
  virtual bool const is_leaf() override { return false; }

private:
  Node *l_child;
  Node *r_child;
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

  Condition* get_cond() const { return cond; }
  virtual std::string str() override { return cond->str(); }
  virtual bool const is_leaf() override { return true; }

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

  TokenType get_type() const { return type; }
  std::string get_val() const { return val; }
  Condition* get_cond() const { return cond; }

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
  Node *ast_root;
  std::vector<Token*> tokens;

  Node* expr(std::vector<Token*> tokens, unsigned int &currTokenIdx) const;
  Node* term(std::vector<Token*> tokens, unsigned int &currTokenIdx) const;
  Node* factor(std::vector<Token*> tokens, unsigned int &currTokenIdx) const;
  bool eval_rec(Node *node, const Event &msg) const;
  void lex();
  void parse();

public:
  ConditionExpr(std::string e) :
      expression { e }, ast_root { NULL } {
    lex();
    parse();
  }
  ~ConditionExpr() {
    if (ast_root)
      delete ast_root;
    for (unsigned int i = 0; i < tokens.size(); i++) {
      delete tokens[i];
    }
  }
  ConditionExpr(const ConditionExpr &other) = delete;
  ConditionExpr& operator=(const ConditionExpr &other) = delete;

  bool eval(const Event &msg);
};

#endif /* RULES_CONDITION_H_ */
