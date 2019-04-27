#ifndef QUERYPLAN_H
#define QUERYPLAN_H

#include <iostream>
#include "Function.h"
#include "Optimizer.h"
#include "Statistics.h"

enum PlanNodeType {
  BASE_NODE,
  DUPLICATE_REMOVAL,
  GROUP_BY,
  JOIN,
  PROJECT,
  SELECT_FILE,
  SELECT_PIPE,
  SUM,
  RELATION_NODE
};

class BaseNode {
 protected:
 public:
  PlanNodeType nodeType;
  Schema *schema;
  BaseNode *left;
  BaseNode *right;
  BaseNode() {
    nodeType = BASE_NODE;
    left = NULL;
    right = NULL;
  }
  virtual void dummy(){};
};

class RelationNode : public BaseNode {
 public:
  char *relName;
  Schema *schema;
  RelationNode() {
    nodeType = RELATION_NODE;
    relName = NULL;
    schema = NULL;
  }
  void dummy() {}
};

class JoinNode : public BaseNode {
 public:
  CNF *cnf;
  Record *literal;
  Schema *schema;
  JoinNode() {
    nodeType = JOIN;
    cnf = NULL;
    literal = NULL;
    schema = NULL;
  }
  void dummy() {}
};

// class DuplicateRemovalNode : public BaseNode {
// } DuplicateRemovalNode;

// class GroupByNode : public BaseNode {
//   OrderMaker *o;
//   Function f;
// };

// class ProjectNode : BaseNode {
//   int *keepMe;
//   int numAttsInput;
//   int numAttsOutput;
// } ProjectNode;

// class SumNode : BaseNode {
//   Function *f;
// } SumNode;

class QueryPlan {
 public:
  Statistics *statsObject;
  QueryPlan(Statistics *s);
  void generateTree(JoinNode *joinNode);
  std::pair<std::string, std::string> SplitQualifiedAtt(std::string value);
  bool IsQualifiedAtt(std::string value);
};

#endif