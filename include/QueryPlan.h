#ifndef QUERYPLAN_H
#define QUERYPLAN_H

#include "Comparison.h"
#include "Pipe.h"
#include "Record.h"
#include "Schema.h"

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

class BaseNode;

struct Link {
  BaseNode *value;
  Pipe *pipe;
  int id;

  static int pool;

  Link() {
    value = NULL;
    pipe = NULL;
    id = -1;
  }

  Link(BaseNode *val) {
    value = val;
    id = Link::pool;
    Link::pool++;
  }
};

class BaseNode {
 protected:
 public:
  PlanNodeType nodeType;
  Schema *schema;
  Link left;
  Link right;
  Link parent;
  BaseNode() { nodeType = BASE_NODE; }
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
 private:
  BaseNode *root;

 public:
  QueryPlan(BaseNode *root);
  void Execute();
  void Print();
};
#endif