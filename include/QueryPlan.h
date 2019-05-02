#ifndef QUERYPLAN_H
#define QUERYPLAN_H

#include <iostream>
#include "Comparison.h"
#include "Function.h"
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
  BaseNode() {
    nodeType = BASE_NODE;
    schema = NULL;
  }
  virtual void dummy(){};
};

class RelationNode : public BaseNode {
 public:
  char *relName;
  RelationNode() {
    nodeType = RELATION_NODE;
    relName = NULL;
  }
  void dummy() {}
};

class JoinNode : public BaseNode {
 public:
  CNF *cnf;
  Record *literal;
  JoinNode() {
    nodeType = JOIN;
    cnf = NULL;
    literal = NULL;
  }
  void dummy() {}
};

class SelectPipeNode : public JoinNode {};

class DuplicateRemovalNode : public BaseNode {
 public:
  DuplicateRemovalNode(){};
  void dummy() {}
};

class GroupByNode : public BaseNode {
 public:
  OrderMaker *o;
  Function *f;
  GroupByNode() {
    o = NULL;
    f = NULL;
  }
  void dummy() {}
};

class ProjectNode : public BaseNode {
 public:
  int *keepMe;
  int numAttsInput;
  int numAttsOutput;
  ProjectNode() {
    keepMe = NULL;
    numAttsInput = 0;
    numAttsOutput = 0;
  }
  void dummy() {}
};

class SumNode : public BaseNode {
 public:
  Function *f;
  SumNode() { f = NULL; }
  void dummy() {}
};

class QueryPlan {
 private:
  BaseNode *root;

 public:
  QueryPlan(BaseNode *root);
  void Execute();
  void Print();
  void PrintTree(BaseNode *base);
};
#endif