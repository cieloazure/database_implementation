#ifndef QUERYPLAN_H
#define QUERYPLAN_H

#include <iostream>
#include <unordered_map>
#include "Comparison.h"
#include "DBFile.h"
#include "Function.h"
#include "GroupBy.h"
#include "Join.h"
#include "Pipe.h"
#include "Project.h"
#include "Record.h"
#include "Schema.h"
#include "SelectFile.h"
#include "SelectPipe.h"
#include "Sum.h"
#include "WriteOut.h"

extern char *whereToGiveOutput;

enum PlanNodeType {
  BASE_NODE,
  DUPLICATE_REMOVAL,
  GROUP_BY,
  JOIN,
  PROJECT,
  SELECT_FILE,
  SELECT_PIPE,
  SUM,
  RELATION_NODE,
  WRITE_OUT
};

class BaseNode;

struct Link {
  BaseNode *value;
  BaseNode *rvalue;
  Pipe *pipe;
  int id;

  static int pool;

  Link() {
    value = NULL;
    pipe = NULL;
    id = -1;
  }

  Link(BaseNode *val, BaseNode *rval) {
    value = val;
    rvalue = rval;
    id = Link::pool;
    Link::pool++;
    pipe = NULL;
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
  DBFile *dbFile;
  CNF *cnf;
  Record *literal;
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
  OrderMaker *groupAtts;
  Function *computeMe;
  GroupByNode() {
    nodeType = GROUP_BY;
    groupAtts = NULL;
    computeMe = NULL;
  }
  void dummy() {}
};

class ProjectNode : public BaseNode {
 public:
  int *keepMe;
  int numAttsInput;
  int numAttsOutput;
  ProjectNode() {
    nodeType = PROJECT;
    keepMe = NULL;
    numAttsInput = 0;
    numAttsOutput = 0;
  }
  void dummy() {}
};

class SumNode : public BaseNode {
 public:
  Function *computeMe;
  SumNode() {
    nodeType = SUM;
    computeMe = NULL;
  }
  void dummy() {}
};

class WriteOutNode : public BaseNode {
 public:
  FILE *file;
  WriteOutNode() {
    nodeType = WRITE_OUT;
    file = NULL;
  }
  void dummy() {}
};

enum WhereOutput { StdOut, None, File, Undefined };

struct SortInfo {
  OrderMaker *sortOrder;
  int runLength;

  SortInfo(OrderMaker *so, int rl) {
    sortOrder = so;
    runLength = rl;
  }
};

struct RelationTuple {
  std::string relName;
  Schema *schema;
  DBFile *dbFile;
  RelationTuple() {}
  RelationTuple(Schema *s, DBFile *db) {
    schema = s;
    dbFile = db;
  }
  RelationTuple(Schema *s) { schema = s; }
};

class QueryPlan {
 private:
  BaseNode *root;

 public:
  QueryPlan(BaseNode *root);
  void Execute();
  void Print();
  void PrintTree(BaseNode *base);
  void SetOutput(WhereOutput op);
  void CreateConcretePipes(BaseNode *iter,
                           std::unordered_map<int, Pipe *> &idToPipe);

  void CreateRelationalOperators(BaseNode *iter,
                                 std::unordered_map<int, Pipe *> &idToPipe,
                                 std::vector<RelationalOp *> &operators);
};
#endif