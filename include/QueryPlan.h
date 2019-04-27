#ifndef QUERYPLAN_H
#define QUERYPLAN_H

#include <iostream>
#include "Function.h"
#include "Optimizer.h"
#include "Schema.h"

enum {
  DUPLICATE_REMOVAL,
  GROUP_BY,
  JOIN,
  PROJECT,
  SELECT_FILE,
  SELECT_PIPE,
  SUM
};

typedef struct BaseNode {
  int nodeType;
  Schema *schema;
  BaseNode *left;
  BaseNode *right;
} BaseNode;

typedef struct DuplicateRemovalNode : BaseNode {
} DuplicateRemovalNode;

typedef struct GroupByNode : BaseNode {
  OrderMaker *o;
  Function f;
} GroupByNode;

typedef struct JoinNode : BaseNode {
  CNF *cnf;
  Record *record;
} JoinNode, SelectFileNode, SelectPipeNode;

typedef struct ProjectNode : BaseNode {
  int *keepMe;
  int numAttsInput;
  int numAttsOutput;
} ProjectNode;

typedef struct SumNode : BaseNode {
  Function *f;
} SumNode;

typedef struct RelationNode : BaseNode {
  char *relName;
} RelationNode;

class QueryPlan {
 public:
  void generateTree(JoinNode *joinNode);
};

#endif