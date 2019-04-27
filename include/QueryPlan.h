#ifndef QUERYPLAN_H
#define QUERYPLAN_H

#include <iostream>
#include "Schema.h"
#include "Function.h"
#include "Optimizer.h"
#include "Statistics.h"

enum
{
    DUPLICATE_REMOVAL,
    GROUP_BY,
    JOIN,
    PROJECT,
    SELECT_FILE,
    SELECT_PIPE,
    SUM
};

typedef struct BaseNode
{
    int nodeType;
    Schema *schema;
    BaseNode *left;
    BaseNode *right;
} BaseNode;

typedef struct DuplicateRemovalNode : BaseNode
{
} DuplicateRemovalNode;

typedef struct GroupByNode : BaseNode
{
    OrderMaker *o;
    Function f;
} GroupByNode;

typedef struct JoinNode : BaseNode
{
    CNF *cnf;
    Record *record;
    Schema *schema;
} JoinNode, SelectNode;

typedef struct ProjectNode : BaseNode
{
    int *keepMe;
    int numAttsInput;
    int numAttsOutput;
} ProjectNode;

typedef struct SumNode : BaseNode
{
    Function *f;
} SumNode;

typedef struct RelationNode : BaseNode
{
    char *relName;
    Schema *schema;
} RelationNode;
class QueryPlan
{

public:
    Statistics *statsObject;
    QueryPlan(Statistics *s);
    void generateTree(JoinNode *joinNode);
    std::pair<std::string, std::string> SplitQualifiedAtt(
        std::string value);
    bool IsQualifiedAtt(std::string value);
};

#endif