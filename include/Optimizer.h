#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <list>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include "Statistics.h"
#include "StatisticsState.h"
#include "QueryPlan.h"

extern "C" {
typedef struct yy_buffer_state *YY_BUFFER_STATE;
int yyparse(void);  // defined in y.tab.c
YY_BUFFER_STATE yy_scan_string(const char *str);
void yy_delete_buffer(YY_BUFFER_STATE buffer);
}

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;        // the predicate in the WHERE clause
extern struct NameList *groupingAtts;  // grouping atts (NULL if no grouping)
extern struct NameList *
    attsToSelect;  // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts;  // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query
extern struct AndList *final;

struct Memo {
  double cost;
  long size;
  Statistics *state;
  struct BaseNode *root;
};

class Optimizer {
 public:
  Statistics *currentState;
  Optimizer();
  void Read(char *fromWhere);
  void ReadParserDatastructures();

  void OptimumOrderingOfJoin(
                             std::unordered_map<std::string, Schema *> relNameToSchema,
                             Statistics *prevStats,
                             std::vector<std::string> relNames,
                             std::vector<std::vector<std::string>> joinMatrix);

  bool ConstructJoinCNF(std::vector<std::string> relNames,
                        std::vector<std::vector<std::string>> joinMatrix,
                        std::string left, std::string right);

  std::vector<std::string> GenerateCombinations(int n, int r);

  std::vector<std::string> GetRelNamesFromBitSet(
      std::string bitset, std::vector<std::string> relNames);

  std::string GetMinimumOfPossibleCosts(
      std::map<std::string, double> possibleCosts);

  int BitSetDifferenceWithPrev(std::string set, std::string minCostString);

  void SeparateJoinsandSelects(
      std::vector<std::vector<std::string>> &joinMatrix);
  bool IsALiteral(Operand *op);
  bool ContainsLiteral(ComparisonOp *compOp);
};

#endif