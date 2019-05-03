#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <list>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include "QueryPlan.h"
#include "Statistics.h"
#include "StatisticsState.h"

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

class QueryOptimizer {
  // Class which helps us get to a optimzed version of a  query plan.
 private:
  Statistics *currentStats;
  std::unordered_map<std::string, RelationTuple *> *relNameToRelTuple;

 public:
  QueryOptimizer();
  QueryOptimizer(
      Statistics *currentStats,
      std::unordered_map<std::string, RelationTuple *> *relNameToRelTuple);

  // TODO: Only need this function to remain public
  QueryPlan *GetOptimizedPlan();

  // Helper functions(Temporarily public for testing)
  // Will be made private in future

  QueryPlan *GetOptimizedPlan(std::string query);
  QueryPlan *GetOptimizedPlanUtil();

  // Find the best order in which relations can be combined using Dynamic
  // programming
  BaseNode *OptimumOrderingOfJoin(
      std::unordered_map<std::string, RelationTuple *> relNameToRelTuple,
      Statistics *prevStats, std::vector<std::string> relNames,
      std::vector<std::vector<std::string>> joinMatrix);

  // Append all other nodes in the given command on top of this node
  BaseNode *GenerateTree(
      struct BaseNode *child,
      std::unordered_map<std::string, RelationTuple *> relNameToRelTuple);

  // Utility functions begin
  bool ConstructJoinCNF(std::vector<std::string> relNames,
                        std::vector<std::vector<std::string>> joinMatrix,
                        std::string left, std::string right);

  bool ConstructSelectFileAllTuplesCNF(Schema *schema, std::string relName);

  std::vector<std::string> GenerateCombinations(int n, int r);

  std::vector<std::string> GetRelNamesFromBitSet(
      std::string bitset, std::vector<std::string> relNames);

  std::string GetMinimumOfPossibleCosts(
      std::map<std::string, double> possibleCosts);

  int BitSetDifferenceWithPrev(std::string set, std::string minCostString);

  void SeparateJoinsandSelects(
      Statistics *currentStats,
      std::vector<std::vector<std::string>> &joinMatrix);

  bool IsQualifiedAtt(std::string value);
  std::pair<std::string, std::string> SplitQualifiedAtt(std::string value);
  bool IsALiteral(Operand *op);
  bool ContainsLiteral(ComparisonOp *compOp);
  // Utility functions end
};

#endif