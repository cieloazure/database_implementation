#ifndef STATISTICS_H
#define STATISTICS_H

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
#include "ParseTree.h"
#include "StatisticsState.h"

namespace dbi {}
class Statistics {
 public:
  StatisticsState *currentState;
  /* Core functions */
  Statistics();
  Statistics(Statistics &copyMe);  // Performs deep copy
  ~Statistics();

  void AddRel(char *relName, int numTuples);
  void AddAtt(char *relName, char *attName, int numDistincts);
  void CopyRel(char *oldName, char *newName);
  void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
  double Estimate(struct AndList *parseTree, char *relNames[], int numToJoin);
  void Read(char *fromWhere);
  void Write(char *toWhere);
  /* Core functions ends */

  /* Helper functions */

  // Helpers to check errors in relNames for Estimate/Apply
  bool CheckAttNameInRel(struct AndList *parseTree,
                         std::vector<std::string> relNames);
  bool CheckAndList(struct AndList *andList, std::vector<std::string> relNames);
  bool CheckOrList(struct OrList *orList, std::vector<std::string> relNames);
  bool CheckOperand(struct Operand *operand, std::vector<std::string> relNames);

  /* Utility functions to calculate cost of AND & OR list */
  void CalculateCostAndList(AndList *andList, std::vector<double> &costs,
                            std::vector<std::string> &relNames,
                            StatisticsState *currentState);

  void CalculateCostOrList(OrList *orList, std::vector<double> &costs,
                           std::vector<std::string> &relNames,
                           StatisticsState *currentState);

  /* Helper Functions to calculate OrList cost */
  void CalculateCostOrListHelper(
      OrList *orList, std::vector<double> &costs,
      std::vector<std::vector<double>> &independentCosts,
      std::vector<std::string> &relNames, StatisticsState *currentState);
  double ApplySelectionOrFormulaList(std::vector<double> orListsCost,
                                     int totalTuples);
  double ApplySelectionOrFormula(double distinctValuesOr1,
                                 double distinctValuesOr2, int totalTuples);

  /* Utility function to calculate cost of various operators */
  std::pair<double, int> CalculateCostSelectionEquality(
      ComparisonOp *compOp, std::vector<std::string> relNames,
      std::vector<double> costs, StatisticsState *currentState);

  std::pair<double, int> CalculateCostSelectionInequality(
      ComparisonOp *compOp, std::vector<std::string> relNames,
      std::vector<double> costs, StatisticsState *currentState);

  std::tuple<double, std::string, std::pair<int, int>> CalculateCostJoin(
      ComparisonOp *compOp, std::vector<std::string> &relNames,
      std::vector<double> &costs, StatisticsState *currentState);

  /* Utility functions */
  bool IsALiteral(Operand *op);
  bool ContainsLiteral(ComparisonOp *compOp);

  double CalculateCost(AndList *parseTree, char *relNames[], int numToJoin,
                       StatisticsState *currentState);

  // Required to debug/test state variables
  void PrintRelationStore();
  void PrintAttributeStore();
  void PrintDisjointSets();
  /* Helpers end */
};

#endif
