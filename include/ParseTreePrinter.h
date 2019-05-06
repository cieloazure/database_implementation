#ifndef PARSE_TREE_PRINTER_H
#define PARSE_TREE_PRINTER_H

#include <iostream>
#include "ParseTree.h"

extern struct FuncOperator
    *finalFunction;               // the aggregate function (NULL if no agg)
extern struct TableList *tables;  // the list of tables and aliases in the query
extern struct AndList *boolean;   // the predicate in the WHERE clause
extern struct NameList *groupingAtts;  // grouping atts (NULL if no grouping)
extern struct NameList *
    attsToSelect;  // the set of attributes in the SELECT (NULL if no such atts)

class ParseTreePrinter {
  // Utility functions to print the parsing strucutres
 public:
  static void PrintAndList(struct AndList *boolean);
  static void PrintFuncOperator(struct FuncOperator *finalFunction);
  static void PrintTableList(struct TableList *tables);
  static void PrintNameList(struct NameList *atts);
  static void PrintOrList(struct OrList *pOr);
  static void PrintComparisonOp(struct ComparisonOp *pCom);
  static void PrintOperand(struct Operand *pOperand);
  static void PrintAndListHelper(struct AndList *boolean);
  static void PrintSQL(struct TableList *tables, struct NameList *attsToSelect,
                       struct NameList *groupingAtts, struct AndList *boolean,
                       struct FuncOperator *finalFunction);

  static void PrintFuncOperatorHelper(struct FuncOperator *func);
  static void PrintFuncOperand(struct FuncOperand *fo);
};
#endif