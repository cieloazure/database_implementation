#include "ParseTreePrinter.h"

void ParseTreePrinter::PrintAndList(struct AndList *boolean) {
  if (boolean == NULL) {
    std::cout << "null]" << std::endl;
    return;
  }
  std::cout << "WHERE CONDITION -> [";
  PrintAndListHelper(boolean);
  std::cout << "]" << std::endl;
}

void ParseTreePrinter::PrintAndListHelper(struct AndList *boolean) {
  if (boolean != NULL) {
    struct OrList *pOr = boolean->left;
    PrintOrList(pOr);
    if (boolean->rightAnd) {
      std::cout << " AND ";
      PrintAndListHelper(boolean->rightAnd);
    }
  } else {
    return;
  }
}

void ParseTreePrinter::PrintFuncOperator(struct FuncOperator *func) {
  std::cout << "FUNCTION -> Aggregation Function ";
  if (func == NULL) {
    std::cout << "not present!" << std::endl;
    return;
  } else {
    std::cout << "present! Don't know how to "
                 "print it for understanding!"
              << std::endl;
  }
  // PrintFuncOperatorHelper(func);
}

void ParseTreePrinter::PrintFuncOperatorHelper(struct FuncOperator *func) {
  if (func != NULL) {
    PrintFuncOperand(func->leftOperand);
    std::cout << "  " << func->code;
    PrintFuncOperatorHelper(func->right);
  } else {
    return;
  }
}

void ParseTreePrinter::PrintFuncOperand(struct FuncOperand *fo) {
  if (fo != NULL) {
    std::cout << "    " << fo->value;
  } else {
    return;
  }
}

void ParseTreePrinter::PrintTableList(struct TableList *tables) {
  std::cout << "TABLES -> [";
  if (tables == NULL) {
    std::cout << "null]" << std::endl;
    return;
  }

  struct TableList *temp = tables;
  while (temp->next != NULL) {
    std::cout << temp->tableName << " ALIASED AS " << temp->aliasAs << ", ";
    temp = temp->next;
  }
  std::cout << temp->tableName << " ALIASED AS " << temp->aliasAs << "]"
            << std::endl;
}

void ParseTreePrinter::PrintNameList(struct NameList *names) {
  std::cout << "NAMES -> [";
  if (names == NULL) {
    std::cout << "null]" << std::endl;
    return;
  }
  struct NameList *temp = names;
  while (temp->next != NULL) {
    std::cout << temp->name << ", ";
    temp = temp->next;
  }
  std::cout << temp->name << "]" << std::endl;
}

void ParseTreePrinter::PrintOperand(struct Operand *pOperand) {
  if (pOperand != NULL) {
    std::cout << pOperand->value << " ";
  } else
    return;
}

void ParseTreePrinter::PrintComparisonOp(struct ComparisonOp *pCom) {
  if (pCom != NULL) {
    PrintOperand(pCom->left);
    switch (pCom->code) {
      case LESS_THAN:
        std::cout << " < ";
        break;
      case GREATER_THAN:
        std::cout << " > ";
        break;
      case EQUALS:
        std::cout << " = ";
    }
    PrintOperand(pCom->right);

  } else {
    return;
  }
}

void ParseTreePrinter::PrintOrList(struct OrList *pOr) {
  if (pOr != NULL) {
    struct ComparisonOp *pCom = pOr->left;
    PrintComparisonOp(pCom);

    if (pOr->rightOr) {
      std::cout << " OR ";
      PrintOrList(pOr->rightOr);
    }
  } else {
    return;
  }
}

void ParseTreePrinter::PrintSQL(struct TableList *tables,
                                struct NameList *attsToSelect,
                                struct NameList *groupingAtts,
                                struct AndList *boolean,
                                struct FuncOperator *func) {
  ParseTreePrinter::PrintTableList(tables);
  std::cout << "SELECT ATTRIBUTE ";
  ParseTreePrinter::PrintNameList(attsToSelect);
  std::cout << "GROUPING ATTRIBUTE ";
  ParseTreePrinter::PrintNameList(groupingAtts);
  ParseTreePrinter::PrintAndList(boolean);
  ParseTreePrinter::PrintFuncOperator(func);
  std::cout << std::endl;
}