#include "QueryOptimizer.h"

QueryOptimizer::QueryOptimizer() {
  currentStats = NULL;
  relNameToSchema = NULL;
}
QueryOptimizer::QueryOptimizer(
    Statistics *stats,
    std::unordered_map<std::string, Schema *> *relNameToSchemaMap) {
  currentStats = stats;
  relNameToSchema = relNameToSchemaMap;
}

BaseNode *QueryOptimizer::OptimumOrderingOfJoin(
    std::unordered_map<std::string, Schema *> relNameToSchema,
    Statistics *prevStats, std::vector<std::string> relNames,
    std::vector<std::vector<std::string>> joinMatrix) {
  // Start Optimization

  // Decl
  std::unordered_map<std::string, struct Memo> combinationToMemo;

  // print memo
  auto print = [&combinationToMemo]() -> void {
    for (auto it = combinationToMemo.begin(); it != combinationToMemo.end();
         it++) {
      std::string key = it->first;
      for (int i = 0; i < key.size(); i++) {
        if (key[i]) {
          std::cout << "1";
        } else {
          std::cout << "0";
        }
      }
      std::cout << " ->  {";
      struct Memo val = it->second;
      std::cout << "Size: " << val.size << ",";
      std::cout << "Cost: " << val.cost << "}" << std::endl;
    }
  };

  // Initialize for singletons
  int length = relNames.size();
  if (length < 2) {
    return NULL;
  }

  auto singletons = GenerateCombinations(length, 1);
  for (auto set : singletons) {
    std::vector<std::string> relNamesSubset =
        GetRelNamesFromBitSet(set, relNames);
    struct Memo newMemo;
    newMemo.cost = 0;
    newMemo.size = prevStats->GetRelSize(relNamesSubset[0]);  // get from stats
    newMemo.state = prevStats;

    // Set RelationNode for newMemo
    RelationNode *relNode = new RelationNode;
    char *temp = (char *)relNamesSubset[0].c_str();
    char *dest = new char[relNamesSubset[0].size()];
    std::strcpy(dest, temp);
    relNode->relName = dest;
    relNode->schema = relNameToSchema[relNode->relName];
    relNode->nodeType = RELATION_NODE;

    // Set the root for newMemo
    BaseNode *root = new BaseNode;

    // Set links of root
    Link sentinelLink(relNode);
    root->left = sentinelLink;
    relNode->parent = sentinelLink;

    newMemo.root = root;
    PrintTree(newMemo.root);
    std::cout << std::endl;

    combinationToMemo[set] = newMemo;
  }

  print();

  // Initialize doubletons
  auto doubletons = GenerateCombinations(length, 2);
  for (auto set : doubletons) {
    std::vector<std::string> relNamesSubset =
        GetRelNamesFromBitSet(set, relNames);
    Statistics *prevStatsCopy = new Statistics(*prevStats);
    const char *relNamesCStyle[2];
    std::vector<BaseNode *> joinPair(2, NULL);
    int cStyleIdx = 0;

    for (int stridx = 0; stridx < set.size(); stridx++) {
      if (set[stridx]) {
        // unset the bit if it is set
        set[stridx] = '\0';
        // Search the memoized table and get the size in possible cost
        struct Memo prevMemo = combinationToMemo[set];
        relNamesCStyle[cStyleIdx] = relNamesSubset[cStyleIdx].c_str();
        joinPair[cStyleIdx] = prevMemo.root;
        cStyleIdx++;
        // Set the bit again
        set[stridx] = '\x01';
      }
    }

    // Construct new memo
    struct Memo newMemo;
    newMemo.cost = 0;
    if (!ConstructJoinCNF(relNames, joinMatrix, relNamesCStyle[0],
                          relNamesCStyle[1])) {
      final = NULL;
    }
    newMemo.size = prevStatsCopy->Estimate(final, (char **)relNamesCStyle, 2);
    prevStatsCopy->Apply(final, (char **)relNamesCStyle, 2);
    newMemo.state = prevStatsCopy;

    // Set Join Node for newMemo
    JoinNode *newJoinNode = new JoinNode;
    newJoinNode->nodeType = JOIN;
    RelationNode *relNode1 =
        dynamic_cast<RelationNode *>(joinPair[1]->left.value);
    RelationNode *relNode2 =
        dynamic_cast<RelationNode *>(joinPair[0]->left.value);

    // Set left link of new join node
    Link leftLink(relNode1);
    newJoinNode->left = leftLink;
    relNode1->parent = leftLink;

    // Set right link of new join node
    Link rightLink(relNode2);
    newJoinNode->right = rightLink;
    relNode2->parent = rightLink;

    if (final != NULL) {
      CNF cnf;
      Record literal;
      cnf.GrowFromParseTree(final, relNode1->schema, relNode2->schema, literal);
      OrderMaker left;
      OrderMaker right;
      cnf.GetSortOrders(left, right);
      Schema *s =
          new Schema("join_schema", relNode1->schema, relNode2->schema, &right);
      newJoinNode->schema = s;
      newJoinNode->cnf = &cnf;
      newJoinNode->literal = &literal;
    } else {
      Schema *s = new Schema("join_schema", relNode2->schema, relNode1->schema);
      newJoinNode->schema = s;
    }
    // Set root node for newMemo
    BaseNode *root = new BaseNode;
    Link sentinelLink(newJoinNode);
    root->left = sentinelLink;
    newJoinNode->parent = sentinelLink;

    newMemo.root = root;
    PrintTree(newMemo.root);
    std::cout << std::endl;

    combinationToMemo[set] = newMemo;
    if (length == 2) {
      return newMemo.root;
    }
  }

  print();

  // DP begins
  for (int idx = 3; idx < length + 1; idx++) {
    auto combinations = GenerateCombinations(length, idx);
    for (auto set : combinations) {
      std::vector<std::string> relNamesSubset =
          GetRelNamesFromBitSet(set, relNames);
      std::map<std::string, double> possibleCosts;
      // Iterate through each character of string and get the previous cost
      // stored in the table
      // then choose min of the prev cost
      // Get prev combination by unsetting the bit that was previously set
      possibleCosts.clear();
      for (int stridx = 0; stridx < set.size(); stridx++) {
        // unset the bit if it is set
        if (set[stridx]) {
          set[stridx] = '\0';
          // Search the memoized table and get the size in possible cost
          struct Memo prevMemo = combinationToMemo[set];
          possibleCosts[set] = prevMemo.size;
          set[stridx] = '\x01';
        }
      }

      // Get the minimum cost
      std::string minCostString = GetMinimumOfPossibleCosts(possibleCosts);

      // Join it with the minimum cost and store the state, update combinations
      // map
      struct Memo prevMemo = combinationToMemo[minCostString];
      const char *relNamesCStyle[relNamesSubset.size()];
      for (int i = 0; i < relNamesSubset.size(); i++) {
        relNamesCStyle[i] = relNamesSubset[i].c_str();
      }

      int joinWith = BitSetDifferenceWithPrev(set, minCostString);
      std::string right = relNames[joinWith];
      int leftIdx = 0;
      for (; leftIdx < minCostString.size(); leftIdx++) {
        if (minCostString[leftIdx]) {
          std::string left = relNames[leftIdx];
          if (ConstructJoinCNF(relNames, joinMatrix, left, right)) {
            break;
          }
        }
      }
      Statistics *prevStatsCopy = new Statistics(*prevMemo.state);

      struct Memo newMemo;
      newMemo.cost = prevMemo.size + prevMemo.cost;
      newMemo.size =
          prevStatsCopy->Estimate(final, (char **)relNamesCStyle, idx);
      prevStatsCopy->Apply(final, (char **)relNamesCStyle, idx);
      newMemo.state = prevStatsCopy;

      // Set join node for newMemo
      JoinNode *prevJoinNode =
          dynamic_cast<JoinNode *>(prevMemo.root->left.value);
      RelationNode *newRelNode = new RelationNode;
      newRelNode->nodeType = RELATION_NODE;
      char *temp = (char *)right.c_str();
      char *dest = new char[right.size()];
      std::strcpy(dest, temp);
      newRelNode->relName = dest;
      newRelNode->schema = relNameToSchema[right];
      JoinNode *newJoinNode = new JoinNode;
      newJoinNode->nodeType = JOIN;

      // Set left link of new join node
      Link leftLink(prevJoinNode);
      newJoinNode->left = leftLink;
      prevJoinNode->parent = leftLink;

      // Set right link of new join node
      Link rightLink(newRelNode);
      newJoinNode->right = rightLink;
      newRelNode->parent = rightLink;

      if (final != NULL) {
        CNF cnf;
        Record literal;
        cnf.GrowFromParseTree(final, prevJoinNode->schema, newRelNode->schema,
                              literal);
        OrderMaker left;
        OrderMaker right;
        cnf.GetSortOrders(left, right);
        Schema *s = new Schema("join_schema", prevJoinNode->schema,
                               newRelNode->schema, &right);
        newJoinNode->schema = s;
        newJoinNode->cnf = &cnf;
        newJoinNode->literal = &literal;
      } else {
        Schema s("join_schema", newRelNode->schema, prevJoinNode->schema);
        newJoinNode->schema = &s;
      }
      // Set root node for newMemo
      BaseNode *root = new BaseNode;
      Link sentinelLink(newJoinNode);
      root->left = sentinelLink;
      newJoinNode->parent = sentinelLink;

      newMemo.root = root;
      PrintTree(newMemo.root);
      std::cout << std::endl;

      combinationToMemo[set] = newMemo;
    }

    print();
  }
  // DP ends

  // Return result
  std::string optimalJoin = *(GenerateCombinations(length, length).begin());
  std::cout << "Cost of optimal join:" << combinationToMemo[optimalJoin].cost
            << std::endl;
  std::cout << "Order of join" << std::endl;
  PrintTree(combinationToMemo[optimalJoin].root);
  std::cout << std::endl;
  // End Optimization

  return combinationToMemo[optimalJoin].root;
}

std::vector<std::string> QueryOptimizer::GenerateCombinations(int n, int r) {
  std::vector<std::string> combinations;
  std::string bitmask(r, 1);
  bitmask.resize(n, 0);
  do {
    combinations.push_back(bitmask);
  } while (std::prev_permutation(bitmask.begin(), bitmask.end()));

  return combinations;
}

std::vector<std::string> QueryOptimizer::GetRelNamesFromBitSet(
    std::string bitset, std::vector<std::string> relNames) {
  std::vector<std::string> subset;
  for (int i = 0; i < bitset.size(); i++) {
    if (bitset[i]) {
      subset.push_back(relNames[i]);
    }
  }
  return subset;
}

bool QueryOptimizer::ConstructJoinCNF(
    std::vector<std::string> relNames,
    std::vector<std::vector<std::string>> joinMatrix, std::string left,
    std::string right) {
  auto leftIter = std::find(relNames.begin(), relNames.end(), left);
  int idxLeft = std::distance(relNames.begin(), leftIter);

  auto rightIter = std::find(relNames.begin(), relNames.end(), right);
  int idxRight = std::distance(relNames.begin(), rightIter);

  std::string cnfString;
  cnfString.append("(");
  cnfString.append(left);
  cnfString.append(".");
  if (joinMatrix[idxLeft][idxRight].size() > 0) {
    cnfString.append(joinMatrix[idxLeft][idxRight]);
  } else {
    return false;
  }
  cnfString.append(" = ");
  cnfString.append(right);
  cnfString.append(".");
  cnfString.append(joinMatrix[idxRight][idxLeft]);
  cnfString.append(")");
  std::cout << "Joining...." << cnfString << std::endl;
  yy_scan_string(cnfString.c_str());
  yyparse();
  return true;
}

std::string QueryOptimizer::GetMinimumOfPossibleCosts(
    std::map<std::string, double> possibleCosts) {
  bool begin = true;
  double min = -1.0;
  std::string minCostString;

  for (auto it = possibleCosts.begin(); it != possibleCosts.end(); it++) {
    double currCost = it->second;
    if (begin || currCost < min) {
      min = currCost;
      minCostString = it->first;
      if (begin) begin = false;
    }
  }
  return minCostString;
}

int QueryOptimizer::BitSetDifferenceWithPrev(std::string set,
                                             std::string minCostString) {
  for (int i = 0; i < set.size(); i++) {
    if (set[i] != minCostString[i]) {
      return i;
    }
  }
  return -1;
}

void QueryOptimizer::SeparateJoinsandSelects(
    Statistics *currentStats,
    std::vector<std::vector<std::string>> &joinMatrix) {
  OrList *orList;
  AndList *head = boolean;
  AndList *current = boolean;
  AndList *prev = boolean;

  // generate a table list that will help with filling the matrix
  std::vector<std::string> tableList;
  struct TableList *table = tables;

  while (table) {
    tableList.push_back(table->tableName);
    table = table->next;
  }

  for (int vecSize = 0; vecSize < tableList.size(); ++vecSize) {
    std::vector<std::string> dummy(tableList.size(), std::string(""));
    joinMatrix.push_back(dummy);
  }

  // start populating the matrix
  while (current) {
    orList = current->left;
    if (!orList) {
      // andList empty, throw error?
      return;
    }
    struct ComparisonOp *compOp = orList->left;
    if (compOp != NULL) {
      if (!ContainsLiteral(compOp)) {
        // join operation
        char *operand1 = compOp->left->value;
        char *operand2 = compOp->right->value;

        // find index of these operands in the tables vector
        AttributeStats *attr1 = currentStats->GetRelationNameOfAttribute(
            operand1, tableList, tables);
        AttributeStats *attr2 = currentStats->GetRelationNameOfAttribute(
            operand2, tableList, tables);

        if (attr1 && attr2) {
          std::ptrdiff_t operandOneIndex = std::distance(
              tableList.begin(),
              std::find(tableList.begin(), tableList.end(), attr1->relName));
          std::ptrdiff_t operandTwoIndex = std::distance(
              tableList.begin(),
              std::find(tableList.begin(), tableList.end(), attr2->relName));

          // TODO: check if index out of range?
          joinMatrix[operandOneIndex][operandTwoIndex] =
              std::string(attr2->attName);
          joinMatrix[operandTwoIndex][operandOneIndex] =
              std::string(attr1->attName);
        }

        // pop this join op from the andList
        if (current == head) {
          head = head->rightAnd;
          boolean = boolean->rightAnd;
        } else {
          prev->rightAnd = current->rightAnd;
        }

        if (current != head) {
          prev = prev->rightAnd;
        }
      }
    }
    current = current->rightAnd;  // else go to next AND list element.
  }
}

bool QueryOptimizer::IsALiteral(Operand *op) { return op->code != NAME; }

bool QueryOptimizer::ContainsLiteral(ComparisonOp *compOp) {
  return IsALiteral(compOp->left) || IsALiteral(compOp->right);
}

void QueryOptimizer::PrintTree(BaseNode *base) {
  if (base == NULL) return;
  PrintTree(base->left.value);
  switch (base->nodeType) {
    case BASE_NODE:
      std::cout << "BASE"
                << " ";
      break;
    case JOIN:
      std::cout << "JOIN"
                << " ";
      std::cout << "{Input pipes: (" << base->left.id << "," << base->right.id
                << "), Output pipe: (" << base->parent.id << ")}  ";
      break;
    case RELATION_NODE:
      RelationNode *r = (RelationNode *)base;
      std::cout << r->relName << " ";
      break;
  }
  PrintTree(base->right.value);
}

QueryPlan *QueryOptimizer::GetOptimizedPlan(std::string query) {
  yy_scan_string(query.c_str());
  yyparse();
  std::vector<std::vector<std::string>> joinMatrix;
  SeparateJoinsandSelects(currentStats, joinMatrix);
  bool joinPresent = false;
  for (auto iit = joinMatrix.begin(); iit != joinMatrix.end(); iit++) {
    std::vector<std::string> row = *iit;
    for (auto jit = row.begin(); jit != row.end(); jit++) {
      if ((*jit).size() == 0) {
        std::cout << "NULL"
                  << " ";
      } else {
        if (!joinPresent) joinPresent = true;
        std::cout << *jit << " ";
      }
    }
    std::cout << std::endl;
  }
  std::cout << "Join present:" << joinPresent << std::endl;
  if (joinPresent) {
    std::vector<std::string> relNames;
    struct TableList *table = tables;

    while (table) {
      relNames.push_back(table->tableName);
      table = table->next;
    }
    BaseNode *join = OptimumOrderingOfJoin(*relNameToSchema, currentStats,
                                           relNames, joinMatrix);
  }
  std::cout << std::endl;
  return NULL;
  // BaseNode *optimizedJoinNode = OptimumOrderingOfJoin(
  //     *relNameToSchema, currentStats, relNames, joinMatrix);
}

void QueryOptimizer::GenerateTree(struct JoinNode *joinNode) {
  BaseNode *currentNode =
      new BaseNode;  // a sentinel node that will be the root.

  // Handle DISTINCT
  if (distinctAtts == 1) {
    BaseNode *drNode = new BaseNode;
    if (currentNode) {
      drNode->nodeType = DUPLICATE_REMOVAL;
      currentNode->left = dynamic_cast<DuplicateRemovalNode *>(drNode);
      currentNode = currentNode->left.value;
    }
  }

  // Handle PROJECTS.
  // if (attsToSelect)
  // {
  //   vector<int> keepMe;
  //   NameList *nameList = attsToSelect;

  //   while (nameList)
  //   {
  //     for (auto sch : statsObject->currentState->schemaList)
  //     {
  //       // for (auto att : sch.){}
  //     }
  //   }
  // }

  // Handle SELECTS.
  if (boolean) {
    CNF *cnf = new CNF;
    Record *literal = new Record;
    cnf->GrowFromParseTree(boolean, joinNode->schema, *literal);

    JoinNode *selectNode = new JoinNode;
    selectNode->nodeType = SELECT_FILE;
    selectNode->cnf = cnf;
    selectNode->literal = literal;
    selectNode->schema = joinNode->schema;

    currentNode->left = dynamic_cast<JoinNode *>(selectNode);
    currentNode = currentNode->left.value;
  }

  // Finally join the JOIN subtree
  currentNode->left = dynamic_cast<JoinNode *>(joinNode);
}