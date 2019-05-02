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
      CNF *cnf = new CNF;
      Record literal;
      cnf->GrowFromParseTree(final, relNode1->schema, relNode2->schema,
                             literal);
      cnf->Print();
      OrderMaker left;
      OrderMaker right;
      bool status = cnf->GetSortOrders(left, right);
      Schema *s =
          new Schema("join_schema", relNode1->schema, relNode2->schema, &right);
      newJoinNode->schema = s;
      newJoinNode->cnf = cnf;
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
      return newMemo.root->left.value;
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
        CNF *cnf = new CNF;
        Record literal;
        cnf->GrowFromParseTree2(final, prevJoinNode->schema, newRelNode->schema,
                                literal);
        cnf->Print();
        OrderMaker left;
        OrderMaker right;
        cnf->GetSortOrders(left, right);
        Schema *s = new Schema("join_schema", prevJoinNode->schema,
                               newRelNode->schema, &right);
        newJoinNode->schema = s;
        newJoinNode->cnf = cnf;
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

  return combinationToMemo[optimalJoin].root->left.value;
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
  if (joinMatrix[idxRight][idxLeft].size() > 0) {
    cnfString.append(joinMatrix[idxRight][idxLeft]);
  } else {
    return false;
  }
  cnfString.append(" = ");
  cnfString.append(right);
  cnfString.append(".");
  cnfString.append(joinMatrix[idxLeft][idxRight]);
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

bool QueryOptimizer::IsQualifiedAtt(std::string value) {
  return value.find('.', 0) != std::string::npos;
}

std::pair<std::string, std::string> QueryOptimizer::SplitQualifiedAtt(
    std::string value) {
  size_t idx = value.find('.', 0);
  std::string rel;
  std::string att;
  if (idx == std::string::npos) {
    att = value;
  } else {
    rel = value.substr(0, idx);
    att = value.substr(idx + 1, value.length());
  }
  std::pair<std::string, std::string> retPair;
  retPair.first = rel;
  retPair.second = att;
  return retPair;
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

    BaseNode *root = GenerateTree(join, *relNameToSchema);
    QueryPlan *q = new QueryPlan(root);
    q->PrintTree(root);
  } else {
    RelationNode *relNode = new RelationNode;
    relNode->relName = tables->tableName;
    std::string relNameStr(relNode->relName);
    relNode->schema = (*relNameToSchema)[relNameStr];
    BaseNode *root = GenerateTree(relNode, *relNameToSchema);
    QueryPlan *q = new QueryPlan(root);
    q->PrintTree(root);
  }
  std::cout << std::endl;
  return NULL;
  // BaseNode *optimizedJoinNode = OptimumOrderingOfJoin(
  //     *relNameToSchema, currentStats, relNames, joinMatrix);
}

BaseNode *QueryOptimizer::GenerateTree(
    struct BaseNode *child,
    std::unordered_map<std::string, Schema *> relNameToSchema) {
  BaseNode *currentNode =
      new BaseNode;  // a sentinel node that will be the root.

  BaseNode *root = currentNode;

  // Handle SUM
  if (finalFunction) {
    SumNode *s = new SumNode;

    Function *f = new Function;
    f->GrowFromParseTree(finalFunction, *child->schema);
    s->f = f;

    Link link(s);
    currentNode->left = link;
    s->parent = link;
    currentNode = currentNode->left.value;
  }

  // Handle GROUP BY
  if (groupingAtts) {
    GroupByNode *groupByNode = new GroupByNode;

    CNF *cnf = new CNF;
    Record literal;
    cnf->GrowFromParseTree(final, child->schema, literal);

    OrderMaker *sortOrder = new OrderMaker;
    OrderMaker *dummy = new OrderMaker;

    cnf->GetSortOrders(*sortOrder, *dummy);

    Function *f = new Function;
    f->GrowFromParseTree(finalFunction, *child->schema);

    groupByNode->nodeType = GROUP_BY;
    groupByNode->o = sortOrder;
    groupByNode->f = f;

    Link link(groupByNode);
    currentNode->left = link;
    groupByNode->parent = link;
    currentNode = currentNode->left.value;
  }

  // Handle DISTINCT
  if (distinctAtts == 1) {
    DuplicateRemovalNode *drNode = new DuplicateRemovalNode();
    if (currentNode) {
      drNode->nodeType = DUPLICATE_REMOVAL;
      Link link(drNode);
      currentNode->left = link;
      drNode->parent = link;
      currentNode = currentNode->left.value;
    }
  }

  // Handle PROJECTS.
  if (attsToSelect) {
    ProjectNode *projectNode = new ProjectNode;
    projectNode->nodeType = PROJECT;
    vector<int> keepMe;
    NameList *nameList = attsToSelect;
    // Populate the keepMe array.
    while (nameList) {
      // If attributes are qualified
      std::string attrName = "";
      if (IsQualifiedAtt(nameList->name)) {
        std::pair<std::string, std::string> attSplit =
            SplitQualifiedAtt(std::string(nameList->name));
        attrName += attSplit.second;
      } else {
        attrName = nameList->name;
      }
      int attIndex = child->schema->Find((char *)attrName.c_str());
      keepMe.push_back(attIndex);
      nameList = nameList->next;
    }
    int *keepMeArr = new int[keepMe.size()];
    for (int i = 0; i < keepMe.size(); ++i) {
      keepMeArr[i] = keepMe[i];
    }

    projectNode->keepMe = keepMeArr;
    projectNode->numAttsInput = child->schema->GetNumAtts();
    projectNode->numAttsOutput = keepMe.size();

    Link link(projectNode);
    currentNode->left = link;
    projectNode->parent = link;
    currentNode = currentNode->left.value;
  }

  // Handle SELECTS.
  if (boolean) {
    CNF *cnf = new CNF;            // = new CNF;
    Record *literal = new Record;  // = new Record;
    cnf->GrowFromParseTree(boolean, child->schema, *literal);

    SelectPipeNode *selectNode = new SelectPipeNode;
    selectNode->nodeType = SELECT_FILE;
    selectNode->cnf = cnf;
    selectNode->literal = literal;
    selectNode->schema = child->schema;

    Link link(selectNode);
    currentNode->left = link;
    selectNode->parent = link;
    currentNode = currentNode->left.value;
  }

  // Finally join the JOIN subtree
  Link link(child);
  currentNode->left = link;
  child->parent = link;

  return root;
}