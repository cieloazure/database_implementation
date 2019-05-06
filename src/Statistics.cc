#include "Statistics.h"

Statistics::Statistics() { currentState = new StatisticsState(); }

Statistics::~Statistics() { delete currentState; }

Statistics::Statistics(Statistics &copyMe) {
  currentState = new StatisticsState(copyMe.currentState);
}

void Statistics::AddRel(char *relName, int numTuples) {
  currentState->AddNewRel(relName, numTuples);
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
  currentState->AddNewAtt(relName, attName, numDistincts);
}

void Statistics::CopyRel(char *oldName, char *newName) {
  currentState->CopyRel(oldName, newName);
}

bool Statistics ::IsALiteral(Operand *op) { return op->code != NAME; }

bool Statistics ::ContainsLiteral(ComparisonOp *compOp) {
  return IsALiteral(compOp->left) || IsALiteral(compOp->right);
}

bool Statistics ::CheckAndList(struct AndList *andList,
                               std::vector<std::string> relNames) {
  if (andList != NULL) {
    struct OrList *orList = andList->left;
    bool orListStatus = CheckOrList(orList, relNames);
    bool rightAndListStatus = true;
    if (andList->rightAnd) {
      rightAndListStatus = CheckAndList(andList->rightAnd, relNames);
    }
    return orListStatus && rightAndListStatus;
  } else {
    return true;
  }
}

bool Statistics ::CheckOrList(struct OrList *orList,
                              std::vector<std::string> relNames) {
  if (orList != NULL) {
    struct ComparisonOp *compOp = orList->left;
    // check compOp
    bool compOpStatus = true;
    if (compOp != NULL) {
      compOpStatus = CheckOperand(compOp->left, relNames) &&
                     CheckOperand(compOp->right, relNames);
    }

    bool rightOrStatus = true;
    if (orList->rightOr) {
      rightOrStatus = CheckOrList(orList->rightOr, relNames);
    }

    return compOpStatus && rightOrStatus;
  } else {
    return true;
  }
}

bool Statistics ::CheckOperand(struct Operand *operand,
                               std::vector<std::string> relNames) {
  if (operand != NULL && !IsALiteral(operand)) {
    AttributeStats *att = currentState->FindAtt(operand->value, relNames);
    if (att == NULL) {
      return false;
    } else {
      return true;
    }
  } else {
    return true;
  }
}

bool Statistics ::CheckAttNameInRel(struct AndList *parseTree,
                                    std::vector<std::string> relNames) {
  return CheckAndList(parseTree, relNames);
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[],
                       int numToJoin) {
  CalculateCost(parseTree, relNames, numToJoin, currentState);
}

double Statistics ::Estimate(struct AndList *parseTree, char *relNames[],
                             int numToJoin) {
  StatisticsState *tempState = new StatisticsState(currentState);
  return CalculateCost(parseTree, relNames, numToJoin, tempState);
}

double Statistics::CalculateCost(AndList *parseTree, char *relNames[],
                                 int numToJoin, StatisticsState *currentState) {
  // Get representatives of the sets
  std::set<std::string> disjointSetSubset;
  int ridx = numToJoin;
  for (char *c = *relNames; c; c = *++relNames) {
    std::string cstr(c);
    DisjointSetNode *setNode = currentState->FindSet(cstr);
    disjointSetSubset.insert(setNode->relName);
    ridx--;
    if (!ridx) {
      break;
    }
  }

  // Copy the set to a vector
  // TODO: Change parameters to set
  std::map<std::string, double> relNameToCostMap;

  // Initialize cost for each relation
  for (auto it = disjointSetSubset.begin(); it != disjointSetSubset.end();
       it++) {
    RelationStats *relStats = currentState->FindRel(*it);
    relNameToCostMap.emplace(*it, relStats->numTuples);
  }

  // ERROR CHECKING
  // if (disjointSetSubset.size() != 2) {
  //   std::cout << "[Statistics]:ERROR: Estimate/Apply: ParseTree invalid! The
  //   "
  //                "joins meant in the parseTree does not make sense. Check if
  //                " "the relations are not already joined. This error usually
  //                " "occurs due to trying to join a relation which exists as
  //                join " "and now is a subset of the joined relation"
  //             << std::endl;
  //   return -1.0;
  // }
  std::vector<std::string> disjointVec(disjointSetSubset.begin(),
                                       disjointSetSubset.end());
  if (!CheckAttNameInRel(parseTree, disjointVec)) {
    // std::cout << "[Statistics]:ERROR: Estimate/Apply ParseTree invalid! "
    //              "Attribute name not "
    //              "present in the given relation"
    //           << std::endl;
    return -1.0;
  }

  // if (!IsRelNamesValid(relNamesVec, partitions)) {
  //   throw std::runtime_error("Relation names invalid!");
  //}

  if (parseTree == NULL) {
    if (disjointSetSubset.size() == 1) {
      return -1.0;
    } else {
      // Calculate cross product of the relations in the relNames array
      // Taking union of the two relations to be subjected to cross product
      std::string rel1;
      std::string rel2;
      int i = 0;
      for (auto it = disjointSetSubset.begin(); it != disjointSetSubset.end();
           it++) {
        if (i == 0) {
          rel1 = *it;
        } else {
          rel2 = *it;
        }
        i++;
      }
      DisjointSetNode *rep = currentState->Union(rel1, rel2);
      RelationStats *repRelStats = currentState->FindRel(rep->relName);
      long result = relNameToCostMap[rel1] * relNameToCostMap[rel2];
      repRelStats->numTuples = result;

      RelationStats *relStats1 = currentState->SearchRelStore(rel1);
      if (repRelStats != relStats1) {
        // Copy all the attributes to representative
        for (auto it = relStats1->attributes.begin();
             it != relStats1->attributes.end(); it++) {
          AttributeStats *attStats =
              currentState->SearchAttStore(relStats1->relName, *it);
          attStats->relName = rep->relName;
          currentState->RemoveAttStore(relStats1->relName, *it);
          currentState->InsertAtt(attStats);
          repRelStats->attributes.insert(attStats->attName);
        }
        // Remove this relation
        currentState->RemoveRel(relStats1->relName, false);
      }

      RelationStats *relStats2 = currentState->SearchRelStore(rel2);
      if (repRelStats != relStats2) {
        // Copy all the attributes to representative
        for (auto it = relStats2->attributes.begin();
             it != relStats2->attributes.end(); it++) {
          AttributeStats *attStats =
              currentState->SearchAttStore(relStats2->relName, *it);
          attStats->relName = rep->relName;
          currentState->RemoveAttStore(relStats2->relName, *it);
          currentState->InsertAtt(attStats);
          repRelStats->attributes.insert(attStats->attName);
        }
        // Remove this relation
        currentState->RemoveRel(relStats2->relName, false);
      }

      return result;
    }
  } else {
    CalculateCostAndList(parseTree, relNameToCostMap, currentState);
    for (auto it = relNameToCostMap.begin(); it != relNameToCostMap.end();
         it++) {
      currentState->UpdateRel((*it).first, (*it).second, false);
    }
    if (relNameToCostMap.size() == 1) {
      return (*relNameToCostMap.begin()).second;
    } else {
      return -1.0;
    }
  }
}

void Statistics ::CalculateCostAndList(
    AndList *andList, std::map<std::string, double> &relNameToCostMap,
    StatisticsState *currentState) {
  if (andList != NULL) {
    CalculateCostOrList(andList->left, relNameToCostMap, currentState);
    if (andList->rightAnd) {
      CalculateCostAndList(andList->rightAnd, relNameToCostMap, currentState);
    }
  }
}

void Statistics ::CalculateCostOrList(
    OrList *orList, std::map<std::string, double> &relNameToCostMap,
    StatisticsState *currentState) {
  std::map<std::string, std::vector<double>> relNameToIndependentCostsMap;
  for (auto it = relNameToCostMap.begin(); it != relNameToCostMap.end(); it++) {
    std::vector<double> emptyVec;
    relNameToIndependentCostsMap.emplace((*it).first, emptyVec);
  }

  CalculateCostOrListHelper(orList, relNameToCostMap,
                            relNameToIndependentCostsMap, currentState);

  for (auto it = relNameToIndependentCostsMap.begin();
       it != relNameToIndependentCostsMap.end(); it++) {
    std::string relName = (*it).first;
    std::vector<double> relNameIndependentCosts = (*it).second;
    double relCost = ApplySelectionOrFormulaList(relNameIndependentCosts,
                                                 relNameToCostMap[relName]);

    if (relCost >= 0) {
      relNameToCostMap[relName] = relCost;
    }
  }
}

void Statistics::CalculateCostOrListHelper(
    OrList *orList, std::map<std::string, double> &relNameToCostMap,
    std::map<std::string, std::vector<double>> &relNameToIndependentCostsMap,
    StatisticsState *currentState) {
  if (orList != NULL) {
    struct ComparisonOp *compOp = orList->left;
    if (compOp != NULL) {
      if (compOp->code == EQUALS) {
        // Check quotes or unquotes
        if (ContainsLiteral(compOp)) {
          // Selection on equality attributes
          // T(S) = T(R)/V(R,A)
          std::pair<std::string, double> relNameCostPair =
              CalculateCostSelectionEquality(compOp, relNameToCostMap,
                                             currentState);
          relNameToIndependentCostsMap[relNameCostPair.first].push_back(
              relNameCostPair.second);
        } else {
          // Cost for join
          // T(S JOIN R) = T(S)T(R) / max(V(R,Y)V(S,Y))
          // Where Y is the attribute to joined

          // joinRetTuple
          // 0 -> cost after join
          // 1 -> representative relation name of the joined(unioned) set
          // 2 -> <relname1, relname2> : relations which are joined(unioned)
          std::tuple<std::string, double, std::pair<std::string, std::string>>
              joinRetTuple =
                  CalculateCostJoin(compOp, relNameToCostMap, currentState);

          std::string repString = std::get<0>(joinRetTuple);
          double cost = std::get<1>(joinRetTuple);
          std::pair<std::string, std::string> relToRemove =
              std::get<2>(joinRetTuple);

          relNameToCostMap.erase(relToRemove.first);
          relNameToIndependentCostsMap.erase(relToRemove.first);
          relNameToCostMap.erase(relToRemove.second);
          relNameToIndependentCostsMap.erase(relToRemove.second);
          relNameToCostMap[repString] = cost;
          std::vector<double> costVec;
          costVec.push_back(cost);
          relNameToIndependentCostsMap[repString] = costVec;
        }
      } else {
        // Seletion on inequality attributes Like GREATER_THAN or LESS_THAN
        // T(S) = T(R) / 3
        std::pair<std::string, double> relNameCostPair =
            CalculateCostSelectionInequality(compOp, relNameToCostMap,
                                             currentState);

        relNameToIndependentCostsMap[relNameCostPair.first].push_back(
            relNameCostPair.second);
      }

      if (orList->rightOr) {
        CalculateCostOrListHelper(orList->rightOr, relNameToCostMap,
                                  relNameToIndependentCostsMap, currentState);
      }
    }
  }
}

std::pair<std::string, double> Statistics ::CalculateCostSelectionEquality(
    ComparisonOp *compOp, std::map<std::string, double> &relNameToCostMap,
    StatisticsState *currentState) {
  AttributeStats *att;
  std::vector<std::string> relNames = RelNamesKeySet(relNameToCostMap);
  // Find out which one is literal and get the next one from attribute store
  if (IsALiteral(compOp->left)) {
    att = currentState->FindAtt(compOp->right->value, relNames);
  } else {
    att = currentState->FindAtt(compOp->left->value, relNames);
  }
  if (att == NULL) {
    throw std::runtime_error("Invalid relNames[]. Join operation failed.");
  }
  double distinctValues = att->numDistincts;
  double prev_cost = relNameToCostMap[att->relName];
  std::pair<std::string, double> retTuple(att->relName,
                                          prev_cost / distinctValues);
  return retTuple;
}

std::pair<std::string, double> Statistics ::CalculateCostSelectionInequality(
    ComparisonOp *compOp, std::map<std::string, double> &relNamesToCostMap,
    StatisticsState *currentState) {
  AttributeStats *att;
  std::vector<std::string> relNames = RelNamesKeySet(relNamesToCostMap);
  // Find out which one is literal and get the next one from attribute store
  if (IsALiteral(compOp->left)) {
    att = currentState->FindAtt(compOp->right->value, relNames);
  } else {
    att = currentState->FindAtt(compOp->left->value, relNames);
  }
  if (att == NULL) {
    throw std::runtime_error("Invalid relNames[]. Join operation failed.");
  }
  double initial_cost = relNamesToCostMap[att->relName];
  std::pair<std::string, double> retTuple(att->relName, initial_cost / 3.0);
  return retTuple;
}

std::tuple<std::string, double, std::pair<std::string, std::string>>
Statistics ::CalculateCostJoin(ComparisonOp *op,
                               std::map<std::string, double> &relNameToCostMap,
                               StatisticsState *currentState) {
  std::vector<std::string> relNames = RelNamesKeySet(relNameToCostMap);
  AttributeStats *att1 = currentState->FindAtt(op->left->value, relNames);
  AttributeStats *att2 = currentState->FindAtt(op->right->value, relNames);

  if (att1 == NULL || att2 == NULL) {
    throw std::runtime_error("Invalid relNames[]. Join operation failed.");
  }

  double distinctTuplesLeft = att1->numDistincts;
  double distinctTuplesRight = att2->numDistincts;

  double maxOfLeftOrRight = -1.0;
  if (distinctTuplesLeft > distinctTuplesRight) {
    maxOfLeftOrRight = distinctTuplesLeft;
  } else {
    maxOfLeftOrRight = distinctTuplesRight;
  }

  double tuplesLeft = relNameToCostMap[att1->relName];
  double tuplesRight = relNameToCostMap[att2->relName];

  double result = ((tuplesLeft * tuplesRight) / maxOfLeftOrRight);

  std::pair<std::string, std::string> costsToRemove(att1->relName,
                                                    att2->relName);

  /* CHANGING STATE */
  // modify disjointsets in the state
  DisjointSetNode *rep = currentState->Union(att1->relName, att2->relName);

  // modify the relationStore
  RelationStats *repRelStats = currentState->FindRel(rep->relName);
  repRelStats->numTuples = result;

  // modify the attributeStore
  // modify the attributes in the state
  // The attributes of the joined relation will contain attributes of
  // att1->relName and attributes of att2->relName except att2

  // Change the relation name of attributes
  // Considering that the representative chosen will be the relation belonging
  // to either relStats1 or relStats2
  // TODO: Prove correctness of this situation
  RelationStats *relStats1 = currentState->SearchRelStore(att1->relName);
  if (repRelStats != relStats1) {
    // Copy all the attributes to representative
    for (auto it = relStats1->attributes.begin();
         it != relStats1->attributes.end(); it++) {
      AttributeStats *attStats =
          currentState->SearchAttStore(relStats1->relName, *it);
      attStats->relName = rep->relName;
      currentState->RemoveAttStore(relStats1->relName, *it);
      currentState->InsertAtt(attStats);
      repRelStats->attributes.insert(attStats->attName);
    }
    // Remove this relation
    currentState->RemoveRel(relStats1->relName, false);
  }

  RelationStats *relStats2 = currentState->SearchRelStore(att2->relName);
  if (repRelStats != relStats2) {
    // Copy all the attributes to representative
    for (auto it = relStats2->attributes.begin();
         it != relStats2->attributes.end(); it++) {
      AttributeStats *attStats =
          currentState->SearchAttStore(relStats2->relName, *it);
      attStats->relName = rep->relName;
      currentState->RemoveAttStore(relStats2->relName, *it);
      currentState->InsertAtt(attStats);
      repRelStats->attributes.insert(attStats->attName);
    }
    // Remove this relation
    currentState->RemoveRel(relStats2->relName, false);
  }

  // modify the relations in the state
  // The relation store after join will only contain the representative of the
  // union However the stats of the join will change and reflect the estimate
  // i.e. `result`
  /* CHANGING STATE END */

  /* Return tuple */
  std::tuple<std::string, double, std::pair<std::string, std::string>>
      joinRetVal(rep->relName, result, costsToRemove);
  return joinRetVal;
}

double Statistics ::ApplySelectionOrFormulaList(std::vector<double> orListsCost,
                                                int totalTuples) {
  double result = -1.0;
  auto it = orListsCost.begin();
  if (it == orListsCost.end()) {
    return result;
  }
  result = *it;
  it++;
  if (it == orListsCost.end()) {
    return result;
  }
  double m2 = *it;
  it++;
  result = ApplySelectionOrFormula(result, m2, totalTuples);
  while (it != orListsCost.end()) {
    m2 = *it;
    it++;
    result = ApplySelectionOrFormula(result, m2, totalTuples);
  }
  return result;
}

double Statistics ::ApplySelectionOrFormula(double distinctValuesOr1,
                                            double distinctValuesOr2,
                                            int totalTuples) {
  double op1 = 1 - (distinctValuesOr1 / totalTuples);
  double op2 = 1 - (distinctValuesOr2 / totalTuples);
  return totalTuples * (1 - (op1 * op2));
}

AttributeStats *Statistics::GetRelationNameOfAttribute(
    char *att, std::vector<std::string> relNamesSubset,
    struct TableList *tables) {
  if (currentState->IsQualifiedAtt(att)) {
    // replace alias with original rel name.
    std::pair<std::string, std::string> attSplit =
        currentState->SplitQualifiedAtt(att);
    char *originalRelName;
    struct TableList *curr = tables;
    while (curr) {
      if (curr->aliasAs == attSplit.first) {
        originalRelName = curr->tableName;
        break;
      }
      curr = curr->next;
    }
    if (originalRelName) {
      return currentState->FindAtt(originalRelName, attSplit.second);
    }
  } else {
    return currentState->FindAtt(att, relNamesSubset);
  }
}

void Statistics::PrintAttributeStore() { currentState->PrintAttributeStore(); }
void Statistics::PrintRelationStore() { currentState->PrintRelationStore(); }
void Statistics::PrintDisjointSets() { currentState->PrintDisjointSets(); }

std::vector<std::string> Statistics::RelNamesKeySet(
    std::map<std::string, double> relNamesToCostMap) {
  std::vector<std::string> keySet;
  for (auto it = relNamesToCostMap.begin(); it != relNamesToCostMap.end();
       it++) {
    keySet.push_back((*it).first);
  }
  return keySet;
}

void Statistics::Read(char *fromWhere) { currentState->Read(fromWhere); }

void Statistics::Write(char *toWhere) { currentState->Write(toWhere); }

int Statistics::GetRelSize(std::string rel) {
  struct RelationStats *relStats = currentState->FindRel(rel);
  return relStats->numTuples;
}
