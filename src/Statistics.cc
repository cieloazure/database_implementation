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
  for (char *c = *relNames; c; c = *++relNames) {
    std::string cstr(c);
    DisjointSetNode *setNode = currentState->FindSet(cstr);
    disjointSetSubset.insert(setNode->relName);
  }

  // Copy the set to a vector
  // TODO: Change parameters to set
  std::vector<std::string> relNamesVec;
  std::copy(disjointSetSubset.begin(), disjointSetSubset.end(),
            std::back_inserter(relNamesVec));

  // Initialize cost for each relation
  std::vector<double> costs;
  for (int i = 0; i < relNamesVec.size(); i++) {
    RelationStats *relStats = currentState->FindRel(relNamesVec.at(i));
    costs.push_back(relStats->numTuples);
  }

  if (!CheckAttNameInRel(parseTree, relNamesVec)) {
    throw std::runtime_error("Attribute name is not in relNames");
  }

  // if (!IsRelNamesValid(relNamesVec, partitions)) {
  //   throw std::runtime_error("Relation names are not valid");
  //}
  CalculateCostAndList(parseTree, costs, relNamesVec, currentState);
  int idx = 0;
  for (std::string relName : relNamesVec) {
    currentState->UpdateRel(relName, costs.at(idx), false);
    idx++;
  }
  if (costs.size() == 1) {
    return costs.at(0);
  } else {
    return -1.0;
  }
}

void Statistics ::CalculateCostAndList(AndList *andList,
                                       std::vector<double> &costs,
                                       std::vector<std::string> &relNames,
                                       StatisticsState *currentState) {
  if (andList != NULL) {
    CalculateCostOrList(andList->left, costs, relNames, currentState);
    if (andList->rightAnd) {
      CalculateCostAndList(andList->rightAnd, costs, relNames, currentState);
    }
  }
}

void Statistics ::CalculateCostOrList(OrList *orList,
                                      std::vector<double> &costs,
                                      std::vector<std::string> &relNames,
                                      StatisticsState *currentState) {
  std::vector<std::vector<double>> independentCosts;
  for (int i = 0; i < relNames.size(); i++) {
    std::vector<double> relationCosts;
    independentCosts.push_back(relationCosts);
  }
  CalculateCostOrListHelper(orList, costs, independentCosts, relNames,
                            currentState);
  int i = 0;
  for (auto it = independentCosts.begin(); it != independentCosts.end(); it++) {
    double relCost = -1.0;
    if ((*it).size() > 1) {
      relCost = ApplySelectionOrFormulaList(*it, costs.at(i));
    } else {
      relCost = (*it).at(0);
    }
    costs[i] = relCost;
    i++;
  }
}

void Statistics::CalculateCostOrListHelper(
    OrList *orList, std::vector<double> &costs,
    std::vector<std::vector<double>> &independentCosts,
    std::vector<std::string> &relNames, StatisticsState *currentState) {
  if (orList != NULL) {
    struct ComparisonOp *compOp = orList->left;
    if (compOp != NULL) {
      if (compOp->code == EQUALS) {
        // Check quotes or unquotes
        if (ContainsLiteral(compOp)) {
          // Selection on equality attributes
          // T(S) = T(R)/V(R,A)
          std::pair<double, int> costIndexRetPair =
              CalculateCostSelectionEquality(compOp, relNames, costs,
                                             currentState);
          independentCosts.at(costIndexRetPair.second)
              .push_back(costIndexRetPair.first);
        } else {
          // Cost for join
          // T(S JOIN R) = T(S)T(R) / max(V(R,Y)V(S,Y))
          // Where Y is the attribute to joined
          std::tuple<double, std::string, std::pair<int, int>> joinRetTuple =
              CalculateCostJoin(compOp, relNames, costs, currentState);

          std::pair<int, int> toRemove = std::get<2>(joinRetTuple);

          costs.erase(costs.begin() + toRemove.first);
          relNames.erase(relNames.begin() + toRemove.first);
          independentCosts.erase(independentCosts.begin() + toRemove.first);

          costs.erase(costs.begin() + toRemove.second);
          relNames.erase(relNames.begin() + toRemove.second);
          independentCosts.erase(independentCosts.begin() + toRemove.second);

          relNames.push_back(std::get<1>(joinRetTuple));
          costs.push_back(std::get<0>(joinRetTuple));
          std::vector<double> costRel;
          independentCosts.push_back(costRel);

          std::vector<std::vector<double>>::iterator independentCostsIt;
          std::vector<double>::iterator costIt;

          for (independentCostsIt = independentCosts.begin(),
              costIt = costs.begin();
               independentCostsIt != independentCosts.end();
               independentCostsIt++, costIt++) {
            (*independentCostsIt).clear();
            (*independentCostsIt).push_back(*costIt);
          }
        }
      } else {
        // Seletion on inequality attributes Like GREATER_THAN or LESS_THAN
        // T(S) = T(R) / 3
        std::pair<double, int> costIndexRetPair =
            CalculateCostSelectionInequality(compOp, relNames, costs,
                                             currentState);
        independentCosts.at(costIndexRetPair.second)
            .push_back(costIndexRetPair.first);
      }

      if (orList->rightOr) {
        CalculateCostOrListHelper(orList->rightOr, costs, independentCosts,
                                  relNames, currentState);
      }
    }
  }
}

std::pair<double, int> Statistics ::CalculateCostSelectionEquality(
    ComparisonOp *compOp, std::vector<std::string> relNames,
    std::vector<double> costs, StatisticsState *currentState) {
  AttributeStats *att;
  // Find out which one is literal and get the next one from attribute store
  if (IsALiteral(compOp->left)) {
    att = currentState->FindAtt(compOp->right->value, relNames);
  } else {
    att = currentState->FindAtt(compOp->left->value, relNames);
  }
  double distinctValues = att->numDistincts;
  auto idxit = std::find(relNames.begin(), relNames.end(), att->relName);
  int idx = idxit - relNames.begin();
  double prev_cost = costs.at(idx);
  std::pair<double, int> retTuple(prev_cost / distinctValues, idx);
  return retTuple;
}

std::pair<double, int> Statistics ::CalculateCostSelectionInequality(
    ComparisonOp *compOp, std::vector<std::string> relNames,
    std::vector<double> costs, StatisticsState *currentState) {
  AttributeStats *att;
  // Find out which one is literal and get the next one from attribute store
  if (IsALiteral(compOp->left)) {
    att = currentState->FindAtt(compOp->right->value, relNames);
  } else {
    att = currentState->FindAtt(compOp->left->value, relNames);
  }
  auto idxit = std::find(relNames.begin(), relNames.end(), att->relName);
  int idx = idxit - relNames.begin();
  double initial_cost = costs.at(idx);
  std::pair<double, int> retTuple(initial_cost / 3.0, idx);
  return retTuple;
}

std::tuple<double, std::string, std::pair<int, int>>
Statistics ::CalculateCostJoin(ComparisonOp *op,
                               std::vector<std::string> &relNames,
                               std::vector<double> &costs,
                               StatisticsState *currentState) {
  AttributeStats *att1 = currentState->FindAtt(op->left->value, relNames);
  AttributeStats *att2 = currentState->FindAtt(op->right->value, relNames);

  double distinctTuplesLeft = att1->numDistincts;
  double distinctTuplesRight = att2->numDistincts;

  double maxOfLeftOrRight = -1.0;
  if (distinctTuplesLeft > distinctTuplesRight) {
    maxOfLeftOrRight = distinctTuplesLeft;
  } else {
    maxOfLeftOrRight = distinctTuplesRight;
  }

  auto idxit1 = std::find(relNames.begin(), relNames.end(), att1->relName);
  int idx1 = idxit1 - relNames.begin();
  double tuplesLeft = costs.at(idx1);

  auto idxit2 = std::find(relNames.begin(), relNames.end(), att2->relName);
  int idx2 = idxit2 - relNames.begin();
  double tuplesRight = costs.at(idx2);

  double result = ((tuplesLeft * tuplesRight) / maxOfLeftOrRight);

  std::pair<int, int> costsToRemove(idx1, idx2);

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
  RelationStats *relStats1 = currentState->FindRel(att1->relName);
  if (repRelStats != relStats1) {
    // Copy all the attributes to representative
    for (auto it = relStats1->attributes.begin();
         it != relStats1->attributes.end(); it++) {
      AttributeStats *attStats = currentState->FindAtt(relStats1->relName, *it);
      attStats->relName = rep->relName;
      currentState->RemoveAtt(relStats1->relName, *it);
      currentState->InsertAtt(attStats);
      repRelStats->attributes.insert(attStats->attName);
    }
    // Remove this relation
    currentState->RemoveRel(relStats1->relName, false);
  }

  RelationStats *relStats2 = currentState->FindRel(att2->relName);
  if (repRelStats != relStats2) {
    // Copy all the attributes to representative
    for (auto it = relStats2->attributes.begin();
         it != relStats2->attributes.end(); it++) {
      AttributeStats *attStats = currentState->FindAtt(relStats2->relName, *it);
      attStats->relName = rep->relName;
      repRelStats->attributes.insert(attStats->attName);
    }
    // Remove this relation
    currentState->RemoveRel(relStats2->relName, false);
  }

  // modify the relations in the state
  // The relation store after join will only contain the representative of the
  // union However the stats of the join will change and reflect the estimate
  // i.e. `result`
  /* CHAGING STATE END */

  /* Return tuple */
  std::tuple<double, std::string, std::pair<int, int>> joinRetVal(
      result, rep->relName, costsToRemove);
  return joinRetVal;
}

double Statistics ::ApplySelectionOrFormulaList(std::vector<double> orListsCost,
                                                int totalTuples) {
  double result = 0.0;
  auto it = orListsCost.begin();
  double m1 = *it;
  it++;
  double m2 = *it;
  it++;
  result = ApplySelectionOrFormula(m1, m2, totalTuples);
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

void Statistics::PrintAttributeStore() { currentState->PrintAttributeStore(); }
void Statistics::PrintRelationStore() { currentState->PrintRelationStore(); }
void Statistics::PrintDisjointSets() { currentState->PrintDisjointSets(); }