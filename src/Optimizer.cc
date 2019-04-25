#include "Optimizer.h"

Optimizer::Optimizer() { currentState = new StatisticsState(); }

void Optimizer::ReadParserDatastructures() {
  std::cout << "Here." << std::endl;
}

void Optimizer::Read(char *fromWhere) { currentState->Read(fromWhere); }

void Optimizer::OptimumOrderingOfJoin(
    Statistics *prevStats,
    std::map<std::string, std::string> joinRelTojoinAtt) {
  int length = joinRelTojoinAtt.size();
  std::vector<std::string> relNames;
  std::vector<std::vector<double>> costMatrix;
  std::vector<std::vector<Statistics *>> stateMatrix;

  // Initialization

  // keyset of map
  for (auto it = joinRelTojoinAtt.begin(); it != joinRelTojoinAtt.end(); it++) {
    relNames.push_back(it->first);
  }

  // init matrices
  for (int i = 0; i < length; i++) {
    std::vector<Statistics *> initialState;
    std::vector<double> initialCosts;
    for (int j = 0; j < length; j++) {
      initialCosts.push_back(-1.0);
      initialState.push_back(NULL);
    }
    stateMatrix.push_back(initialState);
    costMatrix.push_back(initialCosts);
  }

  for (int i = 0; i < length; i++) {
    costMatrix[i][i] = 0;
    costMatrix[i][i + 1] = 0;
  }

  // DP begins
  int diff = 2;
  while (diff < length) {
    int start = 0;
    int end = start + diff;
    std::cout << "ITER " << diff << std::endl;
    while (end < length) {
      std::cout << "Calculate min cost of range of relations:" << start
                << " to " << end << std::endl;
      double minCost = -1.0;
      if (diff > 2) {
        // Use costMatrix to estimate cost
        // CalculateCost1(costMatrix, stateMatrix);
      } else {
        // Calculate permutations of three relations
        // and use prevStats to estimate cost
        // CalculateCost2(costMatrix, stateMatrix);
      }
      start++;
      end++;
    }
    diff++;
  }

  // Print
  for (int i = 0; i < length; i++) {
    for (int j = 0; j < length; j++) {
      std::cout << costMatrix[i][j] << " ";
    }
    std::cout << std::endl;
  }
}

void Optimizer::CalculateCost1(
    std::vector<std::vector<double>> &costMatrix,
    std::vector<std::vector<Statistics *>> &stateMatrix,
    std::vector<std::string> &relNames, int start, int end,
    std::map<std::string, std::string> joinRelTojoinAtt) {
  // find min
  bool firstMatch = costMatrix[start][end - 1] < costMatrix[start + 1][end];

  // Get state of min cost
  Statistics *toJoinState;
  if (firstMatch) {
    toJoinState = stateMatrix[start][end - 1];
    // Create parse Tree for join condition
    ConstructJoinCNF(joinRelTojoinAtt, relNames[start], relNames[end]);
  } else {
    toJoinState = stateMatrix[start + 1][end];
    // Create parse Tree for join condition
    ConstructJoinCNF(joinRelTojoinAtt, relNames[start + 1], relNames[start]);
  }
  Statistics *toJoinStateCopy = new Statistics(*toJoinState);

  // Create relNames array for joining relations
  int joinNum = end - start + 1;
  const char *relNamesSubset[joinNum];
  for (int i = start; i < end; i++) {
    relNamesSubset[i] = relNames[i].c_str();
  }

  // calculate new cost
  double cost =
      toJoinStateCopy->Estimate(final, (char **)relNamesSubset, joinNum);
  toJoinStateCopy->Apply(final, (char **)relNamesSubset, joinNum);

  // update the matrices
  costMatrix[start][end] = cost;
  stateMatrix[start][end] = toJoinStateCopy;
}

void Optimizer::CalculateCost2(
    Statistics *prevStats, std::vector<std::vector<double>> &costMatrix,
    std::vector<std::vector<Statistics *>> &stateMatrix,
    std::vector<std::string> &relNames, int start, int end,
    std::map<std::string, std::string> joinRelTojoinAtt) {
  // Create relNames array for joining relations
  int joinNum = end - start + 1;
  int min = -1;
  int minIdx = -1;
  std::vector<Statistics *> perm;
  const char *relNamesSubset[joinNum + 1];

  // Perm 1
  Statistics *copy1 = new Statistics(*prevStats);
  relNamesSubset[0] = relNames[start].c_str();
  relNamesSubset[1] = relNames[start + 1].c_str();
  relNamesSubset[2] = relNames[end].c_str();
  ConstructJoinCNF(joinRelTojoinAtt, relNamesSubset[0], relNamesSubset[1]);
  copy1->Apply(final, (char **)relNamesSubset, 2);
  ConstructJoinCNF(joinRelTojoinAtt, relNamesSubset[0], relNamesSubset[2]);
  double cost1 = copy1->Estimate(final, (char **)relNamesSubset, 3);
  copy1->Apply(final, (char **)relNamesSubset, 3);
  perm.push_back(copy1);
  if (cost1 < min) {
    min = cost1;
    minIdx = 0;
  }

  // Perm 2
  Statistics *copy2 = new Statistics(*prevStats);
  relNamesSubset[0] = relNames[start + 1].c_str();
  relNamesSubset[1] = relNames[end].c_str();
  relNamesSubset[2] = relNames[start].c_str();
  ConstructJoinCNF(joinRelTojoinAtt, relNamesSubset[0], relNamesSubset[1]);
  copy2->Apply(final, (char **)relNamesSubset, 2);
  ConstructJoinCNF(joinRelTojoinAtt, relNamesSubset[0], relNamesSubset[2]);
  double cost2 = copy2->Estimate(final, (char **)relNamesSubset, 3);
  copy2->Apply(final, (char **)relNamesSubset, 3);
  perm.push_back(copy2);
  if (cost2 < min) {
    min = cost2;
    minIdx = 1;
  }

  // Perm 3
  Statistics *copy3 = new Statistics(*prevStats);
  relNamesSubset[0] = relNames[start].c_str();
  relNamesSubset[1] = relNames[end].c_str();
  relNamesSubset[2] = relNames[start + 1].c_str();
  ConstructJoinCNF(joinRelTojoinAtt, relNamesSubset[0], relNamesSubset[1]);
  copy3->Apply(final, (char **)relNamesSubset, 2);
  ConstructJoinCNF(joinRelTojoinAtt, relNamesSubset[0], relNamesSubset[2]);
  double cost3 = copy3->Estimate(final, (char **)relNamesSubset, 3);
  copy3->Apply(final, (char **)relNamesSubset, 3);
  perm.push_back(copy3);
  if (cost3 < min) {
    min = cost3;
    minIdx = 2;
  }
}

void Optimizer::ConstructJoinCNF(
    std::map<std::string, std::string> relNameToJoinAttribute, std::string left,
    std::string right) {
  std::string cnfString;
  cnfString.append("(");
  cnfString.append(relNameToJoinAttribute[left]);
  cnfString.append(" = ");
  cnfString.append(relNameToJoinAttribute[right]);
  cnfString.append(")");
  std::cout << "Joining...." << cnfString << std::endl;
  yy_scan_string(cnfString.c_str());
  yyparse();
}