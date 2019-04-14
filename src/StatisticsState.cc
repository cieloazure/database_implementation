#include "StatisticsState.h"

StatisticsState::StatisticsState() {}
StatisticsState::~StatisticsState() {}
StatisticsState::StatisticsState(StatisticsState *copy) {
  // copy relationStore and attributeStore
  for (auto relIt = copy->relationStore.begin();
       relIt != copy->relationStore.end(); relIt++) {
    std::pair<std::string, RelationStats *> toCopy = *relIt;
    RelationStats *newRelStats = new RelationStats;
    CopyRelStats(toCopy.second, newRelStats, toCopy.second->relName, copy);
    InsertRel(newRelStats);
  }
  // copy disjointSets
  disjointSets.clear();
  std::map<DisjointSetNode *, DisjointSetNode *> newToOldMap;
  std::map<DisjointSetNode *, DisjointSetNode *> oldToNewMap;
  for (auto disjointIt = copy->disjointSets.begin();
       disjointIt != copy->disjointSets.end(); disjointIt++) {
    DisjointSetNode *oldNode = *disjointIt;
    DisjointSetNode *newNode = new DisjointSetNode(oldNode->relName);
    newNode->rank = oldNode->rank;
    oldToNewMap[oldNode] = newNode;
    newToOldMap[newNode] = oldNode;
    RelationStats *relStats = FindRel(oldNode->relName);
    if (relStats != NULL) {
      relStats->disjointSetIndex = disjointSets.size();
    }
    disjointSets.push_back(newNode);
  }

  for (auto disjointIt = disjointSets.begin(); disjointIt != disjointSets.end();
       disjointIt++) {
    DisjointSetNode *newNode = *disjointIt;
    DisjointSetNode *oldNode = newToOldMap[newNode];
    DisjointSetNode *newParent = oldToNewMap[oldNode->parent];
    newNode->parent = newParent;
  }
}

AttStoreKey StatisticsState ::MakeAttStoreKey(std::string relName,
                                              std::string attName) {
  struct AttStoreKey attStoreKey;
  attStoreKey.relName = relName;
  attStoreKey.attName = attName;
  return attStoreKey;
}

void StatisticsState ::AddNewRel(char *relName, int numTuples) {
  // Insert a new relation OR
  // Update the relation stats if the relation exists
  std::string relationName(relName);
  RelationStats *relStats = FindRel(relationName);
  if (relStats == NULL) {
    InsertRel(relationName, numTuples);
  } else {
    UpdateRel(relationName, numTuples, true);
  }
}

void StatisticsState ::AddNewAtt(char *relName, char *attName,
                                 int numDistincts) {
  std::string attributeName(attName);
  std::string relationName(relName);
  AttributeStats *attStats = FindAtt(relationName, attributeName);
  if (attStats == NULL) {
    InsertAtt(relationName, attributeName, numDistincts);
  } else {
    UpdateAtt(relationName, attributeName, numDistincts, true);
  }
}

AttributeStats *StatisticsState ::FindAtt(std::string relName,
                                          std::string attName) {
  try {
    DisjointSetNode *node = FindSet(relName);
    if (node == NULL) {
      throw std::runtime_error("Relation not found in disjoint set");
    }
    return attributeStore.at(MakeAttStoreKey(node->relName, attName));
  } catch (std::out_of_range &e) {
    return NULL;
  }
}

AttributeStats *StatisticsState ::FindAtt(std::string relName,
                                          std::string attName,
                                          StatisticsState *copy) {
  try {
    DisjointSetNode *node = copy->FindSet(relName);
    if (node == NULL) {
      throw std::runtime_error("Relation not found in disjoint set");
    }
    return copy->attributeStore.at(MakeAttStoreKey(node->relName, attName));
  } catch (std::out_of_range &e) {
    return NULL;
  }
}

RelationStats *StatisticsState::FindRel(std::string relName) {
  try {
    DisjointSetNode *node = FindSet(relName);
    return relationStore.at(relName);
  } catch (std::out_of_range &e) {
    return NULL;
  }
}

void StatisticsState ::InsertRel(std::string relName, int numTuples) {
  RelationStats *relStats = new RelationStats;
  relStats->relName = relName;
  relStats->numTuples = numTuples;
  std::pair<std::string, RelationStats *> newTuple(relStats->relName, relStats);
  relationStore.insert(newTuple);
  MakeSet(relStats);
}

void StatisticsState ::UpdateRel(std::string relName, int numTuples,
                                 bool append) {
  RelationStats *relStats = FindRel(relName);
  if (relStats == NULL) {
    throw std::runtime_error("[ArgumentError]: Relation " + relName +
                             " does not exists");
  }
  if (append) {
    relStats->numTuples += numTuples;
  } else {
    relStats->numTuples = numTuples;
  }
}

void StatisticsState ::InsertAtt(std::string relName, std::string attName,
                                 int numDistincts) {
  RelationStats *relStats = FindRel(relName);
  if (relStats == NULL) {
    throw std::runtime_error("[ArgumentError]: Relation " + relName +
                             " does not exists");
  }
  relStats->attributes.insert(attName);
  AttributeStats *attStats = new AttributeStats;
  attStats->relName = relName;
  attStats->attName = attName;
  if (numDistincts < 0) {
    numDistincts = relStats->numTuples;
  }
  attStats->numDistincts = numDistincts;
  std::pair<AttStoreKey, AttributeStats *> newTuple(
      MakeAttStoreKey(relName, attName), attStats);
  attributeStore.insert(newTuple);
}

void StatisticsState ::UpdateAtt(std::string relName, std::string attName,
                                 int numDistincts, bool append) {
  AttributeStats *attStats = FindAtt(relName, attName);
  if (attStats == NULL) {
    throw std::runtime_error("[ArgumentError]: Attribute  " + relName + +"|" +
                             attName + " does not exists");
  }
  if (append) {
    attStats->numDistincts += numDistincts;
  } else {
    attStats->numDistincts = numDistincts;
  }
}

void StatisticsState ::RemoveRel(std::string relName, bool removeAttributes) {
  RelationStats *relStats = FindRel(relName);
  if (relStats == NULL) {
    throw std::runtime_error("[ArgumentError]: Relation " + relName +
                             " does not exists");
  }
  if (removeAttributes) {
    for (auto it = relStats->attributes.begin();
         it != relStats->attributes.end(); it++) {
      RemoveAtt(*it, relStats->relName);
    }
  }
  relationStore.erase(relName);
  // Remove from disjoint set as well
}

void StatisticsState ::RemoveAtt(std::string relName, std::string attName) {
  AttributeStats *attStats = FindAtt(relName, attName);
  if (attStats == NULL) {
    throw std::runtime_error("[ArgumentError]: Attribute  " + relName + +"|" +
                             attName + " does not exists");
  }
  attributeStore.erase(MakeAttStoreKey(relName, attName));
}

void StatisticsState ::CopyRelStats(RelationStats *fromRel,
                                    RelationStats *toRel,
                                    std::string toRelName) {
  // Set the name of the new relation to the new name given
  toRel->relName = toRelName;

  // Set the number of tuple for new relation to the number of tuples for from
  // relation
  toRel->numTuples = fromRel->numTuples;

  // Iterate through all the attributes of the relation and add the attributes
  // in the attribute store for copy relation
  for (auto whichAtt = fromRel->attributes.begin();
       whichAtt != fromRel->attributes.end(); whichAtt++) {
    std::string copyAttStr(*whichAtt);
    toRel->attributes.insert(copyAttStr);

    // Copy the attribute from old relation into new relation and insert it in
    // the attributeStore
    AttributeStats *fromAtt = FindAtt(fromRel->relName, copyAttStr);
    AttributeStats *toAtt = new AttributeStats;
    CopyAttStats(fromAtt, toAtt, toRel->relName);
    InsertAtt(toAtt);
  }
}

void StatisticsState ::CopyRelStats(RelationStats *fromRel,
                                    RelationStats *toRel, std::string toRelName,
                                    StatisticsState *copy) {
  // Set the name of the new relation to the new name given
  toRel->relName = toRelName;

  // Set the number of tuple for new relation to the number of tuples for from
  // relation
  toRel->numTuples = fromRel->numTuples;

  // Iterate through all the attributes of the relation and add the attributes
  // in the attribute store for copy relation
  for (auto whichAtt = fromRel->attributes.begin();
       whichAtt != fromRel->attributes.end(); whichAtt++) {
    std::string copyAttStr(*whichAtt);
    toRel->attributes.insert(copyAttStr);

    // Copy the attribute from old relation into new relation and insert it in
    // the attributeStore
    AttributeStats *fromAtt = FindAtt(fromRel->relName, copyAttStr, copy);
    AttributeStats *toAtt = new AttributeStats;
    CopyAttStats(fromAtt, toAtt, toRel->relName);
    InsertAtt(toAtt);
  }
}

void StatisticsState ::CopyAttStats(AttributeStats *fromAtt,
                                    AttributeStats *toAtt,
                                    std::string toRelName) {
  toAtt->attName = fromAtt->attName;
  toAtt->relName = toRelName;
  toAtt->numDistincts = fromAtt->numDistincts;
}

void StatisticsState ::CopyRel(char *oldRelName, char *newRelName) {
  std::string oldRelationName(oldRelName);
  RelationStats *relStats = FindRel(oldRelationName);
  if (relStats == NULL) {
    throw std::runtime_error("[ArgumentError]: Relation " + oldRelationName +
                             " does not exists");
  }
  RelationStats *relStatsCopy = new RelationStats;
  CopyRelStats(relStats, relStatsCopy, newRelName);
  InsertRel(relStatsCopy);
}

void StatisticsState ::InsertRel(RelationStats *relStats) {
  std::pair<std::string, RelationStats *> newTuple(relStats->relName, relStats);
  relationStore.insert(newTuple);
  MakeSet(relStats);
}

void StatisticsState ::InsertAtt(AttributeStats *attStats) {
  std::pair<AttStoreKey, AttributeStats *> newTuple(
      MakeAttStoreKey(attStats->relName, attStats->attName), attStats);
  attributeStore.insert(newTuple);
}

void StatisticsState ::MakeSet(RelationStats *relStats) {
  relStats->disjointSetIndex = disjointSets.size();
  DisjointSetNode *node = new DisjointSetNode(relStats->relName);
  node->rank = 0;
  node->parent = node;
  disjointSets.push_back(node);
}

void StatisticsState ::Link(DisjointSetNode *x, DisjointSetNode *y) {
  if (x->rank > y->rank) {
    y->parent = x;
  } else {
    x->parent = y;
    if (x->rank == y->rank) {
      y->rank++;
    }
  }
}

DisjointSetNode *StatisticsState ::Union(std::string relName1,
                                         std::string relName2) {
  RelationStats *relStats1 = relationStore.at(relName1);
  RelationStats *relStats2 = relationStore.at(relName2);

  DisjointSetNode *node1 = disjointSets.at(relStats1->disjointSetIndex);
  DisjointSetNode *node2 = disjointSets.at(relStats2->disjointSetIndex);

  DisjointSetNode *nodeRep1 = FindSet(node1);
  DisjointSetNode *nodeRep2 = FindSet(node2);

  Link(nodeRep1, nodeRep2);
  return FindSet(node1);
}

DisjointSetNode *StatisticsState ::FindSet(DisjointSetNode *x) {
  if (x != x->parent) {
    x->parent = FindSet(x->parent);
  }
  return x->parent;
}

DisjointSetNode *StatisticsState ::FindSet(std::string x) {
  for (auto it = disjointSets.begin(); it != disjointSets.end(); it++) {
    if ((*it)->relName == x) {
      return FindSet(*it);
    }
  }
  return NULL;
}

AttributeStats *StatisticsState ::FindAtt(
    char *att, std::vector<std::string> relNamesSubset) {
  std::string attName(att);
  if (IsQualifiedAtt(attName)) {
    std::pair<std::string, std::string> attSplit = SplitQualifiedAtt(attName);
    AttributeStats *attStats = FindAtt(attSplit.first, attSplit.second);
    if (attStats != NULL) {
      return attStats;
    } else {
      return NULL;
    }
  } else {
    for (auto it = relNamesSubset.begin(); it != relNamesSubset.end(); it++) {
      std::string relName = *it;
      AttributeStats *attStats = FindAtt(relName, attName);
      if (attStats != NULL) {
        return attStats;
      }
    }
    return NULL;
  }
}

void StatisticsState ::PrintDisjointSets() {
  std::map<std::string, std::vector<std::string>> sets;
  for (auto it = disjointSets.begin(); it != disjointSets.end(); it++) {
    struct DisjointSetNode *rep = FindSet(*it);
    auto jt = sets.find(rep->relName);
    if (jt == sets.end()) {
      std::vector<std::string> vec;
      vec.push_back((*it)->relName);
      sets.emplace(rep->relName, vec);
    } else {
      sets[rep->relName].push_back((*it)->relName);
    }
  }

  for (auto it = sets.begin(); it != sets.end(); it++) {
    std::vector<std::string> vec = it->second;
    std::cout << "Set ->{";
    for (auto jt = vec.begin(); jt != vec.end(); jt++) {
      std::cout << *jt;
      if (jt + 1 != vec.end()) {
        std::cout << ",";
      }
    }
    std::cout << "}" << std::endl;
  }
}

void StatisticsState ::PrintRelationStore() {
  std::cout << "Relations currently in the store:" << std::endl;
  for (auto relationEntry : relationStore) {
    std::cout << relationEntry.first << std::endl;
    RelationStats *val = relationEntry.second;
    std::cout << "Tuples in the relation " << relationEntry.first << ":"
              << val->numTuples << std::endl;
  }
}

void StatisticsState ::PrintAttributeStore() {
  std::cout << "Attributes currently in the store:" << std::endl;
  for (auto attributeEntry : attributeStore) {
    struct AttStoreKey attStoreKey = attributeEntry.first;
    std::cout << attStoreKey.attName << " in relation " << attStoreKey.relName
              << std::endl;
    AttributeStats *val = attributeEntry.second;
    std::cout << "Num distincts of attribute " << attStoreKey.attName << " in "
              << attStoreKey.relName << ":" << val->numDistincts << std::endl;
  }
}

DisjointSetNode *StatisticsState::FindSet(RelationStats *relStats) {
  DisjointSetNode *node = disjointSets.at(relStats->disjointSetIndex);
  return FindSet(node);
}

bool StatisticsState ::IsQualifiedAtt(std::string value) {
  return value.find('.', 0) != std::string::npos;
}

std::pair<std::string, std::string> StatisticsState::SplitQualifiedAtt(
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

AttributeStats *StatisticsState ::SearchAttStore(std::string relName,
                                                 std::string attName) {
  try {
    return attributeStore.at(MakeAttStoreKey(relName, attName));
  } catch (std::out_of_range &e) {
    return NULL;
  }
}

RelationStats *StatisticsState::SearchRelStore(std::string relName) {
  try {
    return relationStore.at(relName);
  } catch (std::out_of_range &e) {
    return NULL;
  }
}

void StatisticsState::RemoveAttStore(std::string relName, std::string attName) {
  AttributeStats *attStats = SearchAttStore(relName, attName);
  if (attStats == NULL) {
    throw std::runtime_error("[ArgumentError]: Attribute  " + relName + +"|" +
                             attName + " does not exists");
  }
  attributeStore.erase(MakeAttStoreKey(relName, attName));
}