#include "Statistics.h"

Statistics::Statistics() {}

Statistics::~Statistics() {}

Statistics::Statistics(Statistics &copyMe) {
  for (auto it = copyMe.relationStore.begin(); it != copyMe.relationStore.end();
       it++) {
    std::pair<std::string, struct Statistics::RelationStats *>
        copyMeRelationStoreEntry = *it;

    struct Statistics::RelationStats *fromRel = copyMeRelationStoreEntry.second;
    struct Statistics::RelationStats *toRel =
        new struct Statistics::RelationStats;
    // Set the name of the new relation to the new name given
    toRel->relName = fromRel->relName;

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
      struct Statistics::AttributeStats *fromAtt =
          copyMe.attributeStore.at(MakeAttStoreKey(*whichAtt, toRel->relName));
      struct Statistics::AttributeStats *toAtt =
          new struct Statistics::AttributeStats;
      CopyAttStats(fromAtt, toAtt, toRel->relName);
      std::pair<struct Statistics::AttStoreKey,
                struct Statistics::AttributeStats *>
          attributeStoreEntry(MakeAttStoreKey(toAtt->attName, toAtt->relName),
                              toAtt);
      attributeStore.insert(attributeStoreEntry);
    }

    std::pair<std::string, struct Statistics::RelationStats *> newRelStoreEntry(
        toRel->relName, toRel);
    relationStore.insert(newRelStoreEntry);
  }
}

void Statistics::AddRel(char *relName, int numTuples) {
  // Create a string of relName
  std::string relNameStr(relName);

  // Put the relation in the set of subsets
  // initially, each relation is in its very own singleton subset
  std::set<std::string> subset;
  subset.insert(relNameStr);
  partitions.insert(subset);

  // Insert a new relation OR
  // Update the relation stats if the relation exists
  struct Statistics::RelationStats *relStats;
  try {
    relStats = relationStore.at(relNameStr);
    relStats->numTuples += numTuples;
    MakeSet(relStats);
  } catch (const std::out_of_range &oor) {
    relStats = new struct Statistics::RelationStats;
    relStats->numTuples = numTuples;
    relStats->relName = relNameStr;
    MakeSet(relStats);
    std::pair<std::string, struct Statistics::Statistics::RelationStats *>
        newRelStat(relNameStr, relStats);
    relationStore.insert(newRelStat);
  }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
  std::string relNameStr(relName);
  std::string attNameStr(attName);

  // Check if a relation exist
  // If not, numTuples in relation is estimated to be equal to numDistincts
  try {
    struct Statistics::RelationStats *relStats = relationStore.at(relNameStr);
    relStats->attributes.insert(attNameStr);

    // if numDistincts == -1, numDistincts is equal to number of tuples in
    // relation
    if (numDistincts < 0) {
      numDistincts = relStats->numTuples;
    }
  } catch (const std::out_of_range &oor) {
    AddRel(relName, numDistincts);
    struct Statistics::RelationStats *relStats = relationStore.at(relNameStr);
    relStats->attributes.insert(attNameStr);
  }

  // Create a attribute store key and insert OR
  // Update the attribute stats if the attribute exists
  struct AttStoreKey attStoreKey {
    .attName = attNameStr, .relName = relNameStr
  };
  struct Statistics::AttributeStats *attStats;
  try {
    attStats = attributeStore.at(attStoreKey);
    attStats->numDistinctValues += numDistincts;
  } catch (const std::out_of_range &oor) {
    attStats = new struct Statistics::AttributeStats;
    attStats->attName = attNameStr;
    attStats->relName = relNameStr;
    attStats->numDistinctValues = numDistincts;
    std::pair<struct AttStoreKey, struct Statistics::AttributeStats *>
        newAttStat(attStoreKey, attStats);
    attributeStore.insert(newAttStat);
  }
}

void Statistics::CopyRel(char *oldName, char *newName) {
  std::string oldNameStr(oldName);
  std::string newNameStr(newName);

  try {
    struct Statistics::RelationStats *fromRel = relationStore.at(oldNameStr);
    struct Statistics::RelationStats *toRel = new Statistics::RelationStats;
    CopyRelStats(fromRel, toRel, newNameStr);
    std::pair<std::string, struct Statistics::RelationStats *>
        relationStoreEntry(toRel->relName, toRel);
    relationStore.insert(relationStoreEntry);
  } catch (const std::out_of_range &oor) {
    throw std::runtime_error(
        "[Statistics]:[ERROR]: Relation or attribute error!"
        "Hash table in statistics is corrupted and has"
        "missing keys");
  }
}

void Statistics::CopyRelStats(struct Statistics ::RelationStats *fromRel,
                              struct Statistics ::RelationStats *toRel,
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
    struct Statistics::AttributeStats *fromAtt =
        attributeStore.at(MakeAttStoreKey(*whichAtt, fromRel->relName));
    struct Statistics::AttributeStats *toAtt =
        new struct Statistics::AttributeStats;
    CopyAttStats(fromAtt, toAtt, toRelName);
    std::pair<struct AttStoreKey, struct Statistics::AttributeStats *>
        attributeStoreEntry(MakeAttStoreKey(toAtt->attName, toAtt->relName),
                            toAtt);
    attributeStore.insert(attributeStoreEntry);
  }
}

void Statistics ::CopyAttStats(struct Statistics ::AttributeStats *fromAtt,
                               struct Statistics ::AttributeStats *toAtt,
                               std::string toRelName) {
  toAtt->attName = fromAtt->attName;
  toAtt->relName = toRelName;
  toAtt->numDistinctValues = fromAtt->numDistinctValues;
}

void Statistics::Read(char *fromWhere) {
  int fileMode = O_RDWR;
  int statisticsFileDes = open(fromWhere, fileMode, S_IRUSR | S_IWUSR);
  if (statisticsFileDes < 0) {
    printf("Oh dear, something went wrong with open()! %s\n", strerror(errno));
    std::cerr << "Error opening file for writing statistics" << std::endl;
    throw std::runtime_error("Error opening file for writing statistics");
  }

  // Read in the number of relations in the file
  lseek(statisticsFileDes, 0, SEEK_SET);
  size_t numberOfRelations = 0;
  read(statisticsFileDes, &numberOfRelations, sizeof(size_t));

  // Iterate through each building each relation and its associated attributes
  // from file
  for (int whichRel = 0; whichRel < numberOfRelations; whichRel++) {
    ReadRelationStatsFromFile(statisticsFileDes);
  }

  close(statisticsFileDes);
}

void Statistics::Write(char *toWhere) {
  int fileMode = O_TRUNC | O_RDWR | O_CREAT;
  int statisticsFileDes = open(toWhere, fileMode, S_IRUSR | S_IWUSR);
  if (statisticsFileDes < 0) {
    printf("Oh dear, something went wrong with open()! %s\n", strerror(errno));
    std::cerr << "Error opening file for writing statistics" << std::endl;
    throw std::runtime_error("Error opening file for writing statistics");
  }

  // Proposed file structure of statisticsFileDes(statistics.bin):
  // relname1 numTuples numatts(2)
  // attName1 numdistincts
  // attName2 numdistincts

  lseek(statisticsFileDes, 0, SEEK_SET);
  size_t relationSize = relationStore.size();

  // Write the total number of relations in the relation store
  write(statisticsFileDes, &relationSize, sizeof(size_t));

  // Iterate through each relation and write the relation and its associated
  // attributes in the file
  for (auto it = relationStore.begin(); it != relationStore.end(); it++) {
    std::pair<std::string, struct Statistics::RelationStats *>
        relationStoreEntry = *it;

    struct Statistics::RelationStats *relationStatsFromStore =
        relationStoreEntry.second;
    WriteRelationStatsToFile(relationStatsFromStore, statisticsFileDes);
  }

  close(statisticsFileDes);
}

void Statistics ::WriteRelationStatsToFile(
    struct Statistics ::RelationStats *relStats, int statisticsFileDes) {
  // Write the length of the relName in the store
  size_t relNameLength = relStats->relName.size();
  write(statisticsFileDes, &relNameLength, sizeof(size_t));

  // Write the actual string in relName
  write(statisticsFileDes, relStats->relName.c_str(), relNameLength);

  // Write the number of tuples in the store
  write(statisticsFileDes, &relStats->numTuples, sizeof(long));

  // Write the number of attributes for this relation
  size_t attributesLength = relStats->attributes.size();
  write(statisticsFileDes, &attributesLength, sizeof(size_t));

  // Iterate through all attributes and write those attribute stats to file
  for (auto attributeIt = relStats->attributes.begin();
       attributeIt != relStats->attributes.end(); attributeIt++) {
    std::string attName = *attributeIt;
    struct Statistics::AttributeStats *attStats =
        attributeStore.at(MakeAttStoreKey(attName, relStats->relName));

    // Write the attribute stats to the file
    WriteAttributeStatsToFile(attStats, statisticsFileDes);
  }
}

void Statistics ::WriteAttributeStatsToFile(
    struct Statistics ::Statistics::AttributeStats *attStats,
    int statisticsFileDes) {
  // Get length of the attribute name &
  // Write the length of attribute name string
  size_t attNameLen = attStats->attName.size();
  write(statisticsFileDes, &attNameLen, sizeof(size_t));

  // Write the actual string of the attribute name
  write(statisticsFileDes, attStats->attName.c_str(), attNameLen);

  // Write the number of distinct values of the attribute obtained from the
  // attributeStore
  write(statisticsFileDes, &attStats->numDistinctValues, sizeof(long));
}

void Statistics ::ReadRelationStatsFromFile(int statisticsFileDes) {
  struct Statistics::RelationStats *relStats =
      new struct Statistics::RelationStats;
  // Read relName
  size_t keyLength = 0;
  read(statisticsFileDes, &keyLength, sizeof(size_t));
  char key[keyLength];
  read(statisticsFileDes, &key, keyLength);
  for (int k = 0; k < keyLength; k++) {
    relStats->relName.push_back(key[k]);
  }

  // Read numTuples of the relation
  read(statisticsFileDes, &relStats->numTuples, sizeof(long));

  // Read number of attributes belonging to the relation
  size_t numberOfAttributes = 0;
  read(statisticsFileDes, &numberOfAttributes, sizeof(size_t));

  // Iterate and read all the attributes belonging to this relation and store
  // them in attributeStore
  for (int whichAtt = 0; whichAtt < numberOfAttributes; whichAtt++) {
    ReadAttributeStatsFromFile(statisticsFileDes, *relStats);
  }

  // Create an entry in relation store for this stats
  std::pair<std::string, struct Statistics::RelationStats *> relationStoreEntry(
      relStats->relName, relStats);
  relationStore.insert(relationStoreEntry);
}

void Statistics ::ReadAttributeStatsFromFile(
    int statisticsFileDes, struct Statistics::RelationStats &whichRelStats) {
  struct Statistics::AttributeStats *attStats =
      new struct Statistics::AttributeStats;
  // Read length of attribute name & the attribute name
  size_t attLength = 0;
  read(statisticsFileDes, &attLength, sizeof(size_t));
  char attName[attLength];
  read(statisticsFileDes, &attName, attLength);
  for (int k = 0; k < attLength; k++) {
    attStats->attName.push_back(attName[k]);
  }

  // Insert the attribute name in relation stats struct
  whichRelStats.attributes.insert(attStats->attName);

  // Read the number of distinct values for this attribute
  long numDistinctValues = 0;
  read(statisticsFileDes, &numDistinctValues, sizeof(long));
  attStats->numDistinctValues = numDistinctValues;

  // Set the relName of attStats
  attStats->relName = whichRelStats.relName;

  // Create an entry in attribute store for this attribute
  std::pair<struct AttStoreKey, struct Statistics::AttributeStats *>
      attStoreEntry(MakeAttStoreKey(attStats->attName, whichRelStats.relName),
                    attStats);
  attributeStore.insert(attStoreEntry);
}

struct Statistics::AttStoreKey Statistics ::MakeAttStoreKey(
    std::string attName, std::string relName) {
  struct AttStoreKey attStoreKey;
  attStoreKey.attName = attName;
  attStoreKey.relName = relName;
  return attStoreKey;
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
    if (compOp->code == EQUALS) {
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
  } else {
    return true;
  }
}

bool Statistics ::CheckOperand(struct Operand *operand,
                               std::vector<std::string> relNames) {
  if (operand != NULL) {
    std::string attName(operand->value);
    for (auto it = relNames.begin(); it != relNames.end(); it++) {
      std::string relName = *it;
      try {
        attributeStore.at(MakeAttStoreKey(attName, relName));
        return true;
      } catch (const std::out_of_range &oor) {
        continue;
      }
    }
    return false;
  } else {
    return true;
  }
}

bool Statistics ::CheckAttNameInRel(struct AndList *parseTree,
                                    std::vector<std::string> relNames) {
  return CheckAndList(parseTree, relNames);
}

bool Statistics::IsRelNamesValid(std::vector<std::string> relNames,
                                 std::set<std::set<std::string>> partitions) {
  return false;
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[],
                       int numToJoin) {}

double Statistics::Estimate(struct AndList *parseTree, char *relNames[],
                            int numToJoin) {
  std::vector<std::string> relNamesVec;
  for (char *c = *relNames; c; c = *++relNames) {
    relNamesVec.push_back(c);
  }

  if (!CheckAttNameInRel(parseTree, relNamesVec)) {
    throw std::runtime_error("Attribute name is not in relNames");
  }

  // if (!IsRelNamesValid(relNamesVec, partitions)) {
  //   throw std::runtime_error("Relation names are not valid");
  // }

  return 0.0;
}

void Statistics ::PrintRelationStore() {
  std::cout << "Relations currently in the store:" << std::endl;
  for (auto relationEntry : relationStore) {
    std::cout << relationEntry.first << std::endl;
    struct Statistics::RelationStats *val = relationEntry.second;
    std::cout << "Tuples in the relation " << relationEntry.first << ":"
              << val->numTuples << std::endl;
  }
}

void Statistics ::PrintAttributeStore() {
  std::cout << "Attributes currently in the store:" << std::endl;
  for (auto attributeEntry : attributeStore) {
    struct AttStoreKey attStoreKey = attributeEntry.first;
    std::cout << attStoreKey.attName << " in relation " << attStoreKey.relName
              << std::endl;
    struct Statistics::AttributeStats *val = attributeEntry.second;
    std::cout << "Num distincts of attribute " << attStoreKey.attName << " in "
              << attStoreKey.relName << ":" << val->numDistinctValues
              << std::endl;
  }
}

void Statistics ::MakeSet(struct Statistics::RelationStats *relStats) {
  relStats->disjointSetIndex = disjointSets.size();
  struct Statistics::DisjointSetNode *node =
      new struct Statistics::DisjointSetNode(relStats->relName);
  node->rank = 0;
  node->parent = node;
  disjointSets.push_back(node);
}

void Statistics ::Link(Statistics::DisjointSetNode *x,
                       Statistics::DisjointSetNode *y) {
  if (x->rank > y->rank) {
    y->parent = x;
  } else {
    x->parent = y;
    if (x->rank == y->rank) {
      y->rank++;
    }
  }
}

void Statistics ::Union(std::string relName1, std::string relName2) {
  struct RelationStats *relStats1 = relationStore.at(relName1);
  struct RelationStats *relStats2 = relationStore.at(relName2);

  // std::cout << relStats1->disjointSetIndex << std::endl;
  // std::cout << relStats2->disjointSetIndex << std::endl;

  struct DisjointSetNode *node1 = disjointSets.at(relStats1->disjointSetIndex);
  struct DisjointSetNode *node2 = disjointSets.at(relStats2->disjointSetIndex);

  struct DisjointSetNode *nodeRep1 = FindSet(node1);
  struct DisjointSetNode *nodeRep2 = FindSet(node2);

  Link(nodeRep1, nodeRep2);
}

struct Statistics::DisjointSetNode *Statistics ::FindSet(
    Statistics::DisjointSetNode *x) {
  if (x != x->parent) {
    x->parent = FindSet(x->parent);
  }
  return x->parent;
}

void Statistics ::PrintDisjointSets() {
  for (auto it = disjointSets.begin(); it != disjointSets.end(); it++) {
    std::cout << "NODE" << std::endl;
    std::cout << (*it)->relName << std::endl;
    std::cout << (*it)->rank << std::endl;
    std::cout << (*it)->parent->relName << std::endl;
  }
}

void Statistics ::GetSets() {
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
    std::cout << "Set ->{" << std::endl;
    for (auto jt = vec.begin(); jt != vec.end(); jt++) {
      std::cout << (*jt) << std::endl;
    }
    std::cout << "}" << std::endl;
  }
}
