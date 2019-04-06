#include "Statistics.h"

Statistics::Statistics() {}

Statistics::~Statistics() {}

Statistics::Statistics(Statistics &copyMe) {
  for (auto it = copyMe.relationStore.begin(); it != copyMe.relationStore.end();
       it++) {
    std::pair<std::string, struct RelationStats *> copyMeRelationStoreEntry =
        *it;

    struct RelationStats *fromRel = copyMeRelationStoreEntry.second;
    struct RelationStats *toRel = new struct RelationStats;
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
      struct AttributeStats *fromAtt =
          copyMe.attributeStore.at(MakeAttStoreKey(*whichAtt, toRel->relName));
      struct AttributeStats *toAtt = new struct AttributeStats;
      CopyAttStats(fromAtt, toAtt, toRel->relName);
      std::pair<struct AttStoreKey, struct AttributeStats *>
          attributeStoreEntry(MakeAttStoreKey(toAtt->attName, toAtt->relName),
                              toAtt);
      attributeStore.insert(attributeStoreEntry);
    }

    std::pair<std::string, struct RelationStats *> newRelStoreEntry(
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
  struct RelationStats *relStats;
  try {
    relStats = relationStore.at(relNameStr);
    relStats->numTuples += numTuples;
  } catch (const std::out_of_range &oor) {
    relStats = new struct RelationStats;
    relStats->numTuples = numTuples;
    relStats->relName = relNameStr;
    std::pair<std::string, struct RelationStats *> newRelStat(relNameStr,
                                                              relStats);
    relationStore.insert(newRelStat);
  }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
  std::string relNameStr(relName);
  std::string attNameStr(attName);

  // Check if a relation exist
  // If not, numTuples in relation is estimated to be equal to numDistincts
  try {
    struct RelationStats *relStats = relationStore.at(relNameStr);
    relStats->attributes.insert(attNameStr);

    // if numDistincts == -1, numDistincts is equal to number of tuples in
    // relation
    if (numDistincts < 0) {
      numDistincts = relStats->numTuples;
    }
  } catch (const std::out_of_range &oor) {
    AddRel(relName, numDistincts);
    struct RelationStats *relStats = relationStore.at(relNameStr);
    relStats->attributes.insert(attNameStr);
  }

  // Create a attribute store key and insert OR
  // Update the attribute stats if the attribute exists
  struct AttStoreKey attStoreKey {
    .attName = attNameStr, .relName = relNameStr
  };
  struct AttributeStats *attStats;
  try {
    attStats = attributeStore.at(attStoreKey);
    attStats->numDistinctValues += numDistincts;
  } catch (const std::out_of_range &oor) {
    attStats = new struct AttributeStats;
    attStats->attName = attNameStr;
    attStats->relName = relNameStr;
    attStats->numDistinctValues = numDistincts;
    std::pair<struct AttStoreKey, struct AttributeStats *> newAttStat(
        attStoreKey, attStats);
    attributeStore.insert(newAttStat);
  }
}

void Statistics::CopyRel(char *oldName, char *newName) {
  std::string oldNameStr(oldName);
  std::string newNameStr(newName);

  try {
    struct RelationStats *fromRel = relationStore.at(oldNameStr);
    struct RelationStats *toRel = new RelationStats;
    CopyRelStats(fromRel, toRel, newNameStr);
    std::pair<std::string, struct RelationStats *> relationStoreEntry(
        toRel->relName, toRel);
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
    struct AttributeStats *fromAtt =
        attributeStore.at(MakeAttStoreKey(*whichAtt, fromRel->relName));
    struct AttributeStats *toAtt = new struct AttributeStats;
    CopyAttStats(fromAtt, toAtt, toRelName);
    std::pair<struct AttStoreKey, struct AttributeStats *> attributeStoreEntry(
        MakeAttStoreKey(toAtt->attName, toAtt->relName), toAtt);
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
    std::pair<std::string, struct RelationStats *> relationStoreEntry = *it;

    struct RelationStats *relationStatsFromStore = relationStoreEntry.second;
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
    struct AttributeStats *attStats =
        attributeStore.at(MakeAttStoreKey(attName, relStats->relName));

    // Write the attribute stats to the file
    WriteAttributeStatsToFile(attStats, statisticsFileDes);
  }
}

void Statistics ::WriteAttributeStatsToFile(
    struct Statistics ::AttributeStats *attStats, int statisticsFileDes) {
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
  struct RelationStats *relStats = new struct RelationStats;
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
  std::pair<std::string, struct RelationStats *> relationStoreEntry(
      relStats->relName, relStats);
  relationStore.insert(relationStoreEntry);
}

void Statistics ::ReadAttributeStatsFromFile(
    int statisticsFileDes, struct RelationStats &whichRelStats) {
  struct AttributeStats *attStats = new struct AttributeStats;
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
  std::pair<struct AttStoreKey, struct AttributeStats *> attStoreEntry(
      MakeAttStoreKey(attStats->attName, whichRelStats.relName), attStats);
  attributeStore.insert(attStoreEntry);
}

struct Statistics::AttStoreKey Statistics ::MakeAttStoreKey(
    std::string attName, std::string relName) {
  struct AttStoreKey *attStoreKey = new struct AttStoreKey;
  attStoreKey->attName = attName;
  attStoreKey->relName = relName;
  return *attStoreKey;
}

bool Statistics ::CheckAndList(struct AndList *andList, char **relNames) {
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

bool Statistics ::CheckOrList(struct OrList *orList, char **relNames) {
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

bool Statistics ::CheckOperand(struct Operand *operand, char **relNames) {
  if (operand != NULL) {
    std::string attName(operand->value);
    std::cout << "Attribute name" << std::endl;
    std::cout << operand->value << std::endl;
    for (char *relName = *relNames; relName; relName = *++relNames) {
      std::cout << "Relation name" << std::endl;
      std::cout << relName << std::endl;
      std::string relNameStr(relName);
      try {
        attributeStore.at(MakeAttStoreKey(attName, relNameStr));
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
                                    char **relNames) {
  return CheckAndList(parseTree, relNames);
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[],
                       int numToJoin) {}

double Statistics::Estimate(struct AndList *parseTree, char **relNames,
                            int numToJoin) {
  if (!CheckAttNameInRel(parseTree, relNames)) {
    throw std::runtime_error("Attribute name is not in relNames");
  }
  return 0.0;
}

void Statistics ::PrintRelationStore() {
  std::cout << "Relations currently in the store:" << std::endl;
  for (auto relationEntry : relationStore) {
    std::cout << relationEntry.first << std::endl;
    struct RelationStats *val = relationEntry.second;
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
    struct AttributeStats *val = attributeEntry.second;
    std::cout << "Num distincts of attribute " << attStoreKey.attName << " in "
              << attStoreKey.relName << ":" << val->numDistinctValues
              << std::endl;
  }
}
