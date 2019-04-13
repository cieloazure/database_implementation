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

bool Statistics::IsRelNamesValid(std::vector<std::string> relNames,
                                 std::set<std::set<std::string>> partitions) {
  return false;
}