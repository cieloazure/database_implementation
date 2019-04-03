#include "Statistics.h"
#include <stdio.h>

Statistics::Statistics() {}
Statistics::Statistics(Statistics &copyMe) {
  for(auto it = copyMe.relationStore.begin(); it != copyMe.relationStore.end(); it++){
    std::pair<std::string, struct RelationStats *> relationStoreEntry = *it;

    std::string relationStoreKey = relationStoreEntry.first;
    struct RelationStats * relationStoreValue = relationStoreEntry.second;

    AddRel((char *)relationStoreKey.c_str(), relationStoreValue->numTuples);

    for(auto attIt = relationStoreValue->attributes.begin(); 
        attIt != relationStoreValue->attributes.end(); 
        attIt++){

          std::string attName = *attIt;
          struct AttStoreKey attKey;
          attKey.attName = attName;
          attKey.relName = relationStoreKey;
          struct AttributeStats *attStats = copyMe.attributeStore.at(attKey);

          AddAtt((char *)relationStoreKey.c_str(), (char *)attName.c_str(), attStats->numDistinctValues);
    }
  }
}
Statistics::~Statistics() {}

void Statistics::AddRel(char *relName, int numTuples) {
  std::string relNameStr(relName);
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
  struct AttStoreKey attStoreKey;
  attStoreKey.attName = attName;
  attStoreKey.relName = relNameStr;

  // Check if a relation exist
  // If not, numTuples in relation is estimated to be equal to numDistincts
  try{
    struct RelationStats *relStats = relationStore.at(relNameStr);
    relStats->attributes.insert(attNameStr);

    // if numDistincts == -1, numDistincts is equal to number of tuples in
    // relation
    if (numDistincts < 0) {
      numDistincts = relStats->numTuples;
    }
  }catch(const std::out_of_range &oor){
    AddRel(relName, numDistincts);
    struct RelationStats *relStats = relationStore.at(relNameStr);
    relStats->attributes.insert(attNameStr);
  }

  struct AttributeStats *attStats;
  try {
    attStats = attributeStore.at(attStoreKey);
    attStats->numDistinctValues += numDistincts;
  } catch (const std::out_of_range &oor) {
    attStats = new struct AttributeStats;
    attStats->attName = attNameStr;
    attStats->relName = relNameStr;
    attStats->numDistinctValues = numDistincts;
    std::pair<struct AttStoreKey, struct AttributeStats *> newAttStat(attStoreKey,
                                                               attStats);
    attributeStore.insert(newAttStat);
  }
}

void Statistics::CopyRel(char *oldName, char *newName) {
  std::string oldNameStr(oldName);
  std::string newNameStr(newName);

  try{
    struct RelationStats *relStats = relationStore.at(oldNameStr);
    // Copy number of tuples for new relation
    AddRel(newName, relStats->numTuples);

    // Copy number of distinct values for each attribute
    for(auto it = relStats->attributes.begin(); it != relStats->attributes.end(); it++){
      std::string attNameStr = *it;
      struct AttStoreKey attStoreKey;
      attStoreKey.attName = attNameStr;
      attStoreKey.relName = oldNameStr;

      try{
        struct AttributeStats *attStats = attributeStore.at(attStoreKey);
        AddAtt(newName, (char *)attNameStr.c_str(), attStats->numDistinctValues);
      }catch(const std::out_of_range &oor){
        throw std::runtime_error("Attribute error");
      }
    }
  }catch(const std::out_of_range &oor){
    throw std::runtime_error("Relation error");
  }
}

void Statistics::Read(char *fromWhere) {
  int fileMode = O_RDWR;
  int statisticsFileDes = open(fromWhere, fileMode, S_IRUSR | S_IWUSR);
  if(statisticsFileDes < 0){
    printf("Oh dear, something went wrong with open()! %s\n", strerror(errno));
    std::cerr << "Error opening file for writing statistics" << std::endl;
    throw std::runtime_error("Error opening file for writing statistics");
  }

  lseek(statisticsFileDes, 0, SEEK_SET);
  size_t numberOfRelations = 0;
  read(statisticsFileDes, &numberOfRelations, sizeof(size_t));

  for(int i = 0; i < numberOfRelations; i++){
    struct RelationStats relationStoreValue;

    size_t keyLength = 0;
    read(statisticsFileDes, &keyLength, sizeof(size_t));
    char key[keyLength];
    read(statisticsFileDes, &key, keyLength);
    for(int k = 0; k < keyLength; k++){
      relationStoreValue.relName.push_back(key[k]);
    }

    long numTuples = 0;
    read(statisticsFileDes, &numTuples, sizeof(long));
    relationStoreValue.numTuples = numTuples;

    size_t numberOfAttributes = 0;
    read(statisticsFileDes, &numberOfAttributes, sizeof(size_t));

    for(int j = 0; j < numberOfAttributes; j++){
      struct AttributeStats attStoreVal;

      size_t attLength = 0;
      read(statisticsFileDes, &attLength, sizeof(size_t));
      char attName[attLength];
      read(statisticsFileDes, &attName, attLength);
      for(int k = 0; k < attLength; k++){
        attStoreVal.attName.push_back(attName[k]);
      }

      long numDistinctValues = 0;
      read(statisticsFileDes, &numDistinctValues, sizeof(long));
      attStoreVal.numDistinctValues = numDistinctValues;

      attStoreVal.relName = relationStoreValue.relName;
      relationStoreValue.attributes.insert(attStoreVal.attName);

      struct AttStoreKey attStoreKey;
      attStoreKey.attName = attStoreVal.attName;
      attStoreKey.relName = attStoreVal.relName;

      std::pair<struct AttStoreKey, struct AttributeStats *> attStoreEntry(attStoreKey, &attStoreVal);
      attributeStore.insert(attStoreEntry);
    }

    std::pair<std::string, struct RelationStats *> relationStoreEntry(relationStoreValue.relName, &relationStoreValue);
    relationStore.insert(relationStoreEntry);
  }

  close(statisticsFileDes);
}

void Statistics::Write(char *toWhere) {
  int fileMode = O_TRUNC | O_RDWR | O_CREAT;
  int statisticsFileDes = open(toWhere, fileMode, S_IRUSR | S_IWUSR);
  if(statisticsFileDes < 0){
    std::cerr << "Error opening file for writing statistics" << std::endl;
    throw std::runtime_error("Error opening file for writing statistics");
  }

  // Proposed file structure:
  // relname1 numTuples numatts(2)
  // attName1 numdistincts
  // attName2 numdistincts
  // relname2 numTuples numatts(3)
  // attName1 numdistincts
  // attName2 numdistincts
  // attName3 numdistincts

  lseek(statisticsFileDes, 0, SEEK_SET);
  size_t relationSize = relationStore.size();

  // Write the total number of relations in the relation store
  write(statisticsFileDes, &relationSize, sizeof(size_t));
  for(auto it = relationStore.begin(); it != relationStore.end(); it++){
    std::pair<std::string, struct RelationStats *> relationStoreEntry = *it;

    std::string relationStoreKey = relationStoreEntry.first;
    struct RelationStats * relationStoreValue = relationStoreEntry.second;

    size_t relationStoreKeyLength = relationStoreKey.size();
    // Write the length of the relName in the store
    write(statisticsFileDes, &relationStoreKeyLength, sizeof(size_t));
    // Write the actual string in relName
    write(statisticsFileDes, relationStoreKey.c_str(), relationStoreKeyLength);
    // Write the number of tuples in the store
    write(statisticsFileDes, &relationStoreValue->numTuples, sizeof(long));

    // Write the number of attributes for this relation
    size_t attributesLength = relationStoreValue->attributes.size();
    write(statisticsFileDes, &attributesLength, sizeof(size_t));
    for(auto attributeIt = relationStoreValue->attributes.begin(); 
        attributeIt != relationStoreValue->attributes.end(); 
        attributeIt++){

          std::string attName = *attributeIt;
          struct AttStoreKey attKey;
          attKey.attName = attName;
          attKey.relName = relationStoreKey;
          struct AttributeStats *attStats = attributeStore.at(attKey);

          size_t attNameLen = attName.size();
          // Write the length of attribute name string 
          write(statisticsFileDes, &attNameLen, sizeof(size_t));
          // Write the actual string of the attribute name
          write(statisticsFileDes, attName.c_str(), attNameLen);
          // Write the number of distinct values of the attribute obtained from the attributeStore
          write(statisticsFileDes, &attStats->numDistinctValues, sizeof(long));
    }
  }

  close(statisticsFileDes);
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[],
                       int numToJoin) {}
double Statistics::Estimate(struct AndList *parseTree, char **relNames,
                            int numToJoin) {
  return 0.0;
}
