#ifndef STATISTICS_STATE_H
#define STATISTICS_STATE_H

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include "ParseTree.h"

/* Data structures begin */
/* Hash table definition begins */
typedef struct RelationStats {
  std::string relName;
  long numTuples;
  std::set<std::string> attributes;
  int disjointSetIndex;
} RelationStats;

typedef struct AttributeStats {
  std::string attName;
  std::string relName;
  long numDistincts;
} AttributeStats;

typedef struct AttStoreKey {
  std::string attName;
  std::string relName;
} AttStoreKey;

typedef struct AttStoreKeyHash {
  std::size_t operator()(const AttStoreKey &k) const {
    std::size_t seed = 0;
    auto hash_combine = [](std::size_t &seed, const std::string &v) -> void {
      std::hash<std::string> hasher;
      seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    };

    hash_combine(seed, k.attName);
    hash_combine(seed, k.relName);

    return seed;
  }
} AttStoreKeyHash;

typedef struct AttStoreKeyEqual {
  bool operator()(const AttStoreKey &lhs, const AttStoreKey &rhs) const {
    return lhs.attName == rhs.attName && lhs.relName == rhs.relName;
  }
} AttStoreKeyEqual;

/* Hash table data structures ends */
/* Disjoint Set data structure functions  begins*/
typedef struct DisjointSetNode {
  std::string relName;
  int rank;
  DisjointSetNode *parent;

  DisjointSetNode() {}
  DisjointSetNode(std::string rel) : relName(rel) {}
} DisjointSetNode;
/* Data strucres ends */

class StatisticsState {
 public:
  /* State of the statistics is stored in these 3 variables*/
  /*
  1. relationStore
  2. attributeStore
  3. disjointSets
   */
  /* RELATION STORE HASHTABLE */
  std::unordered_map<std::string, RelationStats *> relationStore;
  /* ATTRIBUTE STORE HASHTABLE */
  std::unordered_map<AttStoreKey, AttributeStats *, AttStoreKeyHash,
                     AttStoreKeyEqual>
      attributeStore;
  /* FOREST OF DISJOINT SETS */
  std::vector<DisjointSetNode *> disjointSets;

  StatisticsState();
  StatisticsState(StatisticsState *copyState);
  ~StatisticsState();

  /*Disjoint set functions*/
  void MakeSet(RelationStats *relStats);
  void Link(DisjointSetNode *x, DisjointSetNode *y);
  DisjointSetNode *Union(std::string relName1, std::string relName2);
  DisjointSetNode *FindSet(DisjointSetNode *x);
  DisjointSetNode *FindSet(std::string relName);
  DisjointSetNode *FindSet(RelationStats *relStats);
  /* Disjoint set ends */

  // Helpers for store(hashtable) {relationStore, attributeStore}
  AttStoreKey MakeAttStoreKey(std::string relName, std::string attName);
  AttributeStats *FindAtt(char *att, std::vector<std::string> relNamesSubset);
  AttributeStats *FindAtt(std::string relName, std::string attName);
  AttributeStats *SearchAttStore(std::string relName, std::string attName);
  AttributeStats *FindAtt(std::string relName, std::string attName,
                          StatisticsState *copy);
  RelationStats *FindRel(std::string relName);
  RelationStats *SearchRelStore(std::string relName);
  void RemoveRel(std::string relName, bool removeAttributes);
  void RemoveAtt(std::string relName, std::string attName);
  void RemoveAttStore(std::string relName, std::string attName);
  void AddNewRel(char *relName, int numTuples);
  void AddNewAtt(char *relName, char *attName, int numDistincts);
  void InsertRel(std::string relName, int numTuples);
  void InsertRel(RelationStats *relStats);
  void UpdateRel(std::string relName, int numTuples, bool append);
  void InsertAtt(std::string relName, std::string attName, int numDistincts);
  void InsertAtt(AttributeStats *attStats);
  void UpdateAtt(std::string relName, std::string attName, int numDistincts,
                 bool append);
  void CopyRel(char *oldRelName, char *newRelName);
  void CopyRelStats(RelationStats *fromRel, RelationStats *toRel,
                    std::string toRelName);
  void CopyRelStats(RelationStats *fromRel, RelationStats *toRel,
                    std::string toRelName, StatisticsState *copy);
  void CopyAttStats(AttributeStats *fromAtt, AttributeStats *toAtt,
                    std::string toRelName);
  bool IsQualifiedAtt(std::string value);
  std::pair<std::string, std::string> SplitQualifiedAtt(std::string value);
  // Required to debug/test state variables
  void PrintRelationStore();
  void PrintAttributeStore();
  void PrintDisjointSets();
};

#endif

// Helpers for Write
//   void WriteRelationStatsToFile(struct RelationStats *relStats,
//                                 int statisticsFileDes);
//   void WriteAttributeStatsToFile(struct AttributeStats *attStats,
//                                  int statisticsFileDes);

//   // Helpers for Read
//   void ReadRelationStatsFromFile(int statisticsFileDes);
//   void ReadAttributeStatsFromFile(int statisticsFileDes,
//                                   struct RelationStats &whichRelStats);
