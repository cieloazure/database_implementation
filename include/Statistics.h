#ifndef STATISTICS_H
#define STATISTICS_H

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include "ParseTree.h"

namespace dbi {}
class Statistics {
 public:
  Statistics();
  Statistics(Statistics &copyMe);  // Performs deep copy
  ~Statistics();

  void AddRel(char *relName, int numTuples);
  void AddAtt(char *relName, char *attName, int numDistincts);
  void CopyRel(char *oldName, char *newName);

  void Read(char *fromWhere);
  void Write(char *toWhere);

  void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
  double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

  void PrintRelationStore();
  void PrintAttributeStore();

 private:
  struct RelationStats {
    std::string relName;
    long numTuples;
    std::set<std::string> attributes;
  };

  struct AttributeStats {
    std::string attName;
    std::string relName;
    long numDistinctValues;
  };

  struct AttStoreKey {
    std::string attName;
    std::string relName;
  };

  struct AttStoreKeyHash {
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
  };

  struct AttStoreKeyEqual {
    bool operator()(const AttStoreKey &lhs, const AttStoreKey &rhs) const {
      return lhs.attName == rhs.attName && lhs.relName == rhs.relName;
    }
  };

  std::unordered_map<std::string, struct RelationStats *> relationStore;
  std::unordered_map<struct AttStoreKey, struct AttributeStats *,
                     struct AttStoreKeyHash, struct AttStoreKeyEqual>
      attributeStore;

  // std::set<std::set<std::string>> partitions;
  std::set<std::set<std::string>> partitions;

  void WriteRelationStatsToFile(struct RelationStats *relStats,
                                int statisticsFileDes);
  void WriteAttributeStatsToFile(struct AttributeStats *attStats,
                                 int statisticsFileDes);

  void ReadRelationStatsFromFile(int statisticsFileDes);
  void ReadAttributeStatsFromFile(int statisticsFileDes,
                                  struct RelationStats& whichRelStats);

  struct AttStoreKey MakeAttStoreKey(std::string attName, std::string relName);

  void CopyRelStats(struct RelationStats *fromRel, struct RelationStats *toRel,
                    std::string toRelName);
  void CopyAttStats(struct AttributeStats *fromAtt,
                    struct AttributeStats *toAtt, std::string toRelName);

  bool CheckAttNameInRel(struct AndList *parseTree, char **relNames);

  bool CheckAndList(struct AndList *andList, char **relNames);
  bool CheckOrList(struct OrList *orList, char **relNames);
  bool CheckOperand(struct Operand *operand, char **relNames);
};

#endif
