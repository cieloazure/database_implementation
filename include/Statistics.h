#ifndef STATISTICS_H
#define STATISTICS_H

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
  double Estimate(struct AndList *parseTree, char *relNames[], int numToJoin);

  void PrintRelationStore();
  void PrintAttributeStore();

 private:
  struct RelationStats {
    std::string relName;
    long numTuples;
    std::set<std::string> attributes;
    int disjointSetIndex;
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
    std::size_t operator()(const Statistics::AttStoreKey &k) const {
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
    bool operator()(const Statistics::AttStoreKey &lhs,
                    const Statistics::AttStoreKey &rhs) const {
      return lhs.attName == rhs.attName && lhs.relName == rhs.relName;
    }
  };

  std::unordered_map<std::string, struct Statistics::RelationStats *>
      relationStore;
  std::unordered_map<
      struct Statistics::AttStoreKey, struct Statistics::AttributeStats *,
      struct Statistics::AttStoreKeyHash, struct Statistics::AttStoreKeyEqual>
      attributeStore;

  std::set<std::set<std::string>> partitions;

  void WriteRelationStatsToFile(struct Statistics::RelationStats *relStats,
                                int statisticsFileDes);
  void WriteAttributeStatsToFile(struct Statistics::AttributeStats *attStats,
                                 int statisticsFileDes);

  void ReadRelationStatsFromFile(int statisticsFileDes);
  void ReadAttributeStatsFromFile(
      int statisticsFileDes, struct Statistics::RelationStats &whichRelStats);

  struct Statistics::AttStoreKey MakeAttStoreKey(std::string attName,
                                                 std::string relName);

  void CopyRelStats(struct Statistics::RelationStats *fromRel,
                    struct Statistics::RelationStats *toRel,
                    std::string toRelName);
  void CopyAttStats(struct Statistics::AttributeStats *fromAtt,
                    struct Statistics::AttributeStats *toAtt,
                    std::string toRelName);

  bool CheckAttNameInRel(struct AndList *parseTree,
                         std::vector<std::string> relNames);
  bool CheckAndList(struct AndList *andList, std::vector<std::string> relNames);
  bool CheckOrList(struct OrList *orList, std::vector<std::string> relNames);
  bool CheckOperand(struct Operand *operand, std::vector<std::string> relNames);
  bool IsRelNamesValid(std::vector<std::string> relNames,
                       std::set<std::set<std::string>> partitions);

 public:
  struct DisjointSetNode {
    std::string relName;
    int rank;
    DisjointSetNode *parent;

    DisjointSetNode(std::string rel) : relName(rel) {}
  };

  struct DisjointSetNodeComp {
    bool operator()(const struct Statistics::DisjointSetNode *lhs,
                    const struct Statistics::DisjointSetNode *rhs) const {
      return lhs->relName < rhs->relName;
    }
  };

  std::vector<struct Statistics::DisjointSetNode *> disjointSets;
  void MakeSet(struct Statistics::RelationStats *relStats);
  void Link(Statistics::DisjointSetNode *x, Statistics::DisjointSetNode *y);
  void Union(std::string relName1, std::string relName2);
  struct Statistics::DisjointSetNode *FindSet(Statistics::DisjointSetNode *x);
  void PrintDisjointSets();
  void GetSets();
};

#endif
