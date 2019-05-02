#ifndef DATABASE_H
#define DATABASE_H

#include <stdio.h>
#include <iostream>
#include <unordered_map>
#include "DBFile.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "Statistics.h"

struct RelationTuple {
  std::string relName;
  Schema *schema;
  DBFile *phyicalFile;
};

class Database {
 public:
  Statistics *currentStats;
  std::unordered_map<std::string, RelationTuple *> relationLookUp;
  Database();

  void UpdateStatistics();
  void CreateTable(std::string createTableQuery);
  void DropTable();
  void ExecuteQuery();
  void SetOutput();
  void BulkLoad();
};

#endif
