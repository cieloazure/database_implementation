#ifndef DATABASE_H
#define DATABASE_H

#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include "DBFile.h"
#include "ParseTree.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "Statistics.h"

extern "C" {
typedef struct yy_buffer_state *YY_BUFFER_STATE;
int yyparse(void);  // defined in y.tab.c
YY_BUFFER_STATE yy_scan_string(const char *str);
void yy_delete_buffer(YY_BUFFER_STATE buffer);
}

extern struct NewTable *newTable;
extern struct BulkLoad *bulkLoadInfo;
extern char *whichTableToDrop;
extern char *whereToGiveOutput;
extern char *whichTableToUpdateStatsFor;
extern int operationId;

class Database {
 public:
  static int runLength;
  Statistics *currentStats;
  std::unordered_map<std::string, RelationTuple *> relationLookUp;
  WhereOutput op;
  QueryOptimizer *optimizer;

  Database();
  Database(Statistics *stats,
           std::unordered_map<std::string, RelationTuple *> relNameToTuple);
  void Start();
  void End();
  void UpdateStatistics();
  void CreateTable();
  void DropTable();
  void ExecuteQuery();
  void SetOutput();
  void BulkLoad();
  void ExecuteCommand(std::string command);
};

#endif
