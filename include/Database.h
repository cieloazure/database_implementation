#ifndef DATABASE_H
#define DATABASE_H

#include <chrono>
#include <ctime>
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
 private:
  Statistics *currentStats;
  std::unordered_map<std::string, RelationTuple *> relationLookUp;
  WhereOutput op;
  QueryOptimizer *optimizer;

 public:
  static int runLength;

  Database();
  Database(QueryOptimizer *optimizer);
  Database(Statistics *stats,
           std::unordered_map<std::string, RelationTuple *> relNameToTuple);
  // Start the Database Application
  void Start();

  // Execute command from the CLI
  void ExecuteCommand(std::string command);
  // Create a new schema in the database
  // Can give a option to be either Heap or Sorted
  void CreateTable();
  // Delete a table from database
  void DropTable();
  // Query the databsae
  void ExecuteQuery();
  // Set where the output is to be displayed
  void SetOutput();
  // Bulk load a table file in the relation
  void BulkLoad();
  // TODO: Not Implemented
  // Scour the database files and generate statistics
  void UpdateStatistics();
  // Close the database application
  void End();
};

#endif
