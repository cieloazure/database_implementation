#include "Database.h"

int Database::runLength = 5;

Database::Database() {
  // See if a persistent file exists
  // If yes load the data structures from those files
  // If not create a file
}

void Database::ExecuteCommand(std::string command) {
  yy_scan_string(command.c_str());
  yyparse();
  switch (operationId) {
    case 1:
      CreateTable();
      break;
    case 2:
      ExecuteQuery();
      break;
    case 3:
      BulkLoad();
      break;
    case 4:
      DropTable();
      break;
    case 5:
      SetOutput();
      break;
    case 6:
      UpdateStatistics();
      break;
  }
}

void Database::CreateTable() {
  // DBFile and schema instances to be persisted
  DBFile *dbFile = new DBFile();
  Schema *schema = new Schema(newTable->tName);
  std::string relName(newTable->tName);

  // Add attributes to the schema
  struct SchemaAtts *head = newTable->schemaAtts;
  while (head != NULL) {
    Attribute a;
    a.name = strdup(head->attName);
    std::string attType(head->attType);
    if (!strcmp(attType.c_str(), "INTEGER")) {
      a.myType = Int;
    } else if (!strcmp(attType.c_str(), "DOUBLE")) {
      a.myType = Double;
    } else {
      a.myType = String;
    }
    schema->AddAttribute(a);
    head = head->next;
  }

  if (!strcmp(newTable->fileType, "HEAP")) {
    dbFile->Create(newTable->tName, heap, NULL);
  } else if (!strcmp(newTable->fileType, "SORTED")) {
    // Get the sortAtts if any
    std::vector<std::string> sortAtts;
    struct SortAtts *shead = newTable->sortAtts;
    while (shead != NULL) {
      std::string s(shead->name);
      sortAtts.push_back(s);
      shead = shead->next;
    }

    SortInfo *sortInfo = NULL;
    if (sortAtts.size() > 0) {
      // Create a sort CNF string to get the sortorder
      std::string sortCNFString;
      for (auto it = sortAtts.begin(); it != sortAtts.end(); it++) {
        if (sortCNFString.size() > 0) {
          sortCNFString += " AND ";
        }
        std::string temp(relName + "." + *it);
        sortCNFString += "(" + temp + " = " + temp + ")";
      }

      // Getting sortOrder for sorted File
      yy_scan_string(sortCNFString.c_str());
      yyparse();

      CNF cnf;
      Record literal;
      cnf.GrowFromParseTree(final, schema, literal);

      OrderMaker left;
      OrderMaker dummy;
      cnf.GetSortOrders(left, dummy);

      sortInfo = new SortInfo(&left, runLength);
    }
    dbFile->Create(newTable->tName, sorted, (void *)sortInfo);
  }

  dbFile->Close();
  RelationTuple *relTuple = new RelationTuple;
  relTuple->relName = relName;
  relTuple->schema = schema;
  relTuple->dbFile = dbFile;

  relationLookUp[relName] = relTuple;
}

void Database::BulkLoad() {
  std::string relName(bulkLoadInfo->tName);
  RelationTuple *relTuple = relationLookUp[relName];
  DBFile *dbFile = relTuple->dbFile;
  dbFile->Open(relTuple->relName.c_str());
  dbFile->Load(*relTuple->schema, bulkLoadInfo->fName);
}

void Database::SetOutput() {
  if (!strcmp(whereToGiveOutput, "STDOUT")) {
    op = StdOut;
  } else if (!strcmp(whereToGiveOutput, "NONE")) {
    op = None;
  } else {
    op = File;
  }
}

void Database::ExecuteQuery() {
  QueryPlan *plan = optimizer.GetOptimizedPlan();
  plan->SetOutput(op);
  plan->Print();
  // plan->Execute();
}

void Database::DropTable() {}
void Database::UpdateStatistics() {}