#include "Database.h"

int Database::runLength = 5;

Database::Database() {
  // See if a persistent file exists
  // If yes load the data structures from those files
  // If not create a file
}

void Database::CreateTable(std::string createTableQuery) {
  // Scan the create table query
  yy_scan_string(createTableQuery.c_str());
  yyparse();

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
    if (attType == "INTEGER") {
      a.myType = Int;
    } else if (attType == "DOUBLE") {
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
        sortCNFString += "(" + temp + "=" + temp + ")";
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

  RelationTuple *relTuple = new RelationTuple;
  relTuple->relName = relName;
  relTuple->schema = schema;
  relTuple->dbFile = dbFile;

  relationLookUp[relName] = relTuple;
}
void Database::BulkLoad() {}
void Database::DropTable() {}
void Database::SetOutput() {}
void Database::ExecuteQuery() {}
void Database::UpdateStatistics() {}