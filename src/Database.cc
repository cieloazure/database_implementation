#include "Database.h"

int Database::runLength = 5;

Database::Database() {
  // See if a persistent file exists
  // If yes load the data structures from those files
  // If not create a file
}

void Database::ExecuteCommand(std::string command) {
  std::chrono::time_point<std::chrono::system_clock> start, end;
  yy_scan_string(command.c_str());
  yyparse();
  start = std::chrono::system_clock::now();
  if (!errorflag) {
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
      case 7:
        DisplayHelp();
        break;
    }
    end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsedSeconds = end - start;
    std::cout << "(" << elapsedSeconds.count() << " secs)" << std::endl;
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

  // dbFile->Close();
  RelationTuple *relTuple = new RelationTuple;
  relTuple->relName = relName;
  relTuple->schema = schema;
  relTuple->dbFile = dbFile;

  relationLookUp[relName] = relTuple;
  std::cout << "OK, (0 rows affected)" << std::endl;
}

void Database::BulkLoad() {
  std::string relName(bulkLoadInfo->tName);
  RelationTuple *relTuple = relationLookUp[relName];
  DBFile *dbFile = relTuple->dbFile;
  // dbFile->Open(relTuple->relName.c_str());
  dbFile->Load(*relTuple->schema, bulkLoadInfo->fName);
  std::cout << "OK, Bulk load done!" << std::endl;
}

void Database::SetOutput() {
  if (!strcmp(whereToGiveOutput, "STDOUT")) {
    op = StdOut;
  } else if (!strcmp(whereToGiveOutput, "NONE")) {
    op = None;
  } else {
    op = File;
  }
  std::cout << "OK, SetOutput done!" << std::endl;
}

void Database::ExecuteQuery() {
  QueryPlan *plan = optimizer->GetOptimizedPlan();
  plan->SetOutput(op);
  plan->Print();
  if (op != None) {
    plan->Execute();
  }
}

void Database::Start() {
  // Get query from user
  std::cout << "\n\n\n\nWelcome to Database Implementation Demo v0.1"
            << std::endl;
  std::cout << std::endl;
  std::cout << "Commands end with `;`" << std::endl;
  std::cout << std::endl;
  std::cout << "Type `help` for help" << std::endl;
  std::cout << "Type `Ctrl-D` or `Ctrl-C` to exit" << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;
  while (true) {
    cout << "dbi>  ";
    std::string query;
    std::string line;
    // std::cin.flush();
    bool enter = false;
    while (true) {
      if (enter) {
        query += "  ";
        std::cout << "  ->  ";
      }
      std::getline(std::cin, line);
      if (query == "" && line == "") {
        break;
      }
      if (std::cin.eof()) {
        break;
      }
      query += line;
      if (query[query.size() - 1] == ';') {
        break;
      }
      enter = true;
    }
    if (std::cin.eof()) {
      End();
      std::cout << "Bye!" << std::endl;
      break;
    }
    if (query.size() > 0) {
      std::cout << "------------------------DEBUG MODE----------------------"
                << std::endl;
      std::cout << "Executing Command -> " << query << std::endl;
      ExecuteCommand(query);
    }
  }
}

void Database::End() {
  // Save state
}

Database::Database(
    Statistics *stats,
    std::unordered_map<std::string, RelationTuple *> relNameToTuple) {
  currentStats = stats;
  relationLookUp = relNameToTuple;
  optimizer = new QueryOptimizer(currentStats, &relationLookUp);
}

Database::Database(QueryOptimizer *op) { optimizer = op; }

void Database::DropTable() {}
void Database::UpdateStatistics() {}

void Database::DisplayHelp() {
  std::string helpText(
      "\n\n\n\n1. CREATE TABLE [<table name>] ([<attribute name> <attribute "
      "type>], "
      "[<attribute name> <attribute type>],....) AS [HEAP|SORTED];"
      "\n2. SET OUTPUT [<file name> | STDOUT | NONE];"
      "\n3. INSERT ['file'] INTO [<table name>]"
      "\n4. DROP TABLE [<table name>]"
      "\n5. SELECT [DISTINCT] [<attribute>, <attribute>...] FROM [<table name> "
      "AS <alias name>, ...] WHERE [<or condition | join> AND <or condition | "
      "join> AND ...]"
      "\n5. SELECT [DISTINCT] [<attribute>, <attribute>....] FROM [<table "
      "name> AS <alias name>,....] WHERE [<or condition | join> AND <or "
      "condition | join> AND ...] GROUP BY [<attribute>, <attribute>]\n\n\n\n");
  std::cout << helpText << std::endl;
}