#include "Database.h"

int Database::runLength = 5;

Database::Database()
{
  // See if a persistent file exists
  // If yes load the data structures from those files
  // If not create a file
}

void Database::ExecuteCommand(std::string command)
{
  yy_scan_string(command.c_str());
  yyparse();
  switch (operationId)
  {
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

void Database::CreateTable()
{
  // DBFile and schema instances to be persisted
  DBFile *dbFile = new DBFile();
  Schema *schema = new Schema(newTable->tName);
  std::string relName(newTable->tName);

  // Add attributes to the schema
  struct SchemaAtts *head = newTable->schemaAtts;
  while (head != NULL)
  {
    Attribute a;
    a.name = strdup(head->attName);
    std::string attType(head->attType);
    if (!strcmp(attType.c_str(), "INTEGER"))
    {
      a.myType = Int;
    }
    else if (!strcmp(attType.c_str(), "DOUBLE"))
    {
      a.myType = Double;
    }
    else
    {
      a.myType = String;
    }
    schema->AddAttribute(a);
    head = head->next;
  }

  if (!strcmp(newTable->fileType, "HEAP"))
  {
    dbFile->Create(newTable->tName, heap, NULL);
  }
  else if (!strcmp(newTable->fileType, "SORTED"))
  {
    // Get the sortAtts if any
    std::vector<std::string> sortAtts;
    struct SortAtts *shead = newTable->sortAtts;
    while (shead != NULL)
    {
      std::string s(shead->name);
      sortAtts.push_back(s);
      shead = shead->next;
    }

    SortInfo *sortInfo = NULL;
    if (sortAtts.size() > 0)
    {
      // Create a sort CNF string to get the sortorder
      std::string sortCNFString;
      for (auto it = sortAtts.begin(); it != sortAtts.end(); it++)
      {
        if (sortCNFString.size() > 0)
        {
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

  (*relationLookUp)[relName] = relTuple;
}

void Database::BulkLoad()
{
  std::string relName(bulkLoadInfo->tName);
  RelationTuple *relTuple = (*relationLookUp)[relName];
  DBFile *dbFile = relTuple->dbFile;
  dbFile->Open(relTuple->relName.c_str());
  dbFile->Load(*relTuple->schema, bulkLoadInfo->fName);
}

void Database::SetOutput()
{
  if (!strcmp(whereToGiveOutput, "STDOUT"))
  {
    op = StdOut;
  }
  else if (!strcmp(whereToGiveOutput, "NONE"))
  {
    op = None;
  }
  else
  {
    op = File;
  }
}

void Database::ExecuteQuery()
{
  QueryPlan *plan = optimizer->GetOptimizedPlan();
  plan->SetOutput(op);
  plan->Print();
  plan->Execute();
}

void Database::DropTable() {}
void Database::UpdateStatistics() {}

void Database::Start()
{
  // Get query from user
  std::cout << "Welcome to Database Implementation Demo v0.1" << std::endl;
  std::cout << std::endl;
  std::cout << "Commands end with `;`" << std::endl;
  std::cout << std::endl;
  std::cout << "Type `help` for help" << std::endl;
  std::cout << "Type `Ctrl-D` or `Ctrl-C` to exit" << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;
  while (true)
  {
    cout << "dbi>  ";
    std::string query;
    std::string line;
    // std::cin.flush();
    bool enter = false;
    while (true)
    {
      if (enter)
      {
        query += "  ";
        std::cout << "  ->  ";
      }
      std::getline(std::cin, line);
      if (query == "" && line == "")
      {
        break;
      }
      if (std::cin.eof())
      {
        break;
      }
      query += line;
      if (query[query.size() - 1] == ';')
      {
        break;
      }
      enter = true;
    }
    if (std::cin.eof())
    {
      std::cout << "Bye!" << std::endl;
      break;
    }
    if (query.size() > 0)
    {
      std::cout << "--------------DEBUG MODE--------------" << std::endl;
      std::cout << "Given Query:" << query << std::endl;
      // Run optimization to get QueryPlan
      cout << "Query Plan:" << std::endl;
      QueryPlan *qp = optimizer->GetOptimizedPlan(query);
      qp->Print();
      qp->SetOutput(StdOut);
      qp->Execute();
    }
  }
}

Database::Database(
    Statistics *stats,
    std::unordered_map<std::string, RelationTuple *> *relNameToTuple)
{
  currentStats = stats;
  relationLookUp = relNameToTuple;
}

void Database::ReadPersistantFromData()
{
}

void Database::WritePersistantDataToFile(char *indexToWhere, char *schemaToWhere)
{
  // Write statistics to file.
  char *statsFile = "Statistics.txt";
  currentStats->Write(statsFile);

  std::ofstream indexFileDes(indexToWhere);

  std::ofstream catalogFileDes(schemaToWhere);

  // Write relationLookUp map to file.
  // Iterate through the disjoint set vector and write all the disjoint sets.
  for (std::unordered_map<std::string, RelationTuple *> it = (*relationLookUp).begin())
  {
    // Write relName to index file.
    indexFileDes << it.first << std::endl;
    // Write disjoint set noe to file
    WriteRelationLookUpNode(it.first, it.second, catalogFileDes);
  }
  // fclose(indexFileDes);
  // fclose(catalogFileDes);
}

void Database::WriteRelationLookUpNode(std::string relName, RelationTuple *relTuple, std::ofstream &toWhere)
{
  toWhere << "BEGIN" << std::endl;
  toWhere << relName << std::endl;
  toWhere << relName << ".tbl" << std::endl;

  for (int i = 0; i < relTuple->schema->numAtts; ++i)
  {
    toWhere << relTuple->schema->myAtts->name << " " << relTuple->schema->myAtts->myType << std::endl;
  }
  toWhere << "END" << std::endl;
  // Get length of the relation name &
  // Write the length of relation name string
  // size_t relNameLen = relName.size();
  // write(statisticsFileDes, &relNameLen, sizeof(size_t));

  // // Write the actual string of the relation name
  // write(statisticsFileDes, relName.c_str(), relNameLen);

  // // Get length of the relation name of parent&
  // // Write the length of relation name string
  // size_t parentRelNameLen = disjointSetNode->parent->relName.size();
  // write(statisticsFileDes, &parentRelNameLen, sizeof(size_t));

  // // Write the actual string of the relation name
  // write(statisticsFileDes, disjointSetNode->parent->relName.c_str(), parentRelNameLen);
}