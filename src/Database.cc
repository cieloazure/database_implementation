#include "Database.h"

Database::Database() {}

void Database::UpdateStatistics() {}
void Database::CreateTable() {}
void Database::DropTable() {}
void Database::ExecuteQuery() {
  // Set up a dummy map of relNameToSchema
  Attribute IA = {(char *)"a", Int};
  Attribute IB = {(char *)"b", Int};
  Attribute IC = {(char *)"c", Int};
  Attribute ID = {(char *)"d", Int};

  Attribute rAtts[] = {IA, IB};
  Schema R("R", 2, rAtts);

  Attribute s1Atts[] = {IB, IC};
  Schema S("S", 2, s1Atts);

  Attribute tAtts[] = {IC, ID};
  Schema T("T", 2, tAtts);

  Attribute uAtts[] = {IA, ID};
  Schema U("U", 2, uAtts);

  std::unordered_map<std::string, Schema *> relNameToSchema;
  relNameToSchema["R"] = &R;
  relNameToSchema["S"] = &S;
  relNameToSchema["T"] = &T;
  relNameToSchema["U"] = &U;

  QueryOptimizer o(currentStats, &relNameToSchema);
}
void Database::SetOutput() {}
void Database::BulkLoad() {}