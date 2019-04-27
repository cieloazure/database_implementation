#include "Database.h"

Database::Database()
{
}

void Database::UpdateStatistics() {}
void Database::CreateTable() {}
void Database::DropTable() {}
void Database::ExecuteQuery()
{

    // Set up a dummy map of relNameToSchema
    Attribute IA = {(char *)"a", Int};
    Attribute IB = {(char *)"b", Int};
    Attribute IC = {(char *)"c", Int};
    Attribute ID = {(char *)"d", Int};

    Attribute rAtts[] = {IA, IB};
    Schema rSchema("rSchema", 2, rAtts);

    Attribute s1Atts[] = {IB, IC};
    Schema sSchema("sSchema", 2, s1Atts);

    Attribute tAtts[] = {IC, ID};
    Schema tSchema("tSchema", 2, tAtts);

    Attribute uAtts[] = {IA, ID};
    Schema uSchema("uSchema", 2, uAtts);

    std::unordered_map<std::string, Schema *> relNameToSchema;
    relNameToSchema["R"] = &rSchema;
    relNameToSchema["S"] = &sSchema;
    relNameToSchema["T"] = &tSchema;
    relNameToSchema["U"] = &uSchema;

    QueryOptimizer o(currentStats, &relNameToSchema);
}
void Database::SetOutput() {}
void Database::BulkLoad() {}