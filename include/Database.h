#ifndef DATABASE_H
#define DATABASE_H

#include <iostream>
#include <stdio.h>
#include "QueryOptimizer.h"
#include "Schema.h"
#include "Statistics.h"
#include <unordered_map>

class Database
{
public:
    Statistics *currentStats;
    std::unordered_map<std::string, Schema *> relationToSchema;
    Database();

    void UpdateStatistics();
    void CreateTable();
    void DropTable();
    void ExecuteQuery();
    void SetOutput();
    void BulkLoad();
};

#endif
