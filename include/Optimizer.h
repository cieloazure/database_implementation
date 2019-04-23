#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <list>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include "../include/StatisticsState.h"

extern "C" struct FuncOperator *finalFunction;
extern "C" struct TableList *tables;
extern "C" struct AndList *boolean; // the predicate in the WHERE clause
extern "C" struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern "C" struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern "C" int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern "C" int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

class Optimizer{
    
    StatisticsState *currentState;
public:
    Optimizer();
    void Read(char *fromWhere);
    void ReadParserDatastructures();
};

#endif