#ifndef DATABASE_H
#define DATABASE_H

#include <stdio.h>
#include "Schema.h"
#include <iostream>
#include <map>

class Database
{
public:
    std::map<int, Schema *> theMap;
    Database();
};

#endif
