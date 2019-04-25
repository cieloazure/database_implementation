#ifndef QUERYPLAN_H
#define QUERYPLAN_H

#include <iostream>
#include "Schema.h"

enum
{
    DUPLICATE_REMOVAL,
    GROUP_BY,
    JOIN,
    PROJECT,
    SELECT_FILE,
    SELECT_PIPE,
    SUM
};

typedef struct BaseNode
{
    int nodeType;
    Schema *schema;
};
class QueryPlan
{
};

#endif