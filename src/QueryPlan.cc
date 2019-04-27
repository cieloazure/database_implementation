#include "QueryPlan.h"

int Link::pool = 0;

QueryPlan::QueryPlan(BaseNode *r) { root = r; }

void QueryPlan::Execute() {}
void QueryPlan::Print() {}