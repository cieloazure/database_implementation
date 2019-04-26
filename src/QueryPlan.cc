#include "QueryPlan.h"

void QueryPlan::generateTree(struct JoinNode *joinNode)
{
    BaseNode *currentNode;

    // Handle DISTINCT
    if (distinctAtts == 1)
    {
        DuplicateRemovalNode *drNode = new DuplicateRemovalNode;
        if (currentNode)
        {
            currentNode->left = drNode;
        }
        else
        {
            currentNode = drNode;
        }
    }

    // Handle PROJECTS
    if (boolean)
    {
        AndList *currentAnd = boolean;
        vector<int> keepMe;
        while (currentAnd)
        {
            OrList *orList = currentAnd->left;
            currentAnd = currentAnd->rightAnd;
        }
    }
}