#include "QueryPlan.h"

QueryPlan::QueryPlan(Statistics *s)
{
    statsObject = s;
}

void QueryPlan::generateTree(struct JoinNode *joinNode)
{
    // BaseNode *currentNode;

    // // Handle DISTINCT
    // if (distinctAtts == 1)
    // {
    //     DuplicateRemovalNode *drNode = new DuplicateRemovalNode;
    //     if (currentNode)
    //     {
    //         currentNode->left = drNode;
    //     }
    //     else
    //     {
    //         currentNode = drNode;
    //     }
    // }

    // // Handle PROJECTS.
    // if (attsToSelect)
    // {
    //     vector<int> keepMe;
    //     NameList *nameList = attsToSelect;

    //     while (nameList)
    //     {
    //         for (auto sch : statsObject->currentState->schemaList) {
    //             // for (auto att : sch.){}
    //         }
    //     }
    // }

    // // Handle SELECTS.
    // if (boolean)
    // {
    //     CNF *cnf = new CNF;

    //     // cnf->GrowFromParseTree(boolean, )
    //     // AndList *currentAnd = boolean;

    //     // while (currentAnd)
    //     // {
    //     //     OrList *orList = currentAnd->left;
    //     //     while (orList)
    //     //     {
    //     //         struct ComparisonOp *compOp = orList->left;

    //     //         orList = orList->rightOr;
    //     //     }

    //     //     currentAnd = currentAnd->rightAnd;
    //     }
    // }
}

bool QueryPlan::IsQualifiedAtt(std::string value)
{
    return value.find('.', 0) != std::string::npos;
}

std::pair<std::string, std::string> QueryPlan::SplitQualifiedAtt(
    std::string value)
{
    size_t idx = value.find('.', 0);
    std::string rel;
    std::string att;
    if (idx == std::string::npos)
    {
        att = value;
    }
    else
    {
        rel = value.substr(0, idx);
        att = value.substr(idx + 1, value.length());
    }
    std::pair<std::string, std::string> retPair;
    retPair.first = rel;
    retPair.second = att;
    return retPair;
}