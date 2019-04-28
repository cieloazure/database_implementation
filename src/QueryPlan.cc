#include "QueryPlan.h"

int Link::pool = 0;

QueryPlan::QueryPlan(BaseNode *r) { root = r; }

void QueryPlan::Execute() {}
void QueryPlan::Print() {}

void QueryPlan::PrintTree(BaseNode *base)
{
  if (base == NULL)
    return;
  PrintTree(base->left.value);
  switch (base->nodeType)
  {
  case BASE_NODE:
  {
    std::cout << "SENTINEL NODE" << std::endl;
    break;
  }
  case JOIN:
  {
    JoinNode *j = dynamic_cast<JoinNode *>(base);
    std::cout << "********" << std::endl;
    std::cout << "JOIN OPERATION" << std::endl;
    std::cout << "\tInput pipes:" << std::endl;
    std::cout << "\t\t1." << base->left.id << std::endl;
    std::cout << "\t\t2." << base->right.id << std::endl;
    std::cout << "\tOutput pipe:" << base->parent.id << std::endl;
    std::cout << "\tOutput Schema:" << std::endl;
    base->schema->Print("\t\t");
    std::cout << "\tJoin CNF:";
    j->cnf->Print();
    std::cout << std::endl;
    std::cout << "********" << std::endl;
    break;
  }
  case RELATION_NODE:
  {
    std::cout << "********" << std::endl;
    RelationNode *r = dynamic_cast<RelationNode *>(base);
    std::cout << "RELATION NODE(LEAF)" << std::endl;
    std::cout << "\tName:" << r->relName << std::endl;
    std::cout << "\tRecords on output pipe:" << r->parent.id << std::endl;
    std::cout << "\tRelation schema" << std::endl;
    base->schema->Print("\t\t");
    std::cout << "********" << std::endl;
    break;
  }
  case SELECT_PIPE:
  {
    JoinNode *s = dynamic_cast<JoinNode *>(base);
    std::cout << "********" << std::endl;
    std::cout << "SELECT PIPE OPERATION" << std::endl;
    std::cout << "\tInput pipes:" << std::endl;
    std::cout << "\t\t1." << base->left.id << std::endl;
    std::cout << "\t\t2." << base->right.id << std::endl;
    std::cout << "\tOutput pipe:" << base->parent.id << std::endl;
    std::cout << "\tSelect pipe CNF:";
    s->cnf->Print();
    std::cout << std::endl;
    std::cout << "********" << std::endl;
    break;
  }
  case SELECT_FILE:
  {
    JoinNode *s = dynamic_cast<JoinNode *>(base);
    std::cout << "********" << std::endl;
    std::cout << "SELECT FILE OPERATION" << std::endl;
    std::cout << "\tInput pipes:" << std::endl;
    std::cout << "\t\t1." << base->left.id << std::endl;
    std::cout << "\t\t2." << base->right.id << std::endl;
    std::cout << "\tOutput pipe:" << base->parent.id << std::endl;
    std::cout << "\tOutput Schema:" << std::endl;
    base->schema->Print("\t\t");
    std::cout << "\tSelect file CNF:";
    s->cnf->Print();
    std::cout << std::endl;
    break;
  }
  case SUM:
  {
    std::cout << "********" << std::endl;
    std::cout << "SUM OPERATION" << std::endl;
    std::cout << "********" << std::endl;
    break;
  }
  case GROUP_BY:
  {
    std::cout << "********" << std::endl;
    std::cout << "GROUP BY OPERATION" << std::endl;
    std::cout << "********" << std::endl;
    break;
  }
  case PROJECT:
  {
    ProjectNode *p = dynamic_cast<ProjectNode *>(base);
    std::cout << "********" << std::endl;
    std::cout << "PROJECT OPERATION" << std::endl;
    std::cout << "\tInput pipe:" << base->left.id << std::endl;
    std::cout << "\tOutput pipe:" << base->parent.id << std::endl;
    std::cout << "Keep Me aray: ";
    for (int i = 0; i < p->numAttsOutput; ++i)
    {
      std::cout << p->keepMe[i] << " ";
    }
    std::cout << std::endl;
    std::cout << "********" << std::endl;
    break;
  }
  case DUPLICATE_REMOVAL:
  {
    DuplicateRemovalNode *d = dynamic_cast<DuplicateRemovalNode *>(base);
    std::cout << "********" << std::endl;
    std::cout << "DUPLICATE REMOVAL" << std::endl;
    std::cout << "\tInput pipe:" << base->left.id << std::endl;
    std::cout << "\tOutput pipe:" << base->parent.id << std::endl;
    std::cout << "\tOutput Schema:" << std::endl;
    base->schema->Print("\t\t");
    std::cout << "********" << std::endl;
    break;
  }
  default:
  {
    std::cout << "ERROR or NOT HANDLED!" << std::endl;
  }
  }
  PrintTree(base->right.value);
}
