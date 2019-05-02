#include "QueryPlan.h"

int Link::pool = 0;

QueryPlan::QueryPlan(BaseNode *r) { root = r; }

void QueryPlan::Execute() {
  // Create concrete pipes for the plan
  // Store it in a map
  // Traverse the tree and create RelationalOp instances
}
void QueryPlan::Print() { PrintTree(root); }

void QueryPlan::PrintTree(BaseNode *base) {
  if (base == NULL) return;
  PrintTree(base->left.value);
  switch (base->nodeType) {
    case BASE_NODE: {
      std::cout << "********" << std::endl;
      std::cout << "SENTINEL NODE" << std::endl;
      std::cout << "********" << std::endl;
      break;
    }
    case JOIN: {
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
    case RELATION_NODE: {
      std::cout << "********" << std::endl;
      RelationNode *r = dynamic_cast<RelationNode *>(base);
      std::cout << "RELATION NODE(LEAF)(SELECT FILE with special CNF OPERATION)"
                << std::endl;
      std::cout << "\tName:" << r->relName << std::endl;
      std::cout << "\tRecords on output pipe:" << r->parent.id << std::endl;
      std::cout << "\tRelation schema" << std::endl;
      base->schema->Print("\t\t");
      std::cout << "********" << std::endl;
      break;
    }
    case SELECT_PIPE: {
      JoinNode *s = dynamic_cast<JoinNode *>(base);
      std::cout << "********" << std::endl;
      std::cout << "SELECT PIPE OPERATION" << std::endl;
      std::cout << "\tInput pipe:" << base->left.id << std::endl;
      std::cout << "\tOutput pipe:" << base->parent.id << std::endl;
      std::cout << "\tSelect pipe CNF:";
      s->cnf->Print();
      std::cout << std::endl;
      std::cout << "********" << std::endl;
      break;
    }
    case SELECT_FILE: {
      JoinNode *s = dynamic_cast<JoinNode *>(base);
      std::cout << "********" << std::endl;
      std::cout << "SELECT FILE OPERATION" << std::endl;
      std::cout << "\tInput pipe:" << base->left.id << std::endl;
      std::cout << "\tOutput pipe:" << base->parent.id << std::endl;
      std::cout << "\tOutput Schema:" << std::endl;
      base->schema->Print("\t\t");
      std::cout << "\tSelect file CNF:";
      s->cnf->Print();
      std::cout << std::endl;
      break;
    }
    case SUM: {
      SumNode *s = dynamic_cast<SumNode *>(base);
      std::cout << "********" << std::endl;
      std::cout << "SUM OPERATION" << std::endl;
      std::cout << "\tInput pipe:" << base->left.id << std::endl;
      std::cout << "\tOutput pipe:" << base->parent.id << std::endl;
      std::cout << "\tFunction:" << std::endl;
      s->f->Print();
      std::cout << "********" << std::endl;
      break;
    }
    case GROUP_BY: {
      GroupByNode *g = dynamic_cast<GroupByNode *>(base);
      std::cout << "********" << std::endl;
      std::cout << "GROUP BY OPERATION" << std::endl;
      std::cout << "\tInput pipe:" << base->left.id << std::endl;
      std::cout << "\tOutput pipe:" << base->parent.id << std::endl;
      std::cout << "\tOrder Maker:" << std::endl;
      g->o->Print();
      std::cout << "\tFunction:" << std::endl;
      g->f->Print();
      std::cout << "********" << std::endl;
      break;
    }
    case PROJECT: {
      ProjectNode *p = dynamic_cast<ProjectNode *>(base);
      std::cout << "********" << std::endl;
      std::cout << "PROJECT OPERATION" << std::endl;
      std::cout << "\tInput pipe:" << base->left.id << std::endl;
      std::cout << "\tOutput pipe:" << base->parent.id << std::endl;
      std::cout << "Keep Me aray: ";
      for (int i = 0; i < p->numAttsOutput; ++i) {
        std::cout << p->keepMe[i] << " ";
      }
      std::cout << std::endl;
      std::cout << "********" << std::endl;
      break;
    }
    case DUPLICATE_REMOVAL: {
      DuplicateRemovalNode *d = dynamic_cast<DuplicateRemovalNode *>(base);
      std::cout << "********" << std::endl;
      std::cout << "DUPLICATE REMOVAL OPERATION" << std::endl;
      std::cout << "\tInput pipe:" << base->left.id << std::endl;
      std::cout << "\tOutput pipe:" << base->parent.id << std::endl;
      std::cout << "\tOutput Schema:" << std::endl;
      base->schema->Print("\t\t");
      std::cout << "********" << std::endl;
      break;
    }
    case WRITE_OUT: {
      WriteOutNode *w = dynamic_cast<WriteOutNode *>(base);
      std::cout << "********" << std::endl;
      std::cout << "WRITE OUT OPERATION" << std::endl;
      std::cout << "\tInput pipe:" << base->left.id << std::endl;
      std::cout << "\tOutput pipe:" << base->parent.id << std::endl;
      std::cout << "\tOutput Schema:" << std::endl;
      base->schema->Print("\t\t");
      std::cout << "********" << std::endl;
      break;
    }
    default: { std::cout << "ERROR or NOT HANDLED!" << std::endl; }
  }
  PrintTree(base->right.value);
}

void QueryPlan::SetOutput(WhereOutput op) {
  FILE *file;
  switch (op) {
    case StdOut:
      file = stdout;
      break;
    case None:
      file = NULL;
      break;
    case File:
      file = fopen(whereToGiveOutput, "w");
      break;
  }

  if (file != NULL) {
    BaseNode *child = root->left.value;
    WriteOutNode *writeOutNode = new WriteOutNode;
    writeOutNode->file = file;
    writeOutNode->schema = child->schema;

    Link writeToChild(child);
    writeOutNode->left = writeToChild;
    child->parent = writeToChild;

    Link sentinelToWrite(writeOutNode);
    root->left = sentinelToWrite;
    writeOutNode->parent = sentinelToWrite;
  }
}