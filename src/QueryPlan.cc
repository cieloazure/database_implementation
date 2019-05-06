#include "QueryPlan.h"

int Link::pool = 0;

QueryPlan::QueryPlan(BaseNode *r) { root = r; }

void QueryPlan::Execute() {
  // Create concrete pipes for the plan
  // and Store it in a map
  std::unordered_map<int, Pipe *> idToPipe;
  CreateConcretePipes(root, idToPipe);
  std::vector<RelationalOp *> operators;
  CreateRelationalOperators(root, idToPipe, operators);
  for (auto op : operators) {
    op->WaitUntilDone();
  }
}

void QueryPlan::CreateRelationalOperators(
    BaseNode *iter, std::unordered_map<int, Pipe *> &idToPipe,
    std::vector<RelationalOp *> &operators) {
  if (iter == NULL) return;
  CreateRelationalOperators(iter->left.value, idToPipe, operators);
  CreateRelationalOperators(iter->right.value, idToPipe, operators);
  switch (iter->nodeType) {
    case BASE_NODE: {
      break;
    }
    case JOIN: {
      Join *joinOp = new Join();
      operators.push_back(joinOp);
      JoinNode *joinNode = dynamic_cast<JoinNode *>(iter);
      Pipe *inL = idToPipe[joinNode->left.id];
      Pipe *inR = idToPipe[joinNode->right.id];
      Pipe *out = idToPipe[joinNode->parent.id];
      joinOp->Run(*inL, *inR, *out, *joinNode->cnf, *joinNode->literal);
      break;
    }
    case GROUP_BY: {
      GroupBy *groupByOp = new GroupBy();
      operators.push_back(groupByOp);
      GroupByNode *groupByNode = dynamic_cast<GroupByNode *>(iter);
      Pipe *in = idToPipe[groupByNode->left.id];
      Pipe *out = idToPipe[groupByNode->parent.id];
      groupByOp->Run(*in, *out, *groupByNode->groupAtts,
                     *groupByNode->computeMe);
      break;
    }
    case SELECT_FILE: {
      break;
    }
    case RELATION_NODE: {
      SelectFile *selectFileOp = new SelectFile();
      operators.push_back(selectFileOp);
      RelationNode *relNode = dynamic_cast<RelationNode *>(iter);
      Pipe *out = idToPipe[relNode->parent.id];
      selectFileOp->Run(*relNode->dbFile, *out, *relNode->cnf,
                        *relNode->literal);
      break;
    }
    case SUM: {
      Sum *sumOp = new Sum();
      operators.push_back(sumOp);
      SumNode *sumNode = dynamic_cast<SumNode *>(iter);
      Pipe *in = idToPipe[sumNode->left.id];
      Pipe *out = idToPipe[sumNode->parent.id];
      sumOp->Run(*in, *out, *sumNode->computeMe);
      break;
    }
    case PROJECT: {
      Project *projOp = new Project();
      operators.push_back(projOp);
      ProjectNode *projNode = dynamic_cast<ProjectNode *>(iter);
      Pipe *in = idToPipe[projNode->left.id];
      Pipe *out = idToPipe[projNode->parent.id];
      projOp->Run(*in, *out, projNode->keepMe, projNode->numAttsInput,
                  projNode->numAttsOutput);
      break;
    }
    case SELECT_PIPE: {
      SelectPipe *selectPipeOp = new SelectPipe();
      operators.push_back(selectPipeOp);
      SelectPipeNode *selectPipeNode = dynamic_cast<SelectPipeNode *>(iter);
      Pipe *in = idToPipe[selectPipeNode->left.id];
      Pipe *out = idToPipe[selectPipeNode->parent.id];
      selectPipeOp->Run(*in, *out, *selectPipeNode->cnf,
                        *selectPipeNode->literal);
      break;
    }
    case DUPLICATE_REMOVAL: {
      DuplicateRemoval *dupRemovalOp = new DuplicateRemoval();
      operators.push_back(dupRemovalOp);
      DuplicateRemovalNode *dupRemovalNode = dynamic_cast<DuplicateRemovalNode *>(iter);
      Pipe *in = idToPipe[dupRemovalNode->left.id];
      Pipe *out = idToPipe[dupRemovalNode->parent.id];
      dupRemovalOp->Run(*in, *out, *dupRemovalNode->schema);
      break;
    }
    case WRITE_OUT: {
      WriteOut *writeOutOp = new WriteOut();
      operators.push_back(writeOutOp);
      WriteOutNode *writeOutNode = dynamic_cast<WriteOutNode *>(iter);
      Pipe *in = idToPipe[writeOutNode->left.id];
      writeOutOp->Run(*in, writeOutNode->file, *writeOutNode->schema);
      break;
    }
  }
}

void QueryPlan::CreateConcretePipes(BaseNode *iter,
                                    std::unordered_map<int, Pipe *> &idToPipe) {
  if (iter == NULL) return;
  CreateConcretePipes(iter->left.value, idToPipe);
  // Assign pipe id to map
  if (iter->left.value != NULL) {
    idToPipe[iter->left.id] = new Pipe(100);
  }
  if (iter->right.value != NULL) {
    idToPipe[iter->right.id] = new Pipe(100);
  }
  CreateConcretePipes(iter->right.value, idToPipe);
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
      std::cout << "\tSelect Schema:" << std::endl;
      base->schema->Print("\t\t");
      std::cout << std::endl;
      std::cout << "********" << std::endl;
      break;
    }
    case SELECT_FILE: {
      SelectPipeNode *s = dynamic_cast<SelectPipeNode *>(base);
      std::cout << "********" << std::endl;
      std::cout << "SELECT FILE OPERATION" << std::endl;
      std::cout << "\tInput pipe:" << base->left.id << std::endl;
      std::cout << "\tOutput pipe:" << base->parent.id << std::endl;
      std::cout << "\tSelect file CNF:";
      s->cnf->Print();
      std::cout << "\tSelect Schema:" << std::endl;
      base->schema->Print("\t\t");
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
      s->computeMe->Print();
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
      g->groupAtts->Print();
      std::cout << "\tFunction:" << std::endl;
      g->computeMe->Print();
      std::cout << "\tGroupBy Schema:" << std::endl;
      base->schema->Print("\t\t");
      std::cout << "********" << std::endl;
      break;
    }
    case PROJECT: {
      ProjectNode *p = dynamic_cast<ProjectNode *>(base);
      std::cout << "********" << std::endl;
      std::cout << "PROJECT OPERATION" << std::endl;
      std::cout << "\tInput pipe:" << base->left.id << std::endl;
      std::cout << "\tOutput pipe:" << base->parent.id << std::endl;
      std::cout << "\tProjects attributes to keep: [";
      for (int i = 0; i < p->numAttsOutput; ++i) {
        std::cout << p->keepMe[i] << ",";
      }
      std::cout << "]" << std::endl;
      std::cout << "\tProject Schema:" << std::endl;
      p->schema->Print("\t\t");
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

    Link writeToChild(child, writeOutNode);
    writeOutNode->left = writeToChild;
    child->parent = writeToChild;

    Link sentinelToWrite(writeOutNode, root);
    root->left = sentinelToWrite;
    writeOutNode->parent = sentinelToWrite;
  }
}