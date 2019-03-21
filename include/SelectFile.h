#ifndef SELECTFILE_H
#define SELECTFILE_H

#include "DBFile.h"
#include "Pipe.h"
#include "RelationalOp.h"

class SelectFile : public RelationalOp {

 public:
  void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);

};

#endif