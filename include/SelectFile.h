#ifndef SELECTFILE_H
#define SELECTFILE_H

#include "DBFile.h"
#include "Pipe.h"
#include "RelationalOp.h"

class SelectFile : public RelationalOp {
 public:
  SelectFile();
  ~SelectFile();
  void Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
  void WaitUntilDone();
  void Use_n_Pages(int n);
};

#endif