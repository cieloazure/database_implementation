#ifndef SELECTFILE_H
#define SELECTFILE_H

#include "DBFile.h"
#include "Pipe.h"
#include "RelationalOp.h"

class SelectFile : public RelationalOp {
  // Physical relational operator for getting input from an existing, already
  // OPENED DBFile
 private:
  static void *SelectFileWorkerThreadRoutine(void *threadparams);

 public:
  SelectFile();
  ~SelectFile();
  void Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
  void WaitUntilDone();
  void Use_n_Pages(int n);

  struct SelectFileWorkerThreadParams {
    DBFile *in;
    Pipe *out;
    CNF *selOp;
    Record *literal;
  };
};

#endif