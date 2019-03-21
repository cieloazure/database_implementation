#ifndef SELECTPIPE_H
#define SELECTPIPE_H

#include "Pipe.h"
#include "RelationalOp.h"

class SelectPipe : public RelationalOp {
 public:
  SelectPipe();
  ~SelectPipe();
  void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
  void WaitUntilDone();
  void Use_n_Pages(int n);

 private:
  static void *WorkerThreadRoutine(void *threadparams);

  struct WorkerThreadParams {
    Pipe *in;
    Pipe *out;
    CNF *selOp;
    Record *literal;
  };

  struct WorkerThreadParams thread_data;
};

#endif