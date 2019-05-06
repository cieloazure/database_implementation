#ifndef SELECTPIPE_H
#define SELECTPIPE_H

#include "Pipe.h"
#include "RelationalOp.h"

class SelectPipe : public RelationalOp {
  // Physical operator to get the record from input pipe which match cnf and put
  // them on output pipe
 private:
  static void *SelectPipeWorkerThreadRoutine(void *threadparams);

 public:
  SelectPipe();
  ~SelectPipe();
  void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
  void WaitUntilDone();
  void Use_n_Pages(int n);

  struct SelectPipeWorkerThreadParams {
    Pipe *in;
    Pipe *out;
    CNF *selOp;
    Record *literal;
  };
};

#endif