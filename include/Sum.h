#ifndef SUM_H
#define SUM_H

#include "Function.h"
#include "Pipe.h"
#include "RelationalOp.h"

class Sum : public RelationalOp {
 private:
  static void *SumWorkerThreadRoutine(void *threadparams);

 public:
  Sum();
  ~Sum();
  void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe);

  //   blocks the caller until the particular relational operator has run to
  //       completion virtual void
  void WaitUntilDone();

  // tells how much internal memory the operation can use virtual void
  void Use_n_Pages(int n);

  struct SumWorkerThreadParams {
    Pipe *inPipe;
    Pipe *outPipe;
    Function *computeMe;
  };
};

#endif