#ifndef GROUPBY_H
#define GROUPBY_H

#include "BigQ.h"
#include "Function.h"
#include "Pipe.h"
#include "RelationalOp.h"

class GroupBy : public RelationalOp {
 public:
  GroupBy();
  ~GroupBy();
  void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts,
           Function &computeMe);

  //   blocks the caller until the particular relational operator has run to
  //       completion virtual void
  void WaitUntilDone();

  // tells how much internal memory the operation can use virtual void
  void Use_n_Pages(int n);
};

#endif