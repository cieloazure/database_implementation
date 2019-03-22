#ifndef DUPLICATEREMOVAL_H
#define DUPLICATEREMOVAL_H

#include "Pipe.h"
#include "RelationalOp.h"

class DuplicateRemoval : public RelationalOp {
 public:
  DuplicateRemoval();
  ~DuplicateRemoval();
  void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema);

  //   blocks the caller until the particular relational operator has run to
  //       completion virtual void
  void WaitUntilDone();

  // tells how much internal memory the operation can use virtual void
  void Use_n_Pages(int n);
};

#endif