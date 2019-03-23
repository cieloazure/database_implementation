#ifndef WRITEOUT_H
#define WRITEOUT_H

#include "Pipe.h"
#include "RelationalOp.h"
#include <stdio.h>
#include <stdlib.h>

class WriteOut : public RelationalOp {
 public:
  WriteOut();
  ~WriteOut();
  void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema);

  //   blocks the caller until the particular relational operator has run to
  //       completion virtual void
  void WaitUntilDone();

  // tells how much internal memory the operation can use virtual void
  void Use_n_Pages(int n);
};

#endif