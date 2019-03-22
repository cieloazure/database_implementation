#ifndef JOIN_H
#define JOIN_H

#include "Pipe.h"
#include "RelationalOp.h"

class Join : public RelationalOp {
 public:
  Join();
  ~Join();
  void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp,
           Record &literal);

  //   blocks the caller until the particular relational operator has run to
  //       completion virtual void
  void WaitUntilDone();

  // tells how much internal memory the operation can use virtual void
  void Use_n_Pages(int n);
};

#endif