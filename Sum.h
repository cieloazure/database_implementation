#ifndef SUM_H
#define SUM_H

#include "Function.h"
#include "Pipe.h"
#include "RelationalOp.h"

class Sum : public RelationalOp {
 public:
  void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe);
};

#endif