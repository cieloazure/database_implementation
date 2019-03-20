#ifndef SELECTPIPE_H
#define SELECTPIPE_H

#include "Pipe.h"
#include "RelationalOp.h"

class SelectPipe : public RelationalOp {
 public:
  void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
};

#endif