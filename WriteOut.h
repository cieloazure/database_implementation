#ifndef WRITEOUT_H
#define WRITEOUT_H

#include "Pipe.h"
#include "RelationalOp.h"

class WriteOut : public RelationalOp {
 public:
  void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema);
};

#endif