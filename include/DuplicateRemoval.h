#ifndef DUPLICATEREMOVAL_H
#define DUPLICATEREMOVAL_H

#include "Pipe.h"
#include "RelationalOp.h"

class DuplicateRemoval : public RelationalOp {
public:
void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
};

#endif 