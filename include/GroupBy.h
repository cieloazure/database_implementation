#ifndef GROUPBY_H
#define GROUPBY_H

#include "Pipe.h"
#include "RelationalOp.h"
#include "Function.h"

class GroupBy : public RelationalOp {
public:
void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
};

#endif