#ifndef JOIN_H
#define JOIN_H

#include "Pipe.h"
#include "RelationalOp.h"

class Join : public RelationalOp {
public:
void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
};

#endif