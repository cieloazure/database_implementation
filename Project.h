#ifndef PROJECT_H
#define PROJECT_H

#include "Pipe.h"
#include "RelationalOp.h"

class Project : public RelationalOp {
public:
void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
};

#endif