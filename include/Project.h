#ifndef PROJECT_H
#define PROJECT_H

#include "Pipe.h"
#include "RelationalOp.h"
#include <algorithm>

class Project : public RelationalOp {
 public:
  Project();
  ~Project();
  void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput,
           int numAttsOutput);
  void WaitUntilDone();
  void Use_n_Pages(int n);
};

#endif