#ifndef PROJECT_H
#define PROJECT_H

#include <algorithm>
#include "Pipe.h"
#include "RelationalOp.h"

class Project : public RelationalOp {
  // Phyical operator for Project relations
 private:
  static void *ProjectWorkerThreadRoutine(void *threadparams);

 public:
  Project();
  ~Project();
  void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput,
           int numAttsOutput);
  void WaitUntilDone();
  void Use_n_Pages(int n);

  struct ProjectWorkerThreadParams {
    Pipe *in;
    Pipe *out;
    int *keepMe;
    int numAttsInput;
    int numAttsOutput;
  };
};

#endif