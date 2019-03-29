#ifndef JOIN_H
#define JOIN_H

#include "HeapDBFile.h"
#include "Pipe.h"
#include "RelationalOp.h"

class Join : public RelationalOp {
 private:
  static void *JoinWorkerThreadRoutine(void *threadparams);

  static void ComposeMergedRecord(Record &left, Record &right,
                                  Schema *leftSchema, Schema *rightSchema,
                                  OrderMaker &rightOrderMaker,
                                  Record *mergedRec);
                  
  static void ComposeMergedRecord(Record &left, Record &right,
                                  Schema *leftSchema, Schema *rightSchema,
                                  Record *mergedRec);

  static void BlockNestedLoopJoinForSortMerge(vector<Page *> leftBuffers,
                                              vector<Page *> rightBuffers,
                                              Schema *leftSchema,
                                              Schema *rightSchema,
                                              OrderMaker &rightOrderMaker,
                                              Pipe *outPipe);

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

  struct JoinWorkerThreadParams {
    Pipe *inPipeL;
    Pipe *inPipeR;
    Pipe *outPipe;
    CNF *selOp;
    Record *literal;
  };
};

#endif