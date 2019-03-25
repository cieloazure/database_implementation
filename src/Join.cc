#include "Join.h"
#include <iostream>
#include "BigQ.h"

struct JoinWorkerThreadParams {
  Pipe *inPipeL;
  Pipe *inPipeR;
  Pipe *outPipe;
  CNF *selOp;
  Record *literal;
};

struct JoinWorkerThreadParams join_thread_data;

void *JoinWorkerThreadRoutine(void *threadparams) {
  struct JoinWorkerThreadParams *params;
  params = (struct JoinWorkerThreadParams *)threadparams;

  Pipe *inPipeL = params->inPipeL;
  Pipe *inPipeR = params->inPipeR;
  Pipe *outPipe = params->outPipe;
  CNF *selOp = params->selOp;
  Record *literal = params->literal;

  // Join logic
  OrderMaker leftOrderMaker;
  OrderMaker rightOrderMaker;
  if (selOp->GetSortOrders(leftOrderMaker, rightOrderMaker)) {
    // sort-merge join
    Pipe outPipeL(100);
    BigQ leftbigq(*inPipeL, outPipeL, leftOrderMaker, 10);

    Pipe outPipeR(100);
    BigQ rightbigq(*inPipeR, outPipeR, rightOrderMaker, 10);

    Record leftRec;
    Record rightRec;

    ComparisonEngine compEng;

    bool leftHasElem = outPipeL.Remove(&leftRec);
    bool rightHasElem = outPipeR.Remove(&rightRec);
    while (leftHasElem && rightHasElem) {
      int compStatus = compEng.Compare(&leftRec, &leftOrderMaker, &rightRec,
                                       &rightOrderMaker);
      if (compStatus == 0) {
        Record mergedRec;
        // mergedRec.MergeRecords(&leftRec, &rightRec,
        //                      numAttsLeft, numAttsRight,
        //                      attsToKeep, numAttsToKeep, startOfRight);
      } else if (compStatus < 0) {
        leftHasElem = outPipeL.Remove(&leftRec);
      } else {
        rightHasElem = outPipeR.Remove(&rightRec);
      }
    }

    outPipeL.ShutDown();
    outPipeR.ShutDown();
  } else {
    // nested loop join
  }

  outPipe->ShutDown();
  pthread_exit(NULL);
}

Join::Join() {}
Join::~Join() {}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp,
               Record &literal) {
  pthread_attr_t attr;

  if (pthread_attr_init(&attr) == 0) {
    if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE) == 0) {
    } else {
      throw runtime_error("Error spawning Project worker");
    }
  } else {
    throw runtime_error("Error spawning Project worker");
  }

  join_thread_data.inPipeL = &inPipeL;
  join_thread_data.inPipeR = &inPipeR;
  join_thread_data.outPipe = &outPipe;
  join_thread_data.selOp = &selOp;
  join_thread_data.literal = &literal;

  pthread_create(&threadid, &attr, JoinWorkerThreadRoutine,
                 (void *)&join_thread_data);
}

void Join::WaitUntilDone() {
  cout << "Join waiting....." << endl;
  pthread_join(threadid, NULL);
  cout << "Join done!" << endl;
}
void Join::Use_n_Pages(int n) {}