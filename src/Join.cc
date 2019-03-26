#include "Join.h"
#include <iostream>
#include "BigQ.h"

void *Join ::JoinWorkerThreadRoutine(void *threadparams) {
  struct JoinWorkerThreadParams *params;
  params = (struct JoinWorkerThreadParams *)threadparams;

  Pipe *inPipeL = params->inPipeL;
  Pipe *inPipeR = params->inPipeR;
  Pipe *outPipe = params->outPipe;
  CNF *selOp = params->selOp;
  Record *literal = params->literal;

  // Join logic starts
  OrderMaker leftOrderMaker;
  OrderMaker rightOrderMaker;
  if (selOp->GetSortOrders(leftOrderMaker, rightOrderMaker)) {
    // sort-merge join start
    cout << "Starting sort-merge join" << endl;
    Pipe sortedOutPipeL(100);
    BigQ leftbigq(*inPipeL, sortedOutPipeL, leftOrderMaker, 10);

    Record left;
    int count = 0;
    while (sortedOutPipeL.Remove(&left)) {
      count++;
      outPipe->Insert(&left);
    }

    cout << "Removed " << count
         << " sorted records from outpipeL and inserted in main outpipe"
         << endl;

    Pipe sortedOutPipeR(100);
    BigQ rightbigq(*inPipeR, sortedOutPipeR, rightOrderMaker, 10);
    Record right;
    int count2 = 0;
    while (sortedOutPipeR.Remove(&right)) {
      count2++;
      outPipe->Insert(&right);
    }
    cout << "Removed " << count2
         << " sorted records from outPipeR and inserted in main outpipe"
         << endl;
    // sort-merge join end
  } else {
    // nested loop join start
    // nested loop join end
  }

  // Join logic ends
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

  struct JoinWorkerThreadParams *thread_data =
      (struct JoinWorkerThreadParams *)malloc(
          sizeof(struct JoinWorkerThreadParams));

  thread_data->inPipeL = &inPipeL;
  thread_data->inPipeR = &inPipeR;
  thread_data->outPipe = &outPipe;
  thread_data->selOp = &selOp;
  thread_data->literal = &literal;

  pthread_create(&threadid, &attr, JoinWorkerThreadRoutine,
                 (void *)thread_data);
}

void Join::WaitUntilDone() {
  cout << "Join waiting....." << endl;
  pthread_join(threadid, NULL);
  cout << "Join done!" << endl;
}
void Join::Use_n_Pages(int n) {}