#include "DuplicateRemoval.h"

struct DuplicateRemovalWorkerThreadParams {
  Pipe *inPipe;
  Pipe *outPipe;
  Schema *mySchema;
};

struct DuplicateRemovalWorkerThreadParams duplicate_removal_thread_data;

void *DuplicateRemovalWorkerThreadRoutine(void *threadparams) {
  struct DuplicateRemovalWorkerThreadParams *params;
  params = (struct DuplicateRemovalWorkerThreadParams *)threadparams;
  Pipe *inPipe = params->inPipe;
  Pipe *outPipe = params->outPipe;
  Schema *mySchema = params->mySchema;
  OrderMaker sortOrder(mySchema);

  // Duplicate removal logic here

  //different pipes for the BigQ thread
  Pipe *bigqInPipe = new Pipe(100);
  Pipe *bigqOutPipe = new Pipe(100);
  int runlen = 3;

  BigQ bq(*inPipe, *bigqOutPipe, sortOrder, runlen);

  Record rec;
  Record *prev = NULL;
  while(bigqOutPipe->Remove(&rec)) {
    Record *copy = new Record;
    copy->Copy(&rec);

    if (prev == &rec) {
      continue;
    }
    prev = copy;
    outPipe->Insert(&rec);
  }

  bigqOutPipe->ShutDown();
  outPipe->ShutDown();
  pthread_exit(NULL);
}

void DuplicateRemoval ::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
  pthread_attr_t attr;

  if (pthread_attr_init(&attr) == 0) {
    if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE) == 0) {
    } else {
      throw runtime_error("Error spawning DuplicateRemoval worker");
    }
  } else {
    throw runtime_error("Error spawning DuplicateRemoval worker");
  }

  duplicate_removal_thread_data.inPipe = &inPipe;
  duplicate_removal_thread_data.outPipe = &outPipe;
  duplicate_removal_thread_data.mySchema = &mySchema;

  pthread_create(&threadid, &attr, DuplicateRemovalWorkerThreadRoutine,
                 (void *)&duplicate_removal_thread_data);
}

DuplicateRemoval ::DuplicateRemoval() {}
DuplicateRemoval ::~DuplicateRemoval() {}
void DuplicateRemoval ::WaitUntilDone() { pthread_join(threadid, NULL); }
void DuplicateRemoval ::Use_n_Pages(int n) {}