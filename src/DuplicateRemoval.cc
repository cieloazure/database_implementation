#include "DuplicateRemoval.h"
#include <iostream>

void *DuplicateRemoval ::DuplicateRemovalWorkerThreadRoutine(
    void *threadparams) {
  struct DuplicateRemovalWorkerThreadParams *params;
  params = (struct DuplicateRemovalWorkerThreadParams *)threadparams;
  Pipe *inPipe = params->inPipe;
  Pipe *outPipe = params->outPipe;
  Schema *mySchema = params->mySchema;

  // Duplicate removal logic start
  OrderMaker sortOrder(mySchema);
  Pipe *sortedOutPipe = new Pipe(100);
  int runlen = 3;
  BigQ bq(*inPipe, *sortedOutPipe, sortOrder, runlen);

  Record *prev = new Record();
  if (!sortedOutPipe->Remove(prev)) {
    cout << "No records in input pipe/ BigQ input pipe close/No records to "
            "remove"
         << endl;
    pthread_exit(NULL);
  }
  Record *copy = new Record();
  copy->Copy(prev);
  outPipe->Insert(copy);

  Record *curr = new Record();
  ComparisonEngine comp;
  while (sortedOutPipe->Remove(curr)) {
    int status = comp.Compare(curr, prev, &sortOrder);
    if (status != 0) {
      Record *copy = new Record();
      copy->Copy(curr);
      outPipe->Insert(copy);
    }
    prev = curr;
    curr = new Record();
  }

  sortedOutPipe->ShutDown();
  // Duplicate removal logic end

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

  struct DuplicateRemovalWorkerThreadParams *thread_data =
      (struct DuplicateRemovalWorkerThreadParams *)malloc(
          sizeof(struct DuplicateRemovalWorkerThreadParams));

  thread_data->inPipe = &inPipe;
  thread_data->outPipe = &outPipe;
  thread_data->mySchema = &mySchema;

  pthread_create(&threadid, &attr, DuplicateRemovalWorkerThreadRoutine,
                 (void *)thread_data);
}

DuplicateRemoval ::DuplicateRemoval() {}
DuplicateRemoval ::~DuplicateRemoval() {}
void DuplicateRemoval ::WaitUntilDone() {
  cout << "Duplicate removal waiting..." << endl;
  pthread_join(threadid, NULL);
  cout << "Duplcate removal done!" << endl;
}
void DuplicateRemoval ::Use_n_Pages(int n) {}