#include "GroupBy.h"
#include <iostream>

struct GroupByWorkerThreadParams {
  Pipe *inPipe;
  Pipe *outPipe;
  OrderMaker *groupAtts;
  Function *computeMe;
};

struct GroupByWorkerThreadParams group_by_thread_data;

void *GroupByWorkerThreadRoutine(void *threadparams) {
  struct GroupByWorkerThreadParams *params;
  params = (struct GroupByWorkerThreadParams *)threadparams;

  Pipe *inPipe = params->inPipe;
  Pipe *outPipe = params->outPipe;
  OrderMaker *groupAtts = params->groupAtts;
  Function *computeMe = params->computeMe;

  // GroupBy logic here
  groupAtts->Print();
  // Pipe *sortedOutPipe = new Pipe(100);
  // BigQ sortedGroupByQ(*inPipe, *sortedOutPipe, *groupAtts, 10);

  // Record temp;
  // while (sortedOutPipe->Remove(&temp)) {
  // }

  // outPipe->ShutDown();
  pthread_exit(NULL);
}

void GroupBy ::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts,
                   Function &computeMe) {
  pthread_attr_t attr;

  if (pthread_attr_init(&attr) == 0) {
    if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE) == 0) {
    } else {
      throw runtime_error("Error spawning GroupBy worker");
    }
  } else {
    throw runtime_error("Error spawning GroupBy worker");
  }

  group_by_thread_data.inPipe = &inPipe;
  group_by_thread_data.outPipe = &outPipe;
  group_by_thread_data.groupAtts = &groupAtts;
  group_by_thread_data.computeMe = &computeMe;

  pthread_create(&threadid, &attr, GroupByWorkerThreadRoutine,
                 (void *)&group_by_thread_data);
}

GroupBy ::GroupBy() {}
GroupBy ::~GroupBy() {}
void GroupBy ::WaitUntilDone() {
  cout << "Group By waiting....." << endl;
  pthread_join(threadid, NULL);
  cout << "Group by done...." << endl;
}
void GroupBy ::Use_n_Pages(int n) {}