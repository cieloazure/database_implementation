#include "GroupBy.h"

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
void GroupBy ::WaitUntilDone() { pthread_join(threadid, NULL); }
void GroupBy ::Use_n_Pages(int n) {}