#include "Sum.h"

struct SumWorkerThreadParams {
  Pipe *inPipe;
  Pipe *outPipe;
  Function *computeMe;
};

struct SumWorkerThreadParams sum_thread_data;

void *SumWorkerThreadRoutine(void *threadparams) {
  struct SumWorkerThreadParams *params;
  params = (struct SumWorkerThreadParams *)threadparams;

  Pipe *inPipe = params->inPipe;
  Pipe *outPipe = params->outPipe;
  Function *computeMe = params->computeMe;

  // Sum logic here

  pthread_exit(NULL);
}

void Sum ::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
  pthread_attr_t attr;

  if (pthread_attr_init(&attr) == 0) {
    if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE) == 0) {
    } else {
      throw runtime_error("Error spawning Sum worker");
    }
  } else {
    throw runtime_error("Error spawning Sum worker");
  }

  sum_thread_data.inPipe = &inPipe;
  sum_thread_data.outPipe = &outPipe;
  sum_thread_data.computeMe = &computeMe;

  pthread_create(&threadid, &attr, SumWorkerThreadRoutine,
                 (void *)&sum_thread_data);
}

Sum ::Sum() {}
Sum ::~Sum() {}
void Sum ::WaitUntilDone() { pthread_join(threadid, NULL); }
void Sum ::Use_n_Pages(int n) {}