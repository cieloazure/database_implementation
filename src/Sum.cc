#include "Sum.h"
#include <iostream>

void *Sum ::SumWorkerThreadRoutine(void *threadparams) {
  struct SumWorkerThreadParams *params =
      (struct SumWorkerThreadParams *)threadparams;

  Pipe *inPipe = params->inPipe;
  Pipe *outPipe = params->outPipe;
  Function *computeMe = params->computeMe;

  // Sum logic here
  Record temp;
  Type t;

  int intAggregator = 0;
  double doubleAggregator = 0.0;

  while (inPipe->Remove(&temp)) {
    int intResult = 0;
    double doubleResult = 0.0;
    t = computeMe->Apply(temp, intResult, doubleResult);
    switch (t) {
      case Int:
        intAggregator += intResult;
        break;
      case Double:
        doubleAggregator += doubleResult;
        break;
      case String:
        break;
    }
  }

  Attribute sum_attr[1];
  sum_attr[0].name = "sum";
  sum_attr[0].myType = t;

  Schema sum_schema("sum", 1, sum_attr);
  Record sum_rec;
  string s = (computeMe->GetReturnsInt()) ? to_string(intAggregator)
                                          : to_string(doubleAggregator);
  s += '|';
  sum_rec.ComposeRecord(&sum_schema, s.c_str());

  outPipe->Insert(&sum_rec);
  outPipe->ShutDown();
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

  struct SumWorkerThreadParams *thread_data =
      (struct SumWorkerThreadParams *)malloc(
          sizeof(struct SumWorkerThreadParams));

  thread_data->inPipe = &inPipe;
  thread_data->outPipe = &outPipe;
  thread_data->computeMe = &computeMe;

  pthread_create(&threadid, &attr, SumWorkerThreadRoutine, (void *)thread_data);
}

Sum ::Sum() {}
Sum ::~Sum() {}

void Sum ::WaitUntilDone() {
  // std::cout << "Sum waiting...." << std::endl;
  pthread_join(threadid, NULL);
  // std::cout << "Sum done!" << std::endl;
}

void Sum ::Use_n_Pages(int n) {}