#include "SelectPipe.h"
#include <iostream>

void *SelectPipe ::SelectPipeWorkerThreadRoutine(void *threadparams) {
  struct SelectPipeWorkerThreadParams *params;
  params = (struct SelectPipeWorkerThreadParams *)threadparams;
  Pipe *in = params->in;
  Pipe *out = params->out;
  CNF *selOp = params->selOp;
  Record *literal = params->literal;

  // Select Pipe logic start
  Record *temp = new Record();
  ComparisonEngine comp;

  while (in->Remove(temp) != 0) {
    if (comp.Compare(temp, literal, selOp)) {
      out->Insert(temp);
    }
  }
  // Select pipe logic end

  out->ShutDown();
  pthread_exit(NULL);
}

void SelectPipe ::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp,
                      Record &literal) {
  pthread_attr_t attr;

  if (pthread_attr_init(&attr) == 0) {
    if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE) == 0) {
    } else {
      throw runtime_error("Error spawning SelectPipe worker");
    }
  } else {
    throw runtime_error("Error spawning SelectPipe worker");
  }

  struct SelectPipeWorkerThreadParams *thread_data =
      (struct SelectPipeWorkerThreadParams *)malloc(
          sizeof(struct SelectPipeWorkerThreadParams));

  thread_data->in = &inPipe;
  thread_data->out = &outPipe;
  thread_data->selOp = &selOp;
  thread_data->literal = &literal;

  pthread_create(&threadid, &attr, SelectPipeWorkerThreadRoutine,
                 (void *)thread_data);
}

SelectPipe ::SelectPipe() {}
SelectPipe ::~SelectPipe() {}

void SelectPipe ::WaitUntilDone() {
  cout << "Select Pipe waiting...." << endl;
  pthread_join(threadid, NULL);
  cout << "Select Pipe done!" << endl;
}

void SelectPipe ::Use_n_Pages(int n) {}