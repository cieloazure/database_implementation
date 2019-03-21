#include "SelectPipe.h"

struct SelectPipeWorkerThreadParams {
  Pipe *in;
  Pipe *out;
  CNF *selOp;
  Record *literal;
};

struct SelectPipeWorkerThreadParams select_pipe_thread_data;

void *SelectPipeWorkerThreadRoutine(void *threadparams) {
  struct SelectPipeWorkerThreadParams *params;
  params = (struct SelectPipeWorkerThreadParams *)threadparams;
  Pipe *in = params->in;
  Pipe *out = params->out;
  CNF *selOp = params->selOp;
  Record *literal = params->literal;

  Record *temp = new Record();
  ComparisonEngine comp;

  while (in->Remove(temp) != 0) {
    if (comp.Compare(temp, literal, selOp)) {
      out->Insert(temp);
    }
  }

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

  select_pipe_thread_data.in = &inPipe;
  select_pipe_thread_data.out = &outPipe;
  select_pipe_thread_data.selOp = &selOp;
  select_pipe_thread_data.literal = &literal;

  pthread_create(&threadid, &attr, SelectPipeWorkerThreadRoutine,
                 (void *)&select_pipe_thread_data);
}

SelectPipe ::SelectPipe() {}
SelectPipe ::~SelectPipe() {}
void SelectPipe ::WaitUntilDone() { pthread_join(threadid, NULL); }
void SelectPipe ::Use_n_Pages(int n) {}