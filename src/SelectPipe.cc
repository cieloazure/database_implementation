#include "SelectPipe.h"
#include <iostream>

void *SelectPipe ::WorkerThreadRoutine(void *threadparams) {
  // SelectPipe logic here
  std::cout << "In worker thread!" << std::endl;
  return NULL;
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

  thread_data.in = &inPipe;
  thread_data.out = &outPipe;
  thread_data.selOp = &selOp;
  thread_data.literal = &literal;

  pthread_create(&threadid, &attr, WorkerThreadRoutine, (void *)&thread_data);
}

SelectPipe ::SelectPipe() {}
SelectPipe ::~SelectPipe() {}
void SelectPipe ::WaitUntilDone() { pthread_join(threadid, NULL); }
void SelectPipe ::Use_n_Pages(int n) {}