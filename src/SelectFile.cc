#include "SelectFile.h"
#include <iostream>

struct SelectFileWorkerThreadParams {
  DBFile *in;
  Pipe *out;
  CNF *selOp;
  Record *literal;
};

struct SelectFileWorkerThreadParams select_file_thread_data;

void *SelectFileWorkerThreadRoutine(void *threadparams) {
  std::cout << "Spawned select file worker thread" << std::endl;
  struct SelectFileWorkerThreadParams *params;
  params = (struct SelectFileWorkerThreadParams *)threadparams;
  DBFile *in = params->in;
  Pipe *out = params->out;
  CNF *selOp = params->selOp;
  Record *literal = params->literal;

  Record *temp = new Record();
  ComparisonEngine comp;

  in->MoveFirst();

  while (in->GetNext(*temp) != 0) {
    std::cout << "Removed a record from file" << std::endl;
    if (comp.Compare(temp, literal, selOp)) {
      Record *copy = new Record();
      copy->Copy(temp);
      out->Insert(copy);
    }
  }

  out->ShutDown();
  pthread_exit(NULL);
}

void SelectFile ::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp,
                      Record &literal) {
  pthread_attr_t attr;

  if (pthread_attr_init(&attr) == 0) {
    if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE) == 0) {
    } else {
      throw runtime_error("Error spawning SelectFile worker");
    }
  } else {
    throw runtime_error("Error spawning SelectFile worker");
  }

  select_file_thread_data.in = &inFile;
  select_file_thread_data.out = &outPipe;
  select_file_thread_data.selOp = &selOp;
  select_file_thread_data.literal = &literal;

  pthread_create(&threadid, &attr, SelectFileWorkerThreadRoutine,
                 (void *)&select_file_thread_data);
}

SelectFile ::SelectFile() = default;
SelectFile ::~SelectFile() {}
void SelectFile ::WaitUntilDone() {
  cout << "Waiting..." << endl;
  pthread_join(threadid, NULL);
}

void SelectFile ::Use_n_Pages(int n) {}