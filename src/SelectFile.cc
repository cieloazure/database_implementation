#include "SelectFile.h"
#include <iostream>

void *SelectFile::SelectFileWorkerThreadRoutine(void *threadparams) {
  // std::cout << "Spawned select file worker thread" << std::endl;
  struct SelectFileWorkerThreadParams *params;
  params = (struct SelectFileWorkerThreadParams *)threadparams;
  DBFile *in = params->in;
  Pipe *out = params->out;
  CNF *selOp = params->selOp;
  Record *literal = params->literal;

  // Select File logic
  Record *temp = new Record();
  ComparisonEngine comp;

  in->MoveFirst();

  while (in->GetNext(*temp) != 0) {
    if (comp.Compare(temp, literal, selOp)) {
      Record *copy = new Record();
      copy->Copy(temp);
      out->Insert(copy);
    }
  }
  // Select File logic end

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

  struct SelectFileWorkerThreadParams *thread_data =
      (struct SelectFileWorkerThreadParams *)malloc(
          sizeof(struct SelectFileWorkerThreadParams));

  thread_data->in = &inFile;
  thread_data->out = &outPipe;
  thread_data->selOp = &selOp;
  thread_data->literal = &literal;

  pthread_create(&threadid, &attr, SelectFileWorkerThreadRoutine,
                 (void *)thread_data);
}

SelectFile ::SelectFile() = default;
SelectFile ::~SelectFile() {}

void SelectFile ::WaitUntilDone() {
  // cout << "Select File Waiting..." << endl;
  pthread_join(threadid, NULL);
  // cout << "Select File done!" << endl;
}

void SelectFile ::Use_n_Pages(int n) {}