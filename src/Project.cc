#include "Project.h"
#include <iostream>

void *Project ::ProjectWorkerThreadRoutine(void *threadparams) {
  struct ProjectWorkerThreadParams *params;
  params = (struct ProjectWorkerThreadParams *)threadparams;

  Pipe *in = params->in;
  Pipe *out = params->out;
  int *keepMe = params->keepMe;
  int numAttsInput = params->numAttsInput;
  int numAttsOutput = params->numAttsOutput;

  // Project logic
  Record *temp = new Record();

  while (in->Remove(temp) != 0) {
    temp->Project(keepMe, numAttsOutput, numAttsInput);
    Record *copy = new Record();
    copy->Copy(temp);
    out->Insert(copy);
  }
  // Project logic ends

  out->ShutDown();
  pthread_exit(NULL);
}

Project::Project() = default;
Project::~Project() {}

void Project ::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput,
                   int numAttsOutput) {
  pthread_attr_t attr;

  if (pthread_attr_init(&attr) == 0) {
    if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE) == 0) {
    } else {
      throw runtime_error("Error spawning Project worker");
    }
  } else {
    throw runtime_error("Error spawning Project worker");
  }

  struct ProjectWorkerThreadParams *thread_data =
      (struct ProjectWorkerThreadParams *)malloc(
          sizeof(struct ProjectWorkerThreadParams));

  thread_data->in = &inPipe;
  thread_data->out = &outPipe;
  thread_data->keepMe = keepMe;
  thread_data->numAttsInput = numAttsInput;
  thread_data->numAttsOutput = numAttsOutput;

  pthread_create(&threadid, &attr, ProjectWorkerThreadRoutine,
                 (void *)thread_data);
}

void Project::WaitUntilDone() {
  cout << "Project Waiting...." << endl;
  pthread_join(threadid, NULL);
  cout << "Project done waiting!" << endl;
}

void Project::Use_n_Pages(int n) {}
