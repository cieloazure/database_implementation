#include "WriteOut.h"
#include <iostream>

void *WriteOut ::WriteOutWorkerThreadRoutine(void *threadparams) {
  struct WriteOutWorkerThreadParams *params;
  params = (struct WriteOutWorkerThreadParams *)threadparams;
  Pipe *in = params->inPipe;
  FILE *file = params->outFile;
  Schema *mySchema = params->mySchema;

  // WriteOut logic
  Record *temp = new Record();
  int count = 0;
  while (in->Remove(temp) != 0) {
    string s = temp->TextFileVersion(mySchema);
    fprintf(file, "%s\n", s.c_str());
    count++;
  }
  std::cout << "OK, " << count << " rows returned";
  // WriteOut logic end here

  pthread_exit(NULL);
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
  pthread_attr_t attr;

  if (pthread_attr_init(&attr) == 0) {
    if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE) == 0) {
    } else {
      throw runtime_error("Error spawning SelectPipe worker");
    }
  } else {
    throw runtime_error("Error spawning SelectPipe worker");
  }

  struct WriteOutWorkerThreadParams *thread_data =
      (struct WriteOutWorkerThreadParams *)malloc(
          sizeof(struct WriteOutWorkerThreadParams));

  thread_data->inPipe = &inPipe;
  thread_data->outFile = outFile;
  thread_data->mySchema = &mySchema;

  pthread_create(&threadid, &attr, WriteOutWorkerThreadRoutine,
                 (void *)thread_data);
}

WriteOut::WriteOut() {}
WriteOut::~WriteOut() {}
void WriteOut::WaitUntilDone() {
  // cout << "Write out waiting...." << endl;
  pthread_join(threadid, NULL);
  // cout << "Write out done!" << endl;
}
void WriteOut::Use_n_Pages(int n) {}
