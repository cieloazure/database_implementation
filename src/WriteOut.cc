#include "WriteOut.h"

struct WriteOutWorkerThreadParams {
  Pipe *inPipe;
  FILE *outFile;
  Schema *mySchema;
};

struct WriteOutWorkerThreadParams write_out_thread_data;

void *WriteOutWorkerThreadRoutine(void *threadparams) {
  struct WriteOutWorkerThreadParams *params;
  params = (struct WriteOutWorkerThreadParams *)threadparams;
  Pipe *in = params->inPipe;
  FILE *file = params->outFile;
  Schema *mySchema = params->mySchema;

  // WriteOut logic
  Record *temp = new Record();
  while (in->Remove(temp) != 0) {
    temp->TextFileVersion(mySchema, file);
  }

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

  write_out_thread_data.inPipe = &inPipe;
  write_out_thread_data.outFile = outFile;
  write_out_thread_data.mySchema = &mySchema;

  pthread_create(&threadid, &attr, WriteOutWorkerThreadRoutine,
                 (void *)&write_out_thread_data);
}

WriteOut::WriteOut() {}
WriteOut::~WriteOut() {}
void WriteOut::WaitUntilDone() { pthread_join(threadid, NULL); }
void WriteOut::Use_n_Pages(int n) {}
