#include "BigQ.h"
#include <math.h>
#include <pthread.h>
#include <vector>

struct WorkerThreadParams {
  Pipe *in;
  Pipe *out;
  OrderMaker sortOrder;
  int runlen;
};

struct WorkerThreadParams thread_data;

// Phase 1
void MergeKSortedPages(std::vector<Page *> &input, int k, File *runFile) {
  // initialize a page for the merge

  // initialize a priority queue with a pair consisting of the record in the
  // page and the index of the page in the vector

  // while the priority queue is not empty

  // remove the one with least priority

  // put the record on the page we are merging

  // if the page is full, add the page to the file, create a new page, add the
  // record which failed adding on the page and continue check the index from
  // which it is removed if there are still records in the page from that index

  // get the next record from the page from that index in the vector, add the
  // pair with record and index to the queue
}

void CreateRun(std::vector<Page *> &input, int k, File *runFile) {
  MergeKSortedPages(input, k, runFile);
}

// End of phase 1

// Phase 2
void StreamKSortedRuns(File *runFile, int runsCreated, int runLength,
                       Pipe *out) {
  // Calculate k
  int k = ceil(runsCreated / runLength);
}
// End of phase 2

void *WorkerThreadRoutine(void *threadparams) {
  std::cout << "Spawn Successful!" << std::endl;
  struct WorkerThreadParams *params;
  params = (struct WorkerThreadParams *)threadparams;
  Pipe *in = params->in;
  Pipe *out = params->out;
  OrderMaker sortOrder = params->sortOrder;
  int runlen = params->runlen;

  Record temp;

  Page *buffer = new Page();
  std::vector<Page *> inputPagesForRun;
  File *runFile = new File();
  runFile->Open(0, (char *)"runFile.bin");

  int runs = 0;
  while (in->Remove(&temp) != 0) {
    if (buffer->Append(&temp) == 0) {
      // Page is full now
      // Sort the page and put it into an list which we have to sort for Phase 1
      buffer->Sort(sortOrder);
      inputPagesForRun.push_back(buffer);

      if (inputPagesForRun.size() == runlen) {
        std::cout << "Got " << runlen << " Pages, beginning phase 1"
                  << std::endl;
        // Run Phase 1 to TPPMS and put the run into a file
        CreateRun(inputPagesForRun, runlen, runFile);
        runs++;
        // Empty inputPagesForRun
        inputPagesForRun.clear();
        // Continue to accept records till the pipe is open
      }

      // allocate a new Page for buffer
      buffer = new Page();

      // Append temp to the new Page so as not to lose temp
      buffer->Append(&temp);
    }
  }

  // Last Run

  // Are there any pages in inputPagesForRun?
  // Run Phase 1 of TPPMS and put it on the file
  std::cout << inputPagesForRun.size() << std::endl;

  // Are there any records in buffer?
  // Sort the page and put it on the file
  std::cout << buffer->GetNumRecords() << std::endl;

  buffer->Sort(sortOrder);
  inputPagesForRun.push_back(buffer);

  // Run phase 1 for the last run
  CreateRun(inputPagesForRun, inputPagesForRun.size(), runFile);
  runs++;
  inputPagesForRun.clear();

  // Ready for phase 2
  // Run Phase 2
  StreamKSortedRuns(runFile, runs, runlen, out);

  // Done with phase 2
  out->ShutDown();
  pthread_exit(NULL);
}

// read data from in pipe sort them into runlen pages
// construct priority queue over sorted runs and dump sorted data
// into the out pipe
// finally shut down the out pipe
BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
  pthread_t threadid;

  // The worker thread is detached from the main thread
  // We do not expect the worker thread to join
  // The caller should be expected the sorted records to appear on out and not
  // terminate
  pthread_attr_t attr;
  if (pthread_attr_init(&attr) == 0) {
    if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) == 0) {
    } else {
      throw runtime_error("Error spawning BigQ worker");
    }
  } else {
    throw runtime_error("Error spawning BigQ worker");
  }

  thread_data.in = &in;
  thread_data.out = &out;
  thread_data.runlen = runlen;
  thread_data.sortOrder = sortorder;

  pthread_create(&threadid, &attr, WorkerThreadRoutine, (void *)&thread_data);
}

BigQ::~BigQ() {}
