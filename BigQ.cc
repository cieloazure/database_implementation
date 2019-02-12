#include "BigQ.h"
#include <math.h>
#include <pthread.h>
#include <queue>
#include <utility>
#include <vector>

typedef std::pair<Record *, int> pq_elem_t;

struct WorkerThreadParams {
  Pipe *in;
  Pipe *out;
  OrderMaker sortOrder;
  int runlen;
};

struct WorkerThreadParams thread_data;

void CopyBufferToPage(Page *buffer, Page *flush_to_page) {
  Record to_be_copied;
  while (buffer->GetFirst(&to_be_copied) != 0) {
    flush_to_page->Append(&to_be_copied);
  }
}

// Phase 1
void MergeKSortedPages(std::vector<Page *> &input, int k, File *runFile,
                       OrderMaker &sortOrder) {
  // initialize a page for the merge
  Page *mergedPage = new Page();

  // Comparator for pair type in the priority queue
  ComparisonEngine comp;
  auto comparator = [&sortOrder, &comp](pq_elem_t i1, pq_elem_t i2) -> bool {
    return comp.Compare(i1.first, i2.first, &sortOrder) <= 0;
  };

  // Declaration and initialization of priority queue
  std::priority_queue<pq_elem_t, vector<pq_elem_t>, decltype(comparator)>
      pqueue(comparator);

  // initialize a priority queue with a pair consisting of the record in the
  // page and the index of the page in the vector
  for (int i = 0; i < input.size(); i++) {
    Record *temp = new Record();
    Page *tempPage = input.at(i);
    tempPage->GetFirst(temp);
    pqueue.emplace(temp, i);
  }

  // while the priority queue is not empty
  off_t pageIndex = 0;
  int count = 0;
  while (!pqueue.empty()) {
    // remove the one with highest priority given by the comparator
    pq_elem_t dequeuedElem = pqueue.top();
    pqueue.pop();

    // put the record on the page we are merging
    if (mergedPage->Append(dequeuedElem.first) == 0) {
      // if the page is full, add the page to the file,
      Page *toBeAdded = new Page();
      CopyBufferToPage(
          mergedPage,
          toBeAdded); /* Copy is required to avoid double free error */
      // create a new page
      runFile->AddPage(toBeAdded, pageIndex);
      pageIndex++;
      // add the record which failed adding on the page back on the new page
      mergedPage->Append(dequeuedElem.first);
    }

    // get the next record from the page from that index in the vector,
    // add the pair with the record and index to the queue
    Page *dequeuedElemPage = input.at(dequeuedElem.second);
    Record *temp = new Record();
    if (dequeuedElemPage->GetFirst(temp) != 0) {
      pqueue.emplace(temp, dequeuedElem.second);
    }
  }
}

void CreateRun(std::vector<Page *> &input, int k, File *runFile,
               OrderMaker sortOrder) {
  MergeKSortedPages(input, k, runFile, sortOrder);
}

// End of phase 1

// Phase 2
void StreamKSortedRuns(File *runFile, int runsCreated, int runLength,
                       Pipe *out) {
  // Calculate k
  int k = ceil(runsCreated / runLength);
  std::cout << "Streaming K sorted runs" << std::endl;
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
      // Sort the page and put it into an list which we have to sort for
      // Phase
      // 1
      buffer->Sort(sortOrder);
      inputPagesForRun.push_back(buffer);

      if (inputPagesForRun.size() == runlen) {
        // Run Phase 1 to TPPMS and put the run into a file
        CreateRun(inputPagesForRun, runlen, runFile, sortOrder);
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

  // Are there any records in buffer?
  // Sort the page and put it in the vector
  if (buffer->GetNumRecords() > 0) {
    buffer->Sort(sortOrder);
    inputPagesForRun.push_back(buffer);
  }

  // Are there any pages in inputPagesForRun?
  // Run phase 1 for the last run
  if (inputPagesForRun.size() > 0) {
    CreateRun(inputPagesForRun, inputPagesForRun.size(), runFile, sortOrder);
    runs++;
    inputPagesForRun.clear();
  }

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
  // The caller should be expected the sorted records to appear on out and
  // not terminate
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

/* Set custom stack size for a thread */
// #define NTHREADS 4
// #define N 1000
// #define MEGEXTRA 1000000
// size_t stacksize;
// pthread_attr_getstacksize(&attr, &stacksize);
// printf("Default stack size = %li\n", stacksize);
// stacksize = sizeof(double) * N * N + MEGEXTRA;
// printf("Amount of stack needed per thread = %li\n", stacksize);
// pthread_attr_setstacksize(&attr, stacksize);