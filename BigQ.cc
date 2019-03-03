#include "BigQ.h"
#include <math.h>
#include <pthread.h>
#include <unistd.h>
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

void CopyBufferToPage(Page *from, Page *to) {
  Record to_be_copied;
  while (from->GetFirst(&to_be_copied) != 0) {
    to->Append(&to_be_copied);
  }
}

// Phase 1
template <typename F>
void MergeKSortedPages(std::vector<Page *> &input, int k, File *runFile,
                       F &comparator, int nextPageIndex, Page *buffer) {
  // initialize a page for the merge
  Page *mergedPageBuffer = new Page();

  // Declaration and initialization of priority queue
  std::priority_queue<pq_elem_t, vector<pq_elem_t>, decltype(comparator)>
      pqueue(comparator);

  // initialize a priority queue with a pair consisting of the record in the
  // page and the index of the page in the vector
  for (int i = 0; i < input.size(); i++) {
    Record *tempRec = new Record();
    Page *tempPage = input.at(i);
    if (tempPage->GetFirst(tempRec) != 0) {
      pqueue.emplace(tempRec, i);
    }
  }

  // while the priority queue is not empty
  off_t pageIndexOffset = 0;
  while (!pqueue.empty()) {
    // remove the one with highest priority given by the comparator
    pq_elem_t dequeuedElem = pqueue.top();
    pqueue.pop();

    // put the record on the page we are merging
    if (mergedPageBuffer->Append(dequeuedElem.first) == 0) {
      // if the page is full, add the page to the file,
      Page *toBeAdded = new Page();
      CopyBufferToPage(
          mergedPageBuffer,
          toBeAdded); /* Copy is required to avoid double free error */
      // create a new page
      runFile->AddPage(toBeAdded, nextPageIndex + pageIndexOffset);
      pageIndexOffset++;

      // add the record which failed adding on the page back on the new page
      mergedPageBuffer->Append(dequeuedElem.first);
    }

    // get the next record from the page from that index in the vector,
    // add the pair with the record and index to the queue
    Page *dequeuedElemPage = input.at(dequeuedElem.second);
    Record *tempRec = new Record();
    if (dequeuedElemPage->GetFirst(tempRec) != 0) {
      pqueue.emplace(tempRec, dequeuedElem.second);
    }
  }  // end of while(!pqueue.empty())

  // Check if mergedPageBuffer still has some records to be added to the runFile
  if (mergedPageBuffer->GetNumRecords() > 0) {
    if (pageIndexOffset == input.size()) {
      // Runlength number of pages have already been written
      // The records in mergedPageBuffer are stray records to be handled in the
      // next iteration of createRun hence adding them back to the buffer
      CopyBufferToPage(mergedPageBuffer, buffer);
    } else {
      Page *toBeAdded = new Page();
      CopyBufferToPage(mergedPageBuffer, toBeAdded);
      runFile->AddPage(toBeAdded, nextPageIndex + pageIndexOffset);
    }
  }
}

void CreateRun(std::vector<Page *> &input, int k, File *runFile,
               OrderMaker sortOrder, int runIndex, Page *buffer) {
  ComparisonEngine comp;

  // Comparator for pair type in the priority queue
  // Priority queue comparator has reverse order than the sort order required
  auto comparator = [&sortOrder, &comp](pq_elem_t i1, pq_elem_t i2) -> bool {
    return comp.Compare(i1.first, i2.first, &sortOrder) >= 0;
  };

  MergeKSortedPages(input, k, runFile, comparator, runIndex, buffer);
}

// End of phase 1

// Phase 2
void StreamKSortedRuns(File *runFile, int runsCreated, int runLength,
                       OrderMaker sortOrder, Pipe *out) {
  std::cout << "Streaming K=" << runsCreated << " sorted runs" << std::endl;

  // priority queue initialization
  // Priority queue comparator has reverse order than the sort order required
  ComparisonEngine comp;

  auto comparator = [&sortOrder, &comp](pq_elem_t i1, pq_elem_t i2) -> bool {
    return comp.Compare(i1.first, i2.first, &sortOrder) >= 0;
  };

  std::priority_queue<pq_elem_t, vector<pq_elem_t>, decltype(comparator)>
      pqueue(comparator);

  int pageIndexes[runsCreated];
  vector<Page *> listOfHeads;

  int totalPages =
      runFile->GetLength() - 1;  // function returns total pages + 1.
  int run = 0;
  int runCounter = 0;

  // populate first page of every run
  if (totalPages > 0) {
    while (run < runsCreated) {
      Page *temp = new Page();
      runFile->GetPage(temp, (run * runLength));
      listOfHeads.push_back(temp);
      pageIndexes[run] = 0;
      run++;
    }
  } else {
    cout << "Runs are empty" << endl;
    return;
  }

  cout << "Pages populated successfully" << endl;
  // populate first rec in every page of listOfHeads into the priority queue.

  int record_count = 0;
  for (auto i : listOfHeads) {
    Record *tempRec = new Record();
    if (i->GetFirst(tempRec) == 1) {
      pqueue.emplace(tempRec, runCounter);
    } else {
      cout << "BAD run encountered." << endl;
    }
    runCounter++;
  }

  while (!pqueue.empty()) {
    pq_elem_t dequeuedElem = pqueue.top();
    pqueue.pop();

    // get current run from dequeuedElem.second
    int currentRun = dequeuedElem.second;

    out->Insert(dequeuedElem.first);

    Record *tempRec = new Record();
    if (listOfHeads[currentRun]->GetFirst(tempRec) == 1) {
      pqueue.emplace(tempRec, currentRun);
      record_count++;
    } else {
      pageIndexes[currentRun]++;

      if (pageIndexes[currentRun] < runLength) {
        // treat the last run differently, because it might not contain
        // runLength number of pages
        if (totalPages % runLength !=
            0) {  // less than runLength pages in the last run
          if ((currentRun == runsCreated - 1) &&
              (pageIndexes[currentRun] >= totalPages % runLength)) {
            continue;
          }
        }

        Page *temp = new Page();
        runFile->GetPage(temp,
                         (currentRun * runLength) + pageIndexes[currentRun]);

        if (temp->GetFirst(tempRec) == 1) {
          pqueue.emplace(tempRec, currentRun);
          listOfHeads[currentRun] = temp;
        }
      } else {
        // else the run is exausted and there is nothing to be done.
        // cout << "Run: " << currentRun << " is exausted." << endl;
      }
    }
  }
  cout<<"Added "<<record_count<<" to output queue."<<endl;
}
// End of phase 2

void *WorkerThreadRoutine(void *threadparams) {
  struct WorkerThreadParams *params;
  params = (struct WorkerThreadParams *)threadparams;
  Pipe *in = params->in;
  Pipe *out = params->out;
  OrderMaker sortOrder = params->sortOrder;
  int runlen = params->runlen;

  if (runlen <= 0 || in == NULL || out == NULL) {
    std::cout << "Argument Error in BigQ!" << std::endl;
    out->ShutDown();
    pthread_exit(NULL);
  }

  Record *temp = new Record();

  Page *buffer = new Page();
  std::vector<Page *> inputPagesForRun;
  File *runFile = new File();
  runFile->Open(0, (char *)"runFile.bin");

  int runs = 0;
  int total_record_count = 0;
  while (in->Remove(temp) != 0) {
    if (buffer->Append(temp) == 0) {
      // Page is full now
      // Sort the page and put it into an list which we have to merge for
      // Phase 1
      buffer->Sort(sortOrder);
      inputPagesForRun.push_back(buffer);

      // allocate a new Page for buffer
      buffer = new Page();

      if (inputPagesForRun.size() == runlen) {
        // Run Phase 1 to TPPMS and put the run into a file
        CreateRun(inputPagesForRun, runlen, runFile, sortOrder, runs * runlen,
                  buffer);

        runs++;
        // Empty inputPagesForRun
        inputPagesForRun.clear();
        // Continue to accept records till the pipe is open
      }

      // Append temp to the new Page so as not to lose temp
      buffer->Append(temp);
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
    CreateRun(inputPagesForRun, inputPagesForRun.size(), runFile, sortOrder,
              runs * runlen, buffer);
    runs++;
    inputPagesForRun.clear();
  }

  std::cout << "Merging and Streaming " << runs << " runs" << std::endl;

  // Ready for phase 2
  // Run Phase 2
  StreamKSortedRuns(runFile, runs, runlen, sortOrder, out);
  // Done with phase 2

  // CleanUp
  
  remove("runFile.bin");
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