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
int MergeKSortedPages(std::vector<Page *> &input, int k, File *runFile,
                      F &comparator, int nextPageIndex) {
  int record_count = 0;
  // initialize a page for the merge
  Page *mergedPage = new Page();

  // Declaration and initialization of priority queue
  std::priority_queue<pq_elem_t, vector<pq_elem_t>, decltype(comparator)>
      pqueue(comparator);

  // initialize a priority queue with a pair consisting of the record in the
  // page and the index of the page in the vector
  for (int i = 0; i < input.size(); i++) {
    Record *temp = new Record();
    Page *tempPage = input.at(i);
    if (tempPage->GetFirst(temp) != 0) {
      Schema mySchema("catalog", "lineitem");
      // cout << "Put in pqueue :" << endl;
      // temp->Print(&mySchema);
      pqueue.emplace(temp, i);
    }
  }

  // while the priority queue is not empty
  off_t pageIndex = 0;
  while (!pqueue.empty()) {
    // remove the one with highest priority given by the comparator
    pq_elem_t dequeuedElem = pqueue.top();
    pqueue.pop();

    // cout << "Popped from pqueue " << endl;
    Schema mySchema("catalog", "lineitem");
    // dequeuedElem.first->Print(&mySchema);

    // put the record on the page we are merging
    record_count++;
    if (mergedPage->Append(dequeuedElem.first) == 0) {
      // if the page is full, add the page to the file,
      Page *toBeAdded = new Page();
      CopyBufferToPage(
          mergedPage,
          toBeAdded); /* Copy is required to avoid double free error */
      // create a new page
      cout << "Page added " << nextPageIndex + pageIndex << endl;
      runFile->AddPage(toBeAdded, nextPageIndex + pageIndex);
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

  Page *toBeAdded = new Page();
  CopyBufferToPage(mergedPage, toBeAdded);
  cout << "Page added " << nextPageIndex + pageIndex << endl;
  runFile->AddPage(toBeAdded, nextPageIndex + pageIndex);
  pageIndex++;

  cout << "created a run with " << record_count << " records" << endl;
  return record_count;
}

int CreateRun(std::vector<Page *> &input, int k, File *runFile,
              OrderMaker sortOrder, int runIndex) {
  // Comparator for pair type in the priority queue
  ComparisonEngine comp;
  // Priority queue comparator has reverse order than the sort order required
  auto comparator = [&sortOrder, &comp](pq_elem_t i1, pq_elem_t i2) -> bool {
    return comp.Compare(i1.first, i2.first, &sortOrder) >= 0;
  };
  return MergeKSortedPages(input, k, runFile, comparator, runIndex);
}

// End of phase 1

// Phase 2
// Possible methodology -
// 1. Get the head of each run
// 2. Get a page from each run in memory and put it in a vector(array)
// 3. Initialize priority queue as in MergeKSortedPages
// 4. Pop the element from priority queue and enter the next element from the
// run 4.1. If the page of that run is out of records load the next page from
// that run 4.1.1 If the run is out of pages we are done with that run
// NOTE: Getting next elem from run -> Get the runIndex from the poppped element
// pair, get the page from vector, if page is out of records get a new page from
// disk, if the run is out of pages, we are done.
void StreamKSortedRuns(File *runFile, int runsCreated, int runLength,
                       OrderMaker sortOrder, Pipe *out) {
  std::cout << "Streaming K=" << runsCreated << " sorted runs" << std::endl;

  ComparisonEngine comp;
  // Priority queue comparator has reverse order than the sort order required
  auto comparator = [&sortOrder, &comp](pq_elem_t i1, pq_elem_t i2) -> bool {
    return comp.Compare(i1.first, i2.first, &sortOrder) >= 0;
  };

  std::priority_queue<pq_elem_t, vector<pq_elem_t>, decltype(comparator)>
      pqueue(comparator);
  int pageIndexes[runsCreated];
  vector<Page *> listOfHeads;
  Schema mySchema("catalog", "lineitem");

  int run = 0;

  // populate first page of every run
  // TDO: sanity check for empty runs
  while (run < runsCreated) {
    Page *temp = new Page();
    runFile->GetPage(temp, (run * runLength));
    listOfHeads.push_back(temp);
    pageIndexes[run] = 0;
    run++;
  }
  cout << "Pages populated successfully" << endl;
  // populate first rec in every page of listOfHeads into the priority queue.

  int runCounter = 0;
  int runsEnded = 0;
  int recordCounter = 0;

  // priority queue initialization
  // TODO: can be moved after putting the page in the vector in order to save
  // time
  for (auto i : listOfHeads) {
    if (pageIndexes[runCounter] == -1) {
      // skip the process if this run is out of pages.
      runCounter++;
      continue;
    }
    Record *tempRec = new Record();
    if (i->GetFirst(tempRec) == 1) {
      pqueue.emplace(tempRec, runCounter);
    } else {
      // the page is empty, so replace it with a new page
      // if no new page exists, skip this entry because we can't reduce the
      // size of listOfHeads as we are iterating over it
      pageIndexes[runCounter]++;

      if (pageIndexes[runCounter] == runLength - 1) {
        // this run is out of pages.
        pageIndexes[runCounter] = -1;
        runsEnded++;
        cout << "Number of runs processed: " << runsEnded << endl;
      } else {
        // if this run has another page, get it's first record and
        // replace the empty page with this new one
        Page *temp = new Page();
        runFile->GetPage(temp,
                         (runCounter * runLength) + pageIndexes[runCounter]);
        temp->GetFirst(tempRec);
        pqueue.emplace(tempRec, runCounter);

        listOfHeads[runCounter] = temp;
      }
    }
    runCounter++;
  }

  cout << "Priority queue size: " << pqueue.size() << endl;

  // ************** TODO: IMPORTANT ***************************************
  // pop the element from priority queue
  // fetch the next record from run index
  // while fetching the record check if a page exists in that run using the
  // pageIndexes[] array
  // COMMENT OUT IF RUNNING MAIN
  while (!pqueue.empty()) {
    pq_elem_t dequeuedElem = pqueue.top();
    pqueue.pop();

    out->Insert(dequeuedElem.first);
    //   // ***************************** TODO **************************
    //   get the run from dequeuedElem.second
    //   check if records exists in the page in the vector
    //   If no records exists on that page get the next page of that run in the
    //   vector and put it in the same index as dequeuedElem.second r
    //    if(pageIndex[runIndex] < runlen){
    //   runIndex = dequeuedElem.second
    //   pageIndexes[runIndex]++
    //   }else{
    //  Nothing to do
    //   }
  }

  // ************** IGNORE ***********************
  // Schema mySchema("catalog", "lineitem");
  // while (run < runsCreated)
  // {
  //   int page = 0;
  //   cout << " *******  RUN #" << run << "****************" << endl;
  //   cout << endl
  //        << endl;
  //   while (page < runLength)
  //   {
  //     Page *temp = new Page();
  //     runFile->GetPage(temp, (run * runLength) + page);
  //     Record *tempRec = new Record();
  //     int count = 0;
  //     while (temp->ReadNext(*tempRec, count) != 0)
  //     {
  //       tempRec->Print(&mySchema);
  //       count++;
  //     }
  //     page++;
  //   }
  //   run++;
  // }
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
  int total_record_count = 0;
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
        total_record_count += CreateRun(inputPagesForRun, runlen, runFile,
                                        sortOrder, runs * runlen);

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
    total_record_count +=
        CreateRun(inputPagesForRun, inputPagesForRun.size(), runFile, sortOrder,
                  runs * inputPagesForRun.size());
    runs++;
    inputPagesForRun.clear();
  }

  cout << "Merging and Streaming " << total_record_count << " records from "
       << runs << " runs" << endl;

  // Ready for phase 2
  // Run Phase 2
  StreamKSortedRuns(runFile, runs, runlen, sortOrder, out);

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