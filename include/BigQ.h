#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "File.h"
#include "Pipe.h"
#include "Record.h"

using namespace std;

class BigQ {
 private:
  static std::string random_string(size_t length);
  static void CopyBufferToPage(Page *from, Page *to);

  template <typename F>
  static void MergeKSortedPages(std::vector<Page *> &input, int k,
                                File *runFile, F &comparator, int nextPageIndex,
                                Page *buffer);

  static void CreateRun(std::vector<Page *> &input, int k, File *runFile,
                        OrderMaker sortOrder, int runIndex, Page *buffer);

  static void StreamKSortedRuns(File *runFile, int runsCreated, int runLength,
                                OrderMaker sortOrder, Pipe *out);

  static void *WorkerThreadRoutine(void *threadparams);

 public:
  BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
  ~BigQ();

  struct WorkerThreadParams {
    Pipe *in;
    Pipe *out;
    OrderMaker *sortOrder;
    int *runlen;
  };
};

#endif
