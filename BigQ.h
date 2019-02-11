#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "File.h"
#include "Pipe.h"
#include "Record.h"

using namespace std;

class BigQ {
 public:
  BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
  ~BigQ();
};

#endif
