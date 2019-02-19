
#include <stdlib.h>
#include <unistd.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include "BigQ.h"
#include "DBFile.h"
#include "Defs.h"
#include "Pipe.h"
#include "Record.h"
#include "TwoWayList.cc"
using namespace std;

extern "C" {
int yyparse(void);  // defined in y.tab.c
}

extern struct AndList *final;

class Test {
 public:
  int *num;
  Test() { num = new int; }
  Test(int a) {
    num = new int;
    (*num) = a;
  }

  void Consume(Test *fromme) { num = fromme->num; }
};

bool compare(void *i1, void *i2) {
  Test *i = (Test *)i1;
  Test *j = (Test *)i2;

  return *(i->num) < *(j->num);
}

int main() { return 0; }
