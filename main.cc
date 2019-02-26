
#include <stdlib.h>
#include <unistd.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include "BigQ.h"
#include "Comparision.h"
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

int main() {
  Schema mySchema("catalog", "lineitem");
  OrderMaker o(&mySchema);

  int file_mode = O_TRUNC | O_RDWR | O_CREAT;
  int fd = open("test.bin", file_mode, S_IRUSR | S_IWUSR);
  o.Serialize(fd);

  OrderMaker p;
  lseek(fd, 0, SEEK_SET);
  p.UnSerialize(fd);
  return 0;
}
