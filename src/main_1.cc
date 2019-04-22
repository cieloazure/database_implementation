
// #include <stdlib.h>
// #include <unistd.h>
// #include <algorithm>
// #include <fstream>
// #include <iostream>
// #include <random>
// #include <string>
// #include <vector>
// #include "BigQ.h"
// #include "Comparison.h"
// #include "DBFile.h"
// #include "Defs.h"
// #include "Pipe.h"
// #include "Record.h"
// #include "SortedDBFile.h"
// #include "TwoWayList.cc"
// using namespace std;

// extern "C" {
// int yyparse(void);  // defined in y.tab.c
// }

// extern struct AndList *final;

// struct SortInfo {
//   OrderMaker *sortOrder;
//   int runLength;

//   SortInfo(OrderMaker *so, int rl) {
//     sortOrder = so;
//     runLength = rl;
//   }
// };

// class Test {
//  public:
//   int *num;
//   Test() { num = new int; }
//   Test(int a) {
//     num = new int;
//     (*num) = a;
//   }

//   void Consume(Test *fromme) { num = fromme->num; }
// };

// bool compare(void *i1, void *i2) {
//   Test *i = (Test *)i1;
//   Test *j = (Test *)i2;

//   return *(i->num) < *(j->num);
// }

// int main() {
//   Schema mySchema("catalog", "lineitem");
//   OrderMaker o(&mySchema);

//   int file_mode = O_TRUNC | O_RDWR | O_CREAT;
//   int fd = open("test.bin", file_mode, S_IRUSR | S_IWUSR);
//   o.Serialize(fd);

//   OrderMaker p;
//   lseek(fd, 0, SEEK_SET);
//   p.UnSerialize(fd);

//   // DBFile *heapFile = new DBFile();
//   // fType t = heap;
//   // heapFile->Create("gtest.bin", t, NULL);
//   // Schema mySchema("catalog", "lineitem");
//   // const char *loadpath = "data_files/lineitem.tbl";
//   // heapFile->Load(mySchema, loadpath);
//   // heapFile->Close();

//   SortInfo *si = new SortInfo(&o, 3);

//   SortedDBFile *sortedDBFile = new SortedDBFile();
//   fType t1 = sorted;
//   sortedDBFile->Create("gtest_sorted.bin", t1, (void *)si);
//   const char *loadpath = "data_files/lineitem.tbl";
//   // sortedDBFile->Load(mySchema, loadpath);

//   Record *temp = new Record();
//   FILE *table_file = fopen(loadpath, "r");

//   int count = 0;
//   std::cout << "Loaded:" << endl;
//   while (temp->SuckNextRecord(&mySchema, table_file) == 1) {
//     if (temp != NULL) {
//       count++;
//       std::cout << "\r" << count;
//       sortedDBFile->Add(*temp);
//       if (count == 10) {
//         break;
//       }
//     }
//   }
//   Record *second = new Record();
//   while (sortedDBFile->GetNext(*second)) {
//     second->Print(&mySchema);
//     cout << endl;
//     cout << endl;
//   };

//   // sleep(50000000);
//   count = 0;
//   while (temp->SuckNextRecord(&mySchema, table_file) == 1) {
//     if (temp != NULL) {
//       count++;
//       std::cout << "\r" << count;
//       sortedDBFile->Add(*temp);
//       if (count == 10) {
//         break;
//       }
//     }
//   }

//   Record *first = new Record();
//   count = 0;
//   while (sortedDBFile->GetNext(*first)) {
//     first->Print(&mySchema);
//     count++;
//     cout << endl;
//     cout << endl;
//   };
//   cout << count << endl;

//   // if (temp->SuckNextRecord(&mySchema, table_file) == 1) {
//   //   sortedDBFile->Add(*temp);
//   // }

//   return 0;
// }
