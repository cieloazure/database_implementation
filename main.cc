
#include <stdlib.h>
#include <iostream>
#include "DBFile.h"
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
  /*
  // try to parse the CNF
  cout << "Enter in your CNF: ";
  if (yyparse() != 0) {
  cout << "Can't parse your CNF.\n";
  exit(1);
  }

  // suck up the schema from the file
  Schema lineitem("catalog", "lineitem");

  // grow the CNF expression from the parse tree
  CNF myComparison;
  Record literal;
  myComparison.GrowFromParseTree(final, &lineitem, literal);

  // print out the comparison to the screen
  myComparison.Print();

  // now open up the text file and start procesing it
  FILE *tableFile = fopen(
  "/Users/akashshingte/Projects/Cpp/DBI - Assignment "
  "1/data_files/lineitem.tbl",
  "r");

  Record temp;
  Schema mySchema("catalog", "lineitem");

  // char *bits = literal.GetBits ();
  // cout << " numbytes in rec " << ((int *) bits)[0] << endl;
  // literal.Print (&supplier);

  // read in all of the records from the text file and see if they match
  // the CNF expression that was typed in
  int counter = 0;
  ComparisonEngine comp;
  while (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
  counter++;
  if (counter % 10000 == 0) {
  cerr << counter << "\n";
  }

  if (comp.Compare(&temp, &literal, &myComparison)) temp.Print(&mySchema);
  }
  */

  /*
  FILE *tableFile = fopen(
      "/Users/akashshingte/Projects/Cpp/DBI - Assignment "
      "1/data_files/lineitem.tbl",
      "r");

  Record temp;
  Schema mySchema("catalog", "lineitem");
  Page *newPage = new Page();
  int counter = 0;

  while (1) {
    if (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
      if (newPage -> Append(&temp) == 0) {
        break;
      } else {
        continue;
      }
    }
  }

  File *newFile = new File();
  char *newFileName = "test.tbl";
  newFile -> Open(0, newFileName);
  newFile -> AddPage(newPage, 1);

  Page *putItHere = new Page();
  newFile -> GetPage(putItHere, 1);

  cout << putItHere -> GetNumRecords();

  // Record temp2;
  // while (putItHere -> GetNumRecords() > 0) {
  //   putItHere -> GetFirst(&temp2);
  //   temp2.Print(&mySchema);
  // }

  newFile->Close();


  File *newFile2 = new File();
  newFile2 -> Open(1, newFileName);
  Page *putItHere2 = new Page();
  newFile2 -> GetPage(putItHere2, 2);
  cout << putItHere2 -> GetNumRecords();

  */

  /*
  DBFile *heapFile = new DBFile();
  fType f = heap;
  heapFile->Create("test.tbl", heap, NULL);

  FILE *tableFile = fopen(
      "/Users/akashshingte/Projects/Cpp/DBI - Assignment "
      "1/data_files/lineitem.tbl",
      "r");

  Record *temp = new Record();
  Schema mySchema("catalog", "lineitem");
  for (int i = 0; i < 10; i++) {
    temp->SuckNextRecord(&mySchema, tableFile);
    heapFile->Add(*temp);
  }

  heapFile->Close();

  heapFile->Open("test.tbl");
  Record *temp2 = new Record();
  while (heapFile->GetNext(*temp2) != 0) {
    temp2->Print(&mySchema);
  }
  cout << "-------MOVING FIRST-----------" << endl;
  heapFile->MoveFirst();
  while (heapFile->GetNext(*temp2) != 0) {
    temp2->Print(&mySchema);
  }
  heapFile->Close();
  heapFile->Open("test.tbl");
  for (int i = 0; i < 10; i++) {
    if (i % 2 == 0) {
      temp->SuckNextRecord(&mySchema, tableFile);
      heapFile->Add(*temp);
    } else {
      heapFile->GetNext(*temp2);
      temp2->Print(&mySchema);
    }
  }
  delete temp;
  delete temp2;
  heapFile->Close();
  */
  /*
  DBFile *heapFile = new DBFile();
  Record *temp = new Record();
  Schema mySchema("catalog", "lineitem");
  FILE *tableFile = fopen(
      "/Users/akashshingte/Projects/Cpp/DBI - Assignment "
      "1/data_files/lineitem.tbl",
      "r");
  fType f = heap;
  heapFile->Create("test.tbl", heap, NULL);
  if (heapFile->GetNext(*temp)) {
    temp->Print(&mySchema);
  }
  temp->SuckNextRecord(&mySchema, tableFile);
  heapFile->Add(*temp);
  // if (heapFile->GetNext(*temp)) {
  //   temp->Print(&mySchema);
  // }
  fclose(tableFile);
  heapFile->Close();
  delete temp;
  */

  /*
    DBFile *heapFile = new DBFile();
    fType f = heap;
    heapFile->Create("test.tbl", heap, NULL);

    FILE *tableFile = fopen(
        "/Users/akashshingte/Projects/Cpp/DBI - Assignment "
        "1/data_files/lineitem.tbl",
        "r");

    Record *temp = new Record();
    Schema mySchema("catalog", "lineitem");
    for (int i = 0; i < 50; i++) {
      temp->SuckNextRecord(&mySchema, tableFile);
      heapFile->Add(*temp);
    }

    heapFile->Close();
    // try to parse the CNF
    cout << "Enter in your CNF: ";
    if (yyparse() != 0) {
      cout << "Can't parse your CNF.\n";
      exit(1);
    }

    // suck up the schema from the file
    Schema lineitem("catalog", "lineitem");

    // grow the CNF expression from the parse tree
    CNF myComparison;
    Record literal;
    myComparison.GrowFromParseTree(final, &lineitem, literal);

    // print out the comparison to the screen
    myComparison.Print();

    cout << "Get next with  comparision" << endl;
    heapFile->Open("test.tbl");
    Record *temp3 = new Record();
    for (int i = 0; i < 10; i++) {
      int status = heapFile->GetNext(*temp3, myComparison, literal, mySchema);
      cout << status << endl;
      if(status != 0){
        temp3->Print(&mySchema);
      }
    }
    // temp3->Print(&mySchema);
    heapFile->Close();
    */
  // DBFile *heapFile = new DBFile();
  // fType f = heap;
  // heapFile->Create("test.tbl", heap, NULL);

  // const char *loadpath = "data_files/lineitem.tbl";

  // Schema mySchema("catalog", "lineitem");
  // heapFile->Load(mySchema, loadpath);
  // heapFile->Close();

  // TwoWayList<Record> *myRecs = new (std::nothrow) TwoWayList<Record>;

  TwoWayList<Test> *myRecs = new (std::nothrow) TwoWayList<Test>;
  FILE *tableFile = fopen("data_files/lineitem.tbl", "r");

  Record *temp = new Record();
  Schema mySchema("catalog", "lineitem");

  int arr[] = {6,  7,  0,    3,   1,   2,     5,   6,   9,
               -1, -2, -100, 101, 600, -1000, 200, -300};
  int length = sizeof(arr) / sizeof(*arr);
  // int length = 10;
  for (int i = 0; i < length; i++) {
    // temp->SuckNextRecord(&mySchema, tableFile);
    // temp->Print(&mySchema);
    // cout << endl;
    // myRecs->Insert(temp);
    Test *t = new Test(arr[i]);
    myRecs->Insert(t);
  }

  OrderMaker order(&mySchema);
  ComparisonEngine comp;
  auto c = [&order, &comp](void *i1, void *i2) -> bool {
    Record *i = (Record *)i1;
    Record *j = (Record *)i2;

    return comp.Compare(i, j, &order) > 0;
  };
  // auto compareLamb = [](void *i1, void *i2) -> bool {
  //   Test *i = (Test *)i1;
  //   Test *j = (Test *)i2;

  //   return *(i->num) < *(j->num);
  // };
  myRecs->Sort(compare);

  myRecs->MoveToStart();

  int count = myRecs->RightLength();
  // // Record *temp2;
  cout << "==========================  Now sorted record "
          "=========================="
       << endl;
  while (count > 0) {
    // Record *temp2 = myRecs->Current(0);
    // temp2->Print(&mySchema);
    Test *temp2 = myRecs->Current(0);
    cout << *(temp2->num);
    cout << endl;
    myRecs->Advance();
    count--;
  }
  return 0;
}
