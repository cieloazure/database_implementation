
#include <stdlib.h>
#include <iostream>
#include "Record.h"
#include "DBFile.h"
using namespace std;

extern "C" {
int yyparse(void);  // defined in y.tab.c
}

extern struct AndList *final;

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

  DBFile* heapFile = new DBFile();
  fType f = heap;
  // heapFile -> Create("test.tbl", heap, NULL);
  // heapFile -> Close();

  heapFile -> Open("test.tbl");
  heapFile -> Close();
}