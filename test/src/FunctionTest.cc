#include <iostream>
#include "DBFile.h"
#include "Function.h"
#include "HeapDBFile.h"
#include "Record.h"
#include "gtest/gtest.h"

extern "C" {
int yyfuncparse(void);  // defined in yyfunc.tab.c
void init_lexical_parser_func(
    char *);                       // defined in lex.yyfunc.c (from Lexerfunc.l)
void close_lexical_parser_func();  // defined in lex.yyfunc.c
}

extern struct FuncOperator *finalfunc;

namespace dbi {

// The fixture for testing class Function.
class FunctionTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    cout << "In setup" << endl;
    HeapDBFile *heapFile = new HeapDBFile();
    fType t = heap;
    heapFile->Create("gtest.bin", t, NULL);
    Schema mySchema("catalog", "lineitem");
    heapFile->Load(mySchema, "data_files/supplier.tbl");
    heapFile->Close();
  }

  static void TearDownTestSuite() {}

 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  FunctionTest() {
    // You can do set-up work for each test here.
  }

  ~FunctionTest() override {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  void SetUp() override {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  void TearDown() override {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests in the test case for Foo.
};

TEST_F(FunctionTest, CHECK_FUNCTION) {
  char *f_string = "(s_acctbal + (s_acctbal * 1.05))";
  Schema mySchema("catalog", "supplier");

  init_lexical_parser_func(f_string);
  yyfuncparse();
  close_lexical_parser_func();

  // grow the CNF expression from the parse tree
  Function f;
  f.GrowFromParseTree(finalfunc, mySchema);

  // print out the comparison to the screen
  f.Print();

  EXPECT_FALSE(f.GetReturnsInt());

  DBFile dbFile;
  dbFile.Open("gtest.bin");
  Record temp;
  dbFile.GetNext(temp);
  temp.Print(&mySchema);
  int i;
  double d;
  f.Apply(temp, i, d);
  cout << i << endl;
  cout << d << endl;
}

}  // namespace dbi
