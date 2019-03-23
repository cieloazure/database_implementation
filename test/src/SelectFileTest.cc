#include <string>
#include "DBFile.h"
#include "HeapDBFile.h"
#include "SelectFile.h"
#include "gtest/gtest.h"

extern "C" {
typedef struct yy_buffer_state *YY_BUFFER_STATE;
int yyparse(void);  // defined in y.tab.c
YY_BUFFER_STATE yy_scan_string(const char *str);
void yy_delete_buffer(YY_BUFFER_STATE buffer);
}

extern struct AndList *final;

namespace dbi {

// The fixture for testing class SelectFile.
class SelectFileTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    cout << "In setup" << endl;
    HeapDBFile *heapFile = new HeapDBFile();
    fType t = heap;
    heapFile->Create("gtest.bin", t, NULL);
    Schema mySchema("catalog", "lineitem");
    heapFile->Load(mySchema, "data_files/lineitem.tbl");
    heapFile->Close();
  }

  static void TearDownTestSuite() {
    remove("gtest.bin");
    remove("gtest.header");
  }

 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  SelectFileTest() {
    // You can do set-up work for each test here.
  }

  ~SelectFileTest() override {
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

TEST_F(SelectFileTest, TEST_WHETHER_THREAD_IS_INVOKED) {
  SelectFile op;
  Pipe *out = new Pipe(100);

  string cnf_string = "(l_orderkey > 25) AND (l_orderkey < 40)";
  Schema mySchema("catalog", "lineitem");

  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string.c_str());
  yyparse();
  yy_delete_buffer(buffer);

  // grow the CNF expression from the parse tree
  CNF cnf;
  Record literal;
  cnf.GrowFromParseTree(final, &mySchema, literal);

  // print out the comparison to the screen
  cnf.Print();

  DBFile *dbFile = new DBFile();
  dbFile->Open("gtest.bin");

  op.Run(*dbFile, *out, cnf, literal);

  Record rec;
  ComparisonEngine comp;
  while (out->Remove(&rec)) {
    EXPECT_TRUE(comp.Compare(&rec, &literal, &cnf));
  }

  delete out;
}

}  // namespace dbi
