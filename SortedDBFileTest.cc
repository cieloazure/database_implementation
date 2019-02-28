#include "DBFile.h"
#include "File.h"
#include "SortedDBFile.h"
#include "gtest/gtest.h"

extern "C" {
typedef struct yy_buffer_state *YY_BUFFER_STATE;
int yyparse(void);  // defined in y.tab.c
YY_BUFFER_STATE yy_scan_string(const char *str);
void yy_delete_buffer(YY_BUFFER_STATE buffer);
}

struct SortInfo {
  OrderMaker *sortOrder;
  int runLength;

  SortInfo(OrderMaker *so, int rl) {
    sortOrder = so;
    runLength = rl;
  }
};

struct TempTupleForSearch {
  OrderMaker queryOrderMaker;
  Record *literal;

  TempTupleForSearch(OrderMaker qom, Record *lit) {
    queryOrderMaker = qom;
    literal = new Record;
    literal->Copy(lit);
  }
};

Schema mySchema("catalog", "lineitem");

extern struct AndList *final;

namespace dbi {

// The fixture for testing class SortedDBFile.
class SortedDBFileTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  SortedDBFileTest() {
    // You can do set-up work for each test here.
  }

  ~SortedDBFileTest() override {
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
    remove("gtest.bin");
    remove("gtest.header");
  }

  // Objects declared here can be used by all tests in the test case for Foo.
  OrderMaker *o = new OrderMaker(&mySchema);
  SortInfo *si = new SortInfo(o, 3);

  TempTupleForSearch *buildQueryOrderMaker(const char cnf_string[]) {
    Schema mySchema("catalog", "lineitem");

    YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
    yyparse();
    yy_delete_buffer(buffer);

    // grow the CNF expression from the parse tree
    CNF cnf;
    Record *literal = new Record();
    cnf.GrowFromParseTree(final, &mySchema, *literal);

    // print out the comparison to the screen
    cnf.Print();

    OrderMaker fileSortOrder(&mySchema);
    fileSortOrder.Print();

    OrderMaker querySortOrder;

    cnf.BuildQueryOrderMaker(fileSortOrder, querySortOrder);

    querySortOrder.Print();

    TempTupleForSearch *t = new TempTupleForSearch(querySortOrder, literal);
    return t;
  }
};

TEST_F(SortedDBFileTest, CREATE_SUCCESS) {
  DBFile *sortFile = new DBFile();
  fType t = sorted;
  EXPECT_TRUE(sortFile->Create("gtest.bin", t, (void *)si));
  delete sortFile;
}

TEST_F(SortedDBFileTest, CREATE_FAILURE_WHEN_FILE_NAME_IS_TOO_LARGE) {
  string a = "a";
  int MAX_FILE_NAME_SIZE = 255;
  string s;
  for (int i = 0; i < MAX_FILE_NAME_SIZE; i++) {
    s.append(a);
  }
  s += ".bin";

  DBFile *sortFile = new DBFile();
  fType t = sorted;
  EXPECT_FALSE(sortFile->Create(s.c_str(), t, (void *)si));
  delete sortFile;
}

TEST_F(SortedDBFileTest, CREATE_FAILURE_WHEN_FILENAME_IS_AN_EMPTY_STRING) {
  DBFile *sortFile = new DBFile();
  fType t = sorted;
  EXPECT_FALSE(sortFile->Create("", t, (void *)si));
  delete sortFile;
}

TEST_F(SortedDBFileTest, CREATE_FAILURE_WHEN_FILE_TYPE_IS_INVALID) {
  DBFile *sortFile = new DBFile();
  EXPECT_FALSE(sortFile->Create("gtest.bin", (fType)4, (void *)si));
  delete sortFile;
}

TEST_F(SortedDBFileTest, OPEN_SUCCESS) {
  DBFile *sortFile = new DBFile();
  fType t = sorted;
  if (sortFile->Create("gtest.bin", t, (void *)si)) {
    sortFile->Close();
    EXPECT_TRUE(sortFile->Open("gtest.bin"));
  }
  delete sortFile;
}

TEST_F(SortedDBFileTest, OPEN_FAILURE_MISSING_METADATA_FILE) {
  DBFile *sortFile = new DBFile();
  fType t = heap;
  if (sortFile->Create("gtest.bin", t, (void *)si)) {
    sortFile->Close();
    remove("gtest.header");
    EXPECT_FALSE(sortFile->Open("gtest.bin"));
  }
  delete sortFile;
}

TEST_F(SortedDBFileTest, OPEN_FAILURE_ON_INVALID_FILE_TYPE_IN_METADATA_FILE) {
  DBFile *sortFile = new DBFile();
  fType t = heap;
  if (sortFile->Create("gtest.bin", t, (void *)si)) {
    sortFile->Close();
    int fd = open("gtest.header", O_RDWR, S_IRUSR | S_IWUSR);
    lseek(fd, 0, SEEK_SET);
    int invalid_value = 4;
    write(fd, &invalid_value, sizeof(fType));
    EXPECT_FALSE(sortFile->Open("gtest.bin"));
  }
  delete sortFile;
}

TEST_F(SortedDBFileTest, OPEN_FAILURE_WHEN_A_FILE_DOES_NOT_EXISTS) {
  DBFile *sortFile = new DBFile();
  EXPECT_FALSE(sortFile->Open("gtest2.bin"));
  delete sortFile;
}

TEST_F(SortedDBFileTest, OPEN_FAILURE_WHEN_A_FILE_NAME_IS_INVALID) {
  DBFile *sortFile = new DBFile();
  fType t = heap;
  if (sortFile->Create("gtest.bin", t, (void *)si)) {
    sortFile->Close();
    EXPECT_FALSE(sortFile->Open(""));
  }
  delete sortFile;
}

TEST_F(SortedDBFileTest, BINARY_SEARCH_PAGE) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  heapFile->Create("gtest.bin", t, NULL);
  Schema mySchema("catalog", "lineitem");
  const char *loadpath = "data_files/lineitem.tbl";
  heapFile->Load(mySchema, loadpath);
  heapFile->Close();

  File *f = new File();
  f->Open(1, (char *)"gtest.bin");
  Page *buffer = new Page();
  f->GetPage(buffer, 49);
  cout << buffer->GetNumRecords() << endl;

  SortedDBFile *sortedDBFile = new SortedDBFile();
  fType t1 = sorted;
  sortedDBFile->Create("gtest_sorted.bin", t1, (void *)si);

  const char cnf_string[] = "(l_orderkey = 69)";
  TempTupleForSearch *ttfs = buildQueryOrderMaker(cnf_string);
  sortedDBFile->BinarySearchPage(buffer, &ttfs->queryOrderMaker, ttfs->literal);
  delete sortedDBFile;
}

TEST_F(SortedDBFileTest, BINARY_SEARCH_FILE) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  heapFile->Create("gtest.bin", t, NULL);
  Schema mySchema("catalog", "lineitem");
  const char *loadpath = "data_files/lineitem.tbl";
  heapFile->Load(mySchema, loadpath);
  heapFile->Close();

  File *file = new File();
  file->Open(1, (char *)"gtest.bin");

  SortedDBFile *sortedDBFile = new SortedDBFile();
  fType t1 = sorted;
  sortedDBFile->Create("gtest_sorted.bin", t1, (void *)si);

  const char cnf_string[] = "(l_orderkey = 14630)";
  TempTupleForSearch *ttfs = buildQueryOrderMaker(cnf_string);
  sortedDBFile->BinarySearchFile(file, &ttfs->queryOrderMaker, ttfs->literal,
                                 0);
  delete sortedDBFile;
}

}  // namespace dbi