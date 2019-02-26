#include "DBFile.h"
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
};

TEST_F(SortedDBFileTest, CREATE_SUCCESS) {
  DBFile *heapFile = new DBFile();
  fType t = sorted;
  EXPECT_TRUE(heapFile->Create("gtest.bin", t, (void *)si));
  delete heapFile;
}

TEST_F(SortedDBFileTest, CREATE_FAILURE_WHEN_FILE_NAME_IS_TOO_LARGE) {
  string a = "a";
  int MAX_FILE_NAME_SIZE = 255;
  string s;
  for (int i = 0; i < MAX_FILE_NAME_SIZE; i++) {
    s.append(a);
  }
  s += ".bin";

  DBFile *heapFile = new DBFile();
  fType t = sorted;
  EXPECT_FALSE(heapFile->Create(s.c_str(), t, (void *)si));
  delete heapFile;
}

TEST_F(SortedDBFileTest,
       CREATE_FAILURE_WHEN_FILENAME_IS_AN_EMPTY_STRING) {
  DBFile *heapFile = new DBFile();
  fType t = sorted;
  EXPECT_FALSE(heapFile->Create("", t, (void *)si));
  delete heapFile;
}

TEST_F(SortedDBFileTest, CREATE_FAILURE_WHEN_FILE_TYPE_IS_INVALID) {
  DBFile *heapFile = new DBFile();
  EXPECT_FALSE(heapFile->Create("gtest.bin", (fType)4, (void *)si));
  delete heapFile;
}

TEST_F(SortedDBFileTest, OPEN_SUCCESS) {
  DBFile *heapFile = new DBFile();
  fType t = sorted;
  if (heapFile->Create("gtest.bin", t, (void *)si)) {
    heapFile->Close();
    EXPECT_TRUE(heapFile->Open("gtest.bin"));
  }
  delete heapFile;
}


TEST_F(SortedDBFileTest, OPEN_FAILURE_MISSING_METADATA_FILE) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, (void *)si)) {
    heapFile->Close();
    remove("gtest.header");
    EXPECT_FALSE(heapFile->Open("gtest.bin"));
  }
  delete heapFile;
}

TEST_F(SortedDBFileTest, OPEN_FAILURE_ON_INVALID_FILE_TYPE_IN_METADATA_FILE) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, (void *)si)) {
    heapFile->Close();
    int fd = open("gtest.header", O_RDWR, S_IRUSR | S_IWUSR);
    lseek(fd, 0, SEEK_SET);
    int invalid_value = 4;
    write(fd, &invalid_value, sizeof(fType));
    EXPECT_FALSE(heapFile->Open("gtest.bin"));
  }
  delete heapFile;
}

TEST_F(SortedDBFileTest, OPEN_FAILURE_WHEN_A_FILE_DOES_NOT_EXISTS) {
  DBFile *heapFile = new DBFile();
  EXPECT_FALSE(heapFile->Open("gtest2.bin"));
  delete heapFile;
}

TEST_F(SortedDBFileTest, OPEN_FAILURE_WHEN_A_FILE_NAME_IS_INVALID) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, (void *)si)) {
    heapFile->Close();
    EXPECT_FALSE(heapFile->Open(""));
  }
  delete heapFile;
}


}  // namespace dbi