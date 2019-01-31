#include "DBFile.h"
#include "gtest/gtest.h"

namespace {

// The fixture for testing class Foo.
class DBFileTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  DBFileTest() {
    // You can do set-up work for each test here.
  }

  ~DBFileTest() override {
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

TEST(DBFileTest, CreateSuccess) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  ASSERT_TRUE(heapFile->Create("gtest.tbl", t, NULL));
}

TEST(DBFileTest, CreateFailureWhenFileNameIsTooLarge) {
  string a = "a";
  int MAX_FILE_NAME_SIZE = 255;
  string s;
  for (int i = 0; i < 255; i++) {
    s.append(a);
  }
  s += ".tbl";

  DBFile *heapFile = new DBFile();
  fType t = heap;
  ASSERT_FALSE(heapFile->Create(s.c_str(), t, NULL));
}

TEST(DBFileTest, OpenSuccess) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.tbl", t, NULL)) {
    heapFile->Close();
    ASSERT_TRUE(heapFile->Open("gtest.tbl"));
  }
}

TEST(DBFileTest, OpenFailureMissingMetadataFile) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.tbl", t, NULL)) {
    heapFile->Close();
    remove("gtest.header");
    ASSERT_FALSE(heapFile->Open("gtest.tbl"));
  }
}

TEST(DBFileTest, OpenFailureOnInvalidFileTypeInMetadataFile) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.tbl", t, NULL)) {
    heapFile->Close();
    int fd = open("gtest.header", O_RDWR, S_IRUSR | S_IWUSR);
    lseek(fd, 0, SEEK_SET);
    int invalid_value = 4;
    write(fd, &invalid_value, sizeof(fType));
    ASSERT_FALSE(heapFile->Open("gtest.tbl"));
  }
}

TEST(DBFileTest, MoveFirst) {}
}  // namespace