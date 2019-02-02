#include "DBFile.h"
#include "gtest/gtest.h"

namespace foo {

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
    remove("gtest.bin");
    remove("gtest.header");
  }

  // Objects declared here can be used by all tests in the test case for Foo.
};

TEST_F(DBFileTest, CreateSuccess) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  ASSERT_TRUE(heapFile->Create("gtest.bin", t, NULL));
  delete heapFile;
}

TEST_F(DBFileTest, CreateFailureWhenFileNameIsTooLarge) {
  string a = "a";
  int MAX_FILE_NAME_SIZE = 255;
  string s;
  for (int i = 0; i < MAX_FILE_NAME_SIZE; i++) {
    s.append(a);
  }
  s += ".bin";

  DBFile *heapFile = new DBFile();
  fType t = heap;
  ASSERT_FALSE(heapFile->Create(s.c_str(), t, NULL));
  delete heapFile;
}

TEST_F(DBFileTest, CreateFailureWhenFileNameIsAnEmptyString) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  ASSERT_FALSE(heapFile->Create("", t, NULL));
  delete heapFile;
}

TEST_F(DBFileTest, CreateFailureWhenFileIsInvalid) {
  DBFile *heapFile = new DBFile();
  ASSERT_FALSE(heapFile->Create("gtest.bin", (fType)4, NULL));
  delete heapFile;
}

TEST_F(DBFileTest, OpenSuccess) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, NULL)) {
    heapFile->Close();
    ASSERT_TRUE(heapFile->Open("gtest.bin"));
  }
  delete heapFile;
}

TEST_F(DBFileTest, OpenFailureMissingMetadataFile) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, NULL)) {
    heapFile->Close();
    remove("gtest.header");
    ASSERT_FALSE(heapFile->Open("gtest.bin"));
  }
  delete heapFile;
}

TEST_F(DBFileTest, OpenFailureOnInvalidFileTypeInMetadataFile) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, NULL)) {
    heapFile->Close();
    int fd = open("gtest.header", O_RDWR, S_IRUSR | S_IWUSR);
    lseek(fd, 0, SEEK_SET);
    int invalid_value = 4;
    write(fd, &invalid_value, sizeof(fType));
    ASSERT_FALSE(heapFile->Open("gtest.bin"));
  }
  delete heapFile;
}

TEST_F(DBFileTest, OpenFailureWhenAFileDoesNotExists) {
  DBFile *heapFile = new DBFile();
  ASSERT_FALSE(heapFile->Open("gtest2.bin"));
  delete heapFile;
}

TEST_F(DBFileTest, OpenFailureWhenAFileNameIsInvalid) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, NULL)) {
    heapFile->Close();
    EXPECT_FALSE(heapFile->Open(""));
  }
  delete heapFile;
}

TEST_F(DBFileTest, MoveFirstWhenGetNextHaveBeenCalledBefore) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, NULL)) {
    const char *loadpath = "data_files/lineitem.tbl";

    Schema mySchema("catalog", "lineitem");
    heapFile->Load(mySchema, loadpath);

    FILE *load_file = fopen(loadpath, "r");
    Record *temp_source_file_record = new Record();
    Record *temp_heap_file_record = new Record();
    Record *first = new Record();
    for (int i = 0; i < 3; i++) {
      temp_source_file_record->SuckNextRecord(&mySchema, load_file);
      if (i == 0) {
        first->Copy(temp_source_file_record);
      }
      heapFile->GetNext(*temp_heap_file_record);
    }

    heapFile->MoveFirst();
    heapFile->GetNext(*temp_heap_file_record);

    ComparisonEngine comp;
    OrderMaker order(&mySchema);

    temp_heap_file_record->Print(&mySchema);
    first->Print(&mySchema);
    ASSERT_TRUE(comp.Compare(temp_heap_file_record, first, &order) == 0);

    delete temp_source_file_record;
    delete temp_heap_file_record;
    delete first;
    fclose(load_file);
    heapFile->Close();
  }
}

TEST_F(DBFileTest, MoveFirstOnFirstRecord) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, NULL)) {
    const char *loadpath = "data_files/lineitem.tbl";

    Schema mySchema("catalog", "lineitem");
    heapFile->Load(mySchema, loadpath);

    FILE *load_file = fopen(loadpath, "r");
    Record *temp_source_file_record = new Record();
    Record *temp_heap_file_record = new Record();
    Record *first = new Record();
    for (int i = 0; i < 1; i++) {
      temp_source_file_record->SuckNextRecord(&mySchema, load_file);
      if (i == 0) {
        first->Copy(temp_source_file_record);
      }
      // No Get Next is being called
    }

    // Just opened a file and moving first
    heapFile->MoveFirst();
    heapFile->GetNext(*temp_heap_file_record);

    ComparisonEngine comp;
    OrderMaker order(&mySchema);

    temp_heap_file_record->Print(&mySchema);
    first->Print(&mySchema);
    ASSERT_TRUE(comp.Compare(temp_heap_file_record, first, &order) == 0);

    delete temp_source_file_record;
    delete temp_heap_file_record;
    delete first;
    fclose(load_file);
    heapFile->Close();
  }
}

TEST_F(DBFileTest, MoveFirstWithNoRecords) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, NULL)) {
    // Just opened a file and moving first
    Record *temp_heap_file_record = new Record();
    heapFile->MoveFirst();
    ASSERT_EQ(0, heapFile->GetNext(*temp_heap_file_record));
    delete temp_heap_file_record;
    heapFile->Close();
  }
}

TEST_F(DBFileTest, MoveFirstWithoutCreation) {
  DBFile *heapFile = new DBFile();
  ASSERT_THROW(heapFile->MoveFirst(), runtime_error);
}

TEST_F(DBFileTest, LoadWithNoOpenDBFile) {
  DBFile *heapFile = new DBFile();
  Schema mySchema("catalog", "lineitem");
  const char *tpch_dir = "data_files/lineitem.tbl";
  ASSERT_THROW(heapFile->Load(mySchema, tpch_dir), runtime_error);
}

TEST_F(DBFileTest, LoadWithNoExistingTableFile) {
  DBFile *heapFile = new DBFile();
  Schema mySchema("catalog", "lineitem");
  const char *tpch_dir = "data_files/does-not-exists.bin";
  ASSERT_THROW(heapFile->Load(mySchema, tpch_dir), runtime_error);
}

TEST_F(DBFileTest, LoadSuccess) {
  DBFile *heapFile = new DBFile();
  if (heapFile->Create("gtest.bin", heap, NULL) == 1) {
    Schema mySchema("catalog", "lineitem");
    const char *tpch_dir = "data_files/lineitem.tbl";
    FILE *f = fopen(tpch_dir, "r");
    Record temp;
    int expected_count = 0;
    while (temp.SuckNextRecord(&mySchema, f)) {
      expected_count++;
    }

    heapFile->Load(mySchema, tpch_dir);

    int actual_count = 0;
    while (heapFile->GetNext(temp)) {
      actual_count++;
    }

    ASSERT_EQ(expected_count, actual_count);
  }
}

TEST_F(DBFileTest, AddAfterASeriesOfGetNext) {}
TEST_F(DBFileTest, AddWhenItJustAddsToBufferWithoutCreatingANewPage) {
  DBFile *heapFile = new DBFile();
  if (heapFile->Create("gtest.bin", heap, NULL)) {
    Schema mySchema("catalog", "lineitem");
    const char *tpch_dir = "data_files/lineitem.tbl";
    FILE *f = fopen(tpch_dir, "r");
    Record temp;
    int expected_count = 0;
    temp.SuckNextRecord(&mySchema, f);
    heapFile->Add(temp);
    int actual_count = 0;
    while (heapFile->GetNext(temp)) {
      actual_count++;
    }

    ASSERT_EQ(1, actual_count);
  }
}
TEST_F(DBFileTest, AddWhenFirstFlushTakesPlace) {}
TEST_F(DBFileTest, AddWhenSubsequentFlushesTakesPlace) {}
TEST_F(DBFileTest, AddWhenANewPageIsCreated) {}
TEST_F(DBFileTest, AddWhenAnExistingPartiallyFilledPageIsUtilized) {}

}  // namespace foo