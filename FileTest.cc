#include "File.h"
#include "gtest/gtest.h"

namespace dbi {

// The fixture for testing class File.
class FileTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  FileTest() {
    // You can do set-up work for each test here.
  }

  ~FileTest() override {
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

TEST_F(FileTest, PAGE_SORTED_TEST) {
  FILE *tableFile = fopen("data_files/lineitem.tbl", "r");
  Record *temp = new Record();
  Schema mySchema("catalog", "lineitem");

  Page *page = new Page();

  temp->SuckNextRecord(&mySchema, tableFile);
  while (page->Append(temp) != 0) {
    temp->SuckNextRecord(&mySchema, tableFile);
  }

  OrderMaker order(&mySchema);
  page->Sort(order);

  Record *prev = new Record();
  page->GetFirst(prev);
  Record *curr = new Record();
  ComparisonEngine comp;
  while (page->GetFirst(curr) != 0) {
    EXPECT_TRUE(comp.Compare(prev, curr, &order) <= 0);
    prev = curr;
  }
}

}  // namespace dbi