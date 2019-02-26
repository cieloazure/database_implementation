#include "HeapDBFile.h"
#include "Record.h"
#include "TwoWayList.cc"
#include "gtest/gtest.h"

namespace dbi {

// The fixture for testing class TwoWayList.
class TwoWayListTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
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

  TwoWayListTest() {
    // You can do set-up work for each test here.
  }

  ~TwoWayListTest() override {
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

  class Test {
   public:
    int *num;
    Test() { num = new int; }
    Test(int a) {
      num = new int;
      (*num) = a;
    }

    // This method is required as TwoWayList Insert is coupled with this method
    // from Record
    // Hence even for a built in type a encapsulation is required
    void Consume(Test *fromme) { num = fromme->num; }
  };

  static bool compareAsc(void *i1, void *i2) {
    Test *i = (Test *)i1;
    Test *j = (Test *)i2;

    return *(i->num) < *(j->num);
  }

  static bool compareDsc(void *i1, void *i2) {
    Test *i = (Test *)i1;
    Test *j = (Test *)i2;

    return *(i->num) > *(j->num);
  }

  // Objects declared here can be used by all tests in the test case for Foo.
};

TEST_F(TwoWayListTest, SORT_INTEGERS_ASCENDING) {
  TwoWayList<Test> *myRecs = new (std::nothrow) TwoWayList<Test>;
  int arr[] = {6, 7, 0, 3, 1, 2, 5, 9};
  int length = sizeof(arr) / sizeof(*arr);

  for (int i = 0; i < length; i++) {
    Test *t = new Test(arr[i]);
    myRecs->Insert(t);
  }

  myRecs->Sort(dbi::TwoWayListTest::compareAsc);

  int expected_arr[] = {0, 1, 2, 3, 5, 6, 7, 9};
  myRecs->MoveToStart();
  int count = myRecs->RightLength();
  int i = 0;
  while (count > 0) {
    Test *temp2 = myRecs->Current(0);
    EXPECT_EQ(expected_arr[i++], *(temp2->num));
    myRecs->Advance();
    count--;
  }
}

TEST_F(TwoWayListTest, SORT_EMPTY_LIST) {
  TwoWayList<Test> *myRecs = new (std::nothrow) TwoWayList<Test>;
  EXPECT_NO_THROW(myRecs->Sort(dbi::TwoWayListTest::compareAsc));
}

TEST_F(TwoWayListTest, SORT_DESCENDING) {
  TwoWayList<Test> *myRecs = new (std::nothrow) TwoWayList<Test>;
  int arr[] = {6, 7, 0, 3, 1, 2, 5, 9};
  int length = sizeof(arr) / sizeof(*arr);

  for (int i = 0; i < length; i++) {
    Test *t = new Test(arr[i]);
    myRecs->Insert(t);
  }

  myRecs->Sort(dbi::TwoWayListTest::compareDsc);

  int expected_arr[] = {9, 7, 6, 5, 3, 2, 1, 0};
  myRecs->MoveToStart();
  int count = myRecs->RightLength();
  int i = 0;
  while (count > 0) {
    Test *temp2 = myRecs->Current(0);
    EXPECT_EQ(expected_arr[i++], *(temp2->num));
    myRecs->Advance();
    count--;
  }
}

TEST_F(TwoWayListTest, SORT_WITH_LAMBDA) {
  TwoWayList<Test> *myRecs = new (std::nothrow) TwoWayList<Test>;
  int arr[] = {6, 7, 0, 3, 1, 2, 5, 9};
  int length = sizeof(arr) / sizeof(*arr);

  for (int i = 0; i < length; i++) {
    Test *t = new Test(arr[i]);
    myRecs->Insert(t);
  }

  auto c = [](void *i1, void *i2) -> bool {
    Test *i = (Test *)i1;
    Test *j = (Test *)i2;

    return *(i->num) > *(j->num);
  };

  myRecs->Sort(c);

  int expected_arr[] = {9, 7, 6, 5, 3, 2, 1, 0};
  myRecs->MoveToStart();
  int count = myRecs->RightLength();
  int i = 0;
  while (count > 0) {
    Test *temp2 = myRecs->Current(0);
    EXPECT_EQ(expected_arr[i++], *(temp2->num));
    myRecs->Advance();
    count--;
  }
}

TEST_F(TwoWayListTest, SORT_RECORD_USING_A_LAMBDA_ASC) {
  TwoWayList<Record> *myRecs = new (std::nothrow) TwoWayList<Record>;
  FILE *tableFile = fopen("data_files/lineitem.tbl", "r");

  Record *temp = new Record();
  Schema mySchema("catalog", "lineitem");

  int length = 10;
  for (int i = 0; i < length; i++) {
    temp->SuckNextRecord(&mySchema, tableFile);
    myRecs->Insert(temp);
  }

  OrderMaker order(&mySchema);
  ComparisonEngine comp;
  auto c = [&order, &comp](void *i1, void *i2) -> bool {
    Record *i = (Record *)i1;
    Record *j = (Record *)i2;

    return comp.Compare(i, j, &order) <= 0;
  };

  myRecs->Sort(c);

  myRecs->MoveToStart();
  int count = myRecs->RightLength();
  Record *prev = myRecs->Current(0);
  myRecs->Advance();
  count--;
  while (count > 0) {
    Record *curr = myRecs->Current(0);
    EXPECT_TRUE(comp.Compare(prev, curr, &order) <= 0);
    prev = curr;
    myRecs->Advance();
    count--;
  }
}

TEST_F(TwoWayListTest, SORT_RECORD_USING_A_LAMBDA_DSC) {
  TwoWayList<Record> *myRecs = new (std::nothrow) TwoWayList<Record>;
  // FILE *tableFile = fopen("data_files/lineitem.tbl", "r");
  HeapDBFile dbFile;
  dbFile.Open("gtest.bin");

  // Record *temp = new Record();
  Record temp;
  Schema mySchema("catalog", "lineitem");

  int length = 10;
  for (int i = 0; i < length; i++) {
    dbFile.GetNext(temp);
    myRecs->Insert(&temp);
    // myRecs->Insert(temp);
  }

  OrderMaker order(&mySchema);
  ComparisonEngine comp;
  auto c = [&order, &comp](void *i1, void *i2) -> bool {
    Record *i = (Record *)i1;
    Record *j = (Record *)i2;

    return comp.Compare(i, j, &order) > 0;
  };

  myRecs->Sort(c);

  myRecs->MoveToStart();
  int count = myRecs->RightLength();
  Record *prev = myRecs->Current(0);
  myRecs->Advance();
  count--;
  while (count > 0) {
    Record *curr = myRecs->Current(0);
    EXPECT_TRUE(comp.Compare(prev, curr, &order) > 0);
    prev = curr;
    myRecs->Advance();
    count--;
  }
}

}  // namespace dbi
