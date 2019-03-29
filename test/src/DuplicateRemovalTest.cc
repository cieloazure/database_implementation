#include <string>
#include "DBFile.h"
#include "DuplicateRemoval.h"
#include "HeapDBFile.h"
#include "Project.h"
#include "gtest/gtest.h"

struct ThreadData {
  Pipe *inputPipe;
  char *path;
  int result;
};

struct ThreadData duplicateRemovalThreadArg;

void *duplicate_removal_producer(void *arg) {
  ThreadData *t = (ThreadData *)arg;

  Pipe *inputPipe = t->inputPipe;
  char *path = t->path;

  Record temp;
  int counter = 0;

  HeapDBFile dbfile;
  dbfile.Open(path);
  cout << " producer: opened HeapDBFile " << path << endl;
  dbfile.MoveFirst();

  while (dbfile.GetNext(temp) == 1 && counter < 20) {
    counter += 1;
    if (counter % 100000 == 0) {
      cerr << " producer: " << counter << endl;
    }
    Record *copy = new Record();
    copy->Copy(&temp);
    inputPipe->Insert(copy);
  }

  dbfile.Close();
  inputPipe->ShutDown();

  cout << " producer: inserted " << counter << " recs into the pipe\n";
  t->result = counter;
  pthread_exit(NULL);
}

namespace dbi {

// The fixture for testing class DuplicateRemoval.
class DuplicateRemovalTest : public ::testing::Test {
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

  DuplicateRemovalTest() {
    // You can do set-up work for each test here.
  }

  ~DuplicateRemovalTest() override {
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

TEST_F(DuplicateRemovalTest, TEST_WHETHER_THREAD_IS_INVOKED) {
  DuplicateRemoval op;
  Project projop;
  Pipe *in = new Pipe(100);
  Pipe *out = new Pipe(100);

  duplicateRemovalThreadArg.inputPipe = in;
  duplicateRemovalThreadArg.path = (char *)"gtest.bin";

  pthread_t thread1;
  pthread_create(&thread1, NULL, duplicate_removal_producer,
                 (void *)&duplicateRemovalThreadArg);

  int keepMeSize = 1;
  int keepMeArr[1] = {0};
  int *keepMe = keepMeArr;
  Schema mySchema("catalog", "lineitem");
  Pipe *projOut = new Pipe(100);
  projop.Run(*in, *projOut, keepMe, mySchema.GetNumAtts(), keepMeSize);
  Schema test_schema("catalog", "lineitem_duplicate_removal_test");
  op.Run(*projOut, *out, test_schema);

  Record rec;
  int count = 0;
  while (out->Remove(&rec)) {
    rec.Print(&test_schema);
    count++;
  }

  EXPECT_EQ(7, count);

  pthread_join(thread1, NULL);
  projop.WaitUntilDone();
  op.WaitUntilDone();

  delete in;
  delete out;
}

}  // namespace dbi
