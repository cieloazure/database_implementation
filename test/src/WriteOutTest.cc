#include <string>
#include "HeapDBFile.h"
#include "WriteOut.h"
#include "gtest/gtest.h"

struct ThreadData {
  Pipe *inputPipe;
  char *path;
  int result;
};

struct ThreadData write_out_thread_arg;

void *write_out_producer(void *arg) {
  ThreadData *t = (ThreadData *)arg;

  Pipe *inputPipe = t->inputPipe;
  char *path = t->path;

  Record temp;
  int counter = 0;

  HeapDBFile dbfile;
  dbfile.Open(path);
  cout << " producer: opened HeapDBFile " << path << endl;
  dbfile.MoveFirst();

  while (dbfile.GetNext(temp) == 1) {
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

// The fixture for testing class WriteOut.
class WriteOutTest : public ::testing::Test {
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

  WriteOutTest() {
    // You can do set-up work for each test here.
  }

  ~WriteOutTest() override {
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

TEST_F(WriteOutTest, TEST_WHETHER_THREAD_IS_INVOKED) {
  WriteOut *op = new WriteOut();
  Pipe *in = new Pipe(100);
  FILE *outFile;
  outFile = fopen("gtest.tbl", "w");

  write_out_thread_arg.inputPipe = in;
  write_out_thread_arg.path = (char *)"gtest.bin";

  pthread_t thread1;
  pthread_create(&thread1, NULL, write_out_producer,
                 (void *)&write_out_thread_arg);

  Schema mySchema("catalog", "lineitem");
  op->Run(*in, outFile, mySchema);

  pthread_join(thread1, NULL);
  op->WaitUntilDone();
  fclose(outFile);

  delete in;

  HeapDBFile *heapFile = new HeapDBFile();
  fType t = heap;
  heapFile->Create("gtest2.bin", t, NULL);
  heapFile->Load(mySchema, "gtest.tbl");

  int actual_count = 0;
  Record temp;
  while (heapFile->GetNext(temp)) {
    actual_count++;
  }
  heapFile->Close();
  EXPECT_EQ(write_out_thread_arg.result, actual_count);
}

}  // namespace dbi
