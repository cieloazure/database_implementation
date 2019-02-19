#include <pthread.h>
#include <unistd.h>
#include <fstream>
#include <random>
#include <vector>
#include "BigQ.cc"
#include "DBFile.h"
#include "Pipe.h"
#include "gtest/gtest.h"

struct ThreadData {
  Pipe *inputPipe;
  char *path;
  int result;
};

struct ThreadData threadArg;

void *producer(void *arg) {
  ThreadData *t = (ThreadData *)arg;

  Pipe *inputPipe = t->inputPipe;
  char *path = t->path;

  Record temp;
  int counter = 0;

  DBFile dbfile;
  dbfile.Open(path);
  cout << " producer: opened DBFile " << path << endl;
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

// The fixture for testing class BigQ.
class BigQTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    cout << "In setup" << endl;
    DBFile *heapFile = new DBFile();
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

  BigQTest() {
    // You can do set-up work for each test here.
  }

  ~BigQTest() override {
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

  int shuffle_file() {
    // initialize random number generator
    std::random_device rd;
    std::mt19937 g(rd());

    // open input file
    string file_name{"data_files/lineitem.tbl"};
    std::ifstream in_file{file_name};
    if (!in_file) {
      std::cerr << "Error: Failed to open file \"" << file_name << "\"\n";
      return -1;
    }

    vector<string> words;
    // if you want to avoid too many reallocations:
    const int expected = 100000000;
    words.reserve(expected);

    string word;
    while (!in_file.eof()) {
      std::getline(in_file, word);
      words.push_back(word);
    }
    words.pop_back();

    std::cout << "Number of elements read: " << words.size() << '\n';
    std::cout << "Beginning shuffle..." << std::endl;

    std::shuffle(words.begin(), words.end(), g);

    std::cout << "Shuffle done." << std::endl;

    // do whatever you need to do with the shuffled vector...
    std::ofstream out_file{"shuffled.tbl"};
    for (auto it = words.begin(); it != words.end(); ++it) {
      out_file << (*it);
      out_file << "\n";
    }
    out_file.close();
    return 0;
  }
};

TEST_F(BigQTest, OUTPUT_PIPE_HAS_SORTED_RECORDS) {
  Pipe *input = new Pipe(100);
  Pipe *output = new Pipe(100);

  threadArg.inputPipe = input;
  threadArg.path = (char *)"gtest.bin";

  pthread_t thread1;
  pthread_create(&thread1, NULL, producer, (void *)&threadArg);

  Schema mySchema("catalog", "lineitem");
  OrderMaker sortOrder(&mySchema);
  int runlen = 3;
  BigQ bq(*input, *output, sortOrder, runlen);

  ComparisonEngine ceng;

  int err = 0;
  int i = 0;

  Record rec[2];
  Record *last = NULL, *prev = NULL;

  while (output->Remove(&rec[i % 2])) {
    prev = last;
    last = &rec[i % 2];

    if (prev && last) {
      if (ceng.Compare(prev, last, &sortOrder) == 1) {
        err++;
      }
    }
    i++;
  }

  cout << " consumer: removed " << i << " recs from the pipe\n";

  EXPECT_FALSE(err);
  void *count;
  pthread_join(thread1, NULL);
  EXPECT_EQ(i, threadArg.result);
}

TEST_F(BigQTest, WHEN_THE_INPUT_PIPE_HAS_NO_RECORDS) {}
TEST_F(BigQTest, WHEN_THE_RUNLENGTH_IS_VALID) {}
TEST_F(BigQTest, WHEN_THE_RUNLENGTH_IS_TOO_BIG) {}
TEST_F(BigQTest, WHEN_THE_RUNLENGTH_IS_TOO_SMALL) {}
TEST_F(BigQTest, WHEN_THE_RUNS_CREATED_IS_LESS_THAN_NUMBER_OF_PAGES_IN_MEMORY) {
}
TEST_F(BigQTest, WHEN_THE_RUNS_CREATED_IS_MORE_THAN_NUMBER_OF_PAGES_IN_MEMORY) {
}
TEST_F(BigQTest, WHEN_INPUT_PIPE_IS_NULL) {}
TEST_F(BigQTest, WHEN_OUTPUT_PIPE_IS_NULL) {}
TEST_F(BigQTest, WHEN_THE_RUNLENGTH_IS_INVALID) {}
TEST_F(BigQTest, WHEN_OUTPUT_PIPE_IS_SHUTDOWN_BEFORE_SORTING_IN_OTHER_THREAD) {}
TEST_F(BigQTest, SIMULTANEOUS_PRODUCER_CONSUMER) {}

}  // namespace dbi
