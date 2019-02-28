#include <pthread.h>
#include <algorithm>
#include <unistd.h>
#include <fstream>
#include <random>
#include <vector>
#include "BigQ.cc"
#include "HeapDBFile.h"
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

void *producerNoRecords(void *arg) {
  ThreadData *t = (ThreadData *)arg;

  Pipe *inputPipe = t->inputPipe;

  int counter = 0;

  inputPipe->ShutDown();

  cout << " producer: inserted " << counter << " recs into the pipe\n";

  t->result = counter;
  pthread_exit(NULL);
}

namespace dbi {

// The fixture for testing class BigQ.
class BigQTest : public ::testing::Test {
 public:
  static int shuffle_file() {
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
  static void SetUpTestSuite() {
    cout << "In setup" << endl;
    HeapDBFile *heapFile = new HeapDBFile();
    fType t = heap;
    heapFile->Create("gtest.bin", t, NULL);
    Schema mySchema("catalog", "lineitem");
    heapFile->Load(mySchema, "data_files/lineitem.tbl");
    heapFile->Close();

    shuffle_file();
    HeapDBFile *heapFileShuffled = new HeapDBFile();
    heapFileShuffled->Create("gtest_shuffled.bin", t, NULL);
    heapFileShuffled->Load(mySchema, "shuffled.tbl");
    heapFileShuffled->Close();
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

  void testRunLength(int runlen, void *(*prod)(void *), bool shuffled = false) {
    Pipe *input = new Pipe(100);
    Pipe *output = new Pipe(100);

    threadArg.inputPipe = input;
    if (shuffled) {
      threadArg.path = (char *)"gtest_shuffled.bin";
    } else {
      threadArg.path = (char *)"gtest.bin";
    }

    pthread_t thread1;
    pthread_create(&thread1, NULL, prod, (void *)&threadArg);

    Schema mySchema("catalog", "lineitem");
    OrderMaker sortOrder(&mySchema);
    // Expected number of pages is 100
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
  cout << " consumer: " << (i - err) << " recs out of " << i
       << " recs in sorted order \n";

    EXPECT_FALSE(err);
    pthread_join(thread1, NULL);
    EXPECT_EQ(i, threadArg.result);
    delete input;
    delete output;
  }
};

TEST_F(BigQTest, OUTPUT_PIPE_HAS_SORTED_RECORDS_WITH_AN_ALREADY_SORTED_FILE) {
  testRunLength(3, producer);
}

TEST_F(BigQTest, OUTPUT_PIPE_HAS_SORTED_RECORDS_WITH_A_SHUFFLED_FILE) {
  testRunLength(3, producer, true);
}

TEST_F(BigQTest, WHEN_THE_INPUT_PIPE_HAS_NO_RECORDS) {
  testRunLength(3, producerNoRecords);
}

TEST_F(BigQTest,
       WHEN_THE_RUNLENGTH_IS_GREATER_THAN_NUMBER_OF_PAGES_IN_THE_FILE) {
  testRunLength(200, producer);
}

TEST_F(BigQTest, WHEN_THE_RUNLENGTH_IS_EQUAL_TO_NUMBER_OF_PAGES_IN_THE_FILE) {
  testRunLength(100, producer);
}

TEST_F(BigQTest,
       WHEN_THE_RUNLENGTH_IS_LESS_THAN_TO_NUMBER_OF_PAGES_IN_THE_FILE) {
  testRunLength(25, producer);
}

TEST_F(BigQTest, WHEN_THE_RUNLENGTH_IS_1) { testRunLength(1, producer); }

TEST_F(BigQTest, WHEN_ARGUMENTS_TO_BIGQ_ARE_INVALID) {
  Pipe *input = NULL;
  Pipe *output = new Pipe(100);
  int runlen = 1;
  Schema mySchema("catalog", "lineitem");
  OrderMaker sortOrder(&mySchema);
  BigQ bigq(*input, *output, sortOrder, runlen);
}

TEST_F(BigQTest, WHEN_OUTPUT_PIPE_IS_SHUTDOWN_BEFORE_SORTING_IN_OTHER_THREAD) {
  Pipe *input = new Pipe(100);
  Pipe *output = new Pipe(100);

  threadArg.inputPipe = input;
  threadArg.path = (char *)"gtest.bin";

  pthread_t thread1;
  pthread_create(&thread1, NULL, producer, (void *)&threadArg);

  Schema mySchema("catalog", "lineitem");
  OrderMaker sortOrder(&mySchema);
  int runlen = 3;
  // Expected number of pages is 100
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

    if (i % 10000 == 0) {
      output->ShutDown();
    }
    i++;
  }

  cout << " consumer: removed " << i << " recs from the pipe\n";
  cout << " consumer: " << (i - err) << " recs out of " << i
       << " recs in sorted order \n";

  EXPECT_FALSE(err);
  pthread_join(thread1, NULL);
  EXPECT_NE(i, threadArg.result);
}

}  // namespace dbi
