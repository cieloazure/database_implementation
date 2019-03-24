#include <string>
#include "DBFile.h"
#include "HeapDBFile.h"
#include "Sum.h"
#include "gtest/gtest.h"

extern "C" {
int yyfuncparse(void);  // defined in y.tab.c
void init_lexical_parser_func(
    char *);                       // defined in lex.yyfunc.c (from Lexerfunc.l)
void close_lexical_parser_func();  // defined in lex.yyfunc.c
}

extern struct FuncOperator *finalfunc;

struct ThreadData {
  Pipe *inputPipe;
  char *path;
  int result;
};

struct ThreadData sumThreadArg;

void *sum_producer(void *arg) {
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

// The fixture for testing class Sum.
class SumTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    cout << "In setup" << endl;
    HeapDBFile *heapFile = new HeapDBFile();
    fType t = heap;
    heapFile->Create("gtest.bin", t, NULL);
    Schema mySchema("catalog", "supplier");
    heapFile->Load(mySchema, "data_files/supplier.tbl");
    heapFile->Close();
  }

  static void TearDownTestSuite() {
    remove("gtest.bin");
    remove("gtest.header");
  }

 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  SumTest() {
    // You can do set-up work for each test here.
  }

  ~SumTest() override {
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

TEST_F(SumTest, TEST_WHETHER_THREAD_IS_INVOKED) {
  Sum op;
  Pipe *in = new Pipe(100);
  Pipe *out = new Pipe(100);

  char *f_string = "(s_acctbal + (s_acctbal * 1.05))";
  Schema mySchema("catalog", "supplier");

  init_lexical_parser_func(f_string);
  yyfuncparse();
  close_lexical_parser_func();

  // grow the CNF expression from the parse tree
  Function f;
  f.GrowFromParseTree(finalfunc, mySchema);

  sumThreadArg.inputPipe = in;
  sumThreadArg.path = (char *)"gtest.bin";

  pthread_t thread1;
  pthread_create(&thread1, NULL, sum_producer, (void *)&sumThreadArg);

  op.Run(*in, *out, f);

  Record rec;
  Attribute sum_attr[1];
  sum_attr[0].name = "sum";
  sum_attr[0].myType = f.GetReturnsInt() ? Int : Double;
  Schema sum_schema("sum", 1, sum_attr);
  while (out->Remove(&rec)) {
    rec.Print(&sum_schema);
  }

  delete out;
  delete in;
}

}  // namespace dbi
