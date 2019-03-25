#include <string>
#include "HeapDBFile.h"
#include "SelectPipe.h"
#include "gtest/gtest.h"

extern "C" {
typedef struct yy_buffer_state *YY_BUFFER_STATE;
int yyparse(void);  // defined in y.tab.c
YY_BUFFER_STATE yy_scan_string(const char *str);
void yy_delete_buffer(YY_BUFFER_STATE buffer);
}

extern struct AndList *final;

struct ThreadData {
  Pipe *inputPipe;
  char *path;
  int result;
};

struct ThreadData spthreadArg;

void *spproducer(void *arg) {
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

// The fixture for testing class SelectPipe.
class SelectPipeTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    cout << "In setup" << endl;
    HeapDBFile *heapFile = new HeapDBFile();
    fType t = heap;
    heapFile->Create("gtest.bin", t, NULL);
    Schema mySchema("catalog", "partsupp");
    heapFile->Load(mySchema, "data_files/partsupp.tbl");
    heapFile->Close();
  }

  static void TearDownTestSuite() {
    remove("gtest.bin");
    remove("gtest.header");
  }

 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  SelectPipeTest() {
    // You can do set-up work for each test here.
  }

  ~SelectPipeTest() override {
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

TEST_F(SelectPipeTest, TEST_WHETHER_THREAD_IS_INVOKED) {
  SelectPipe *op = new SelectPipe();
  Pipe *in = new Pipe(100);
  Pipe *out = new Pipe(100);

  string cnf_string = "(l_orderkey > 25) AND (l_orderkey < 40)";
  Schema mySchema("catalog", "lineitem");

  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string.c_str());
  yyparse();
  yy_delete_buffer(buffer);

  // grow the CNF expression from the parse tree
  CNF cnf;
  Record literal;
  cnf.GrowFromParseTree(final, &mySchema, literal);

  // print out the comparison to the screen
  cnf.Print();

  spthreadArg.inputPipe = in;
  spthreadArg.path = (char *)"gtest.bin";

  pthread_t thread1;
  pthread_create(&thread1, NULL, spproducer, (void *)&spthreadArg);

  op->Run(*in, *out, cnf, literal);

  Record rec;
  ComparisonEngine comp;
  int count = 0;
  while (out->Remove(&rec)) {
    count++;
    EXPECT_TRUE(comp.Compare(&rec, &literal, &cnf));
  }

  cout << "Removed " << count << " records " << endl;

  pthread_join(thread1, NULL);
  delete in;
  delete out;
}

}  // namespace dbi
