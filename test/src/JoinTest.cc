#include <string>
#include "HeapDBFile.h"
#include "Join.h"
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

struct ThreadData jointhreadArg;

void *join_producer(void *arg) {
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

// The fixture for testing class Join.
class JoinTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    cout << "In setup" << endl;
    fType t = heap;

    HeapDBFile *heapFile = new HeapDBFile();
    heapFile->Create("gtest1.bin", t, NULL);
    Schema mySchema("catalog", "supplier");
    heapFile->Load(mySchema, "data_files/supplier.tbl");
    heapFile->Close();

    HeapDBFile *heapFile2 = new HeapDBFile();
    heapFile2->Create("gtest2.bin", t, NULL);
    Schema mySchema2("catalog", "partsupp");
    heapFile2->Load(mySchema, "data_files/partsupp.tbl");
    heapFile2->Close();
  }

  static void TearDownTestSuite() {
    remove("gtest1.bin");
    remove("gtest1.header");

    remove("gtest2.bin");
    remove("gtest2.header");
  }

 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  JoinTest() {
    // You can do set-up work for each test here.
  }

  ~JoinTest() override {
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

TEST_F(JoinTest, TEST_WHETHER_THREAD_IS_INVOKED) {
  Join *op = new Join();
  Pipe *out = new Pipe(100);

  string cnf_string = "(s_suppkey = ps_suppkey)";
  Schema suppSchema("catalog", "supplier");
  Schema partsSuppSchema("catalog", "partsupp");

  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string.c_str());
  yyparse();
  yy_delete_buffer(buffer);

  // grow the CNF expression from the parse tree
  CNF cnf;
  Record literal;
  cnf.GrowFromParseTree(final, &suppSchema, &partsSuppSchema, literal);

  // print out the comparison to the screen
  cnf.Print();

  //   Pipe *in1 = new Pipe(100);
  //   jointhreadArg.inputPipe = in1;
  //   jointhreadArg.path = (char *)"gtest1.bin";
  //   pthread_t thread1;
  //   pthread_create(&thread1, NULL, join_producer, (void *)&jointhreadArg);

  //   Pipe *in2 = new Pipe(100);
  //   jointhreadArg.inputPipe = in1;
  //   jointhreadArg.path = (char *)"gtest2.bin";
  //   pthread_t thread2;
  //   pthread_create(&thread2, NULL, join_producer, (void *)&jointhreadArg);

  //   op->Run(*in1, *in2, *out, )

  //   Record rec;
  //   Schema test_schema("catalog", "lineitem_project_test");
  //   while (out->Remove(&rec)) {
  //     rec.Print(&test_schema);
  //   }

  //   pthread_join(thread1, NULL);
  //   pthread_join(thread2, NULL);
  //   delete in1;
  //   delete in2;
  //   delete out;
}

}  // namespace dbi
