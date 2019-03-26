#include <string>
#include "BigQ.h"
#include "HeapDBFile.h"
#include "Join.h"
#include "gtest/gtest.h"
#include "pthread.h"

extern "C" {
typedef struct yy_buffer_state *YY_BUFFER_STATE;
int yyparse(void);  // defined in y.tab.c
YY_BUFFER_STATE yy_scan_string(const char *str);
void yy_delete_buffer(YY_BUFFER_STATE buffer);
}

extern struct AndList *final;

extern "C" {
struct ThreadData {
  Pipe *inputPipe;
  char *path;
  int result;
};

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

  while (dbfile.GetNext(temp) == 1) {
    counter += 1;
    if (counter % 100000 == 0) {
      cerr << " producer: " << counter << endl;
    }
    Record *copy = new Record();
    copy->Copy(&temp);
    inputPipe->Insert(copy);
  }
  cout << endl;

  dbfile.Close();
  inputPipe->ShutDown();

  cout << " producer: inserted " << counter << " recs into the pipe " << path
       << "\n";
  t->result = counter;
  pthread_exit(NULL);
}
}
// struct ThreadData2 {
//   Pipe *outputPipe;
//   Schema *schema;
//   int result;
//   pthread_t *thread1;
// };

// void *join_consumer(void *threadargs) {
//   ThreadData2 *t = (ThreadData2 *)threadargs;

//   Pipe *outputPipe = t->outputPipe;
//   Schema *schema = t->schema;
//   pthread_t *thread = t->thread1;

//   Record outRec;
//   int counter = 0;
//   while (outputPipe->Remove(&outRec)) {
//     counter++;
//     outRec.Print(schema);
//   }

//   cout << "Removed " << counter << " records from pipe for " << endl;
//   pthread_join(*thread, NULL);
//   pthread_exit(NULL);
// }

namespace dbi {

// The fixture for testing class Join.
class JoinTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    cout << "In setup" << endl;
    fType t = heap;

    HeapDBFile *heapFile = new HeapDBFile();
    heapFile->Create("gtest1.bin", t, NULL);
    Schema mySchema("catalog", "orders");
    heapFile->Load(mySchema, "data_files/orders.tbl");
    heapFile->Close();

    HeapDBFile *heapFile2 = new HeapDBFile();
    heapFile2->Create("gtest2.bin", t, NULL);
    Schema mySchema2("catalog", "lineitem");
    heapFile2->Load(mySchema2, "data_files/lineitem.tbl");
    heapFile2->Close();

    srand(time(NULL));
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
  // string cnf_string = "(s_suppkey = ps_suppkey)";
  // Schema suppSchema("catalog", "supplier");
  // Schema partsSuppSchema("catalog", "partsupp");

  string cnf_string = "(o_orderkey = l_orderkey)";
  Schema ordersSchema("catalog", "orders");
  Schema lineItemSchema("catalog", "lineitem");

  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string.c_str());
  yyparse();
  yy_delete_buffer(buffer);

  // grow the CNF expression from the parse tree
  CNF cnf;
  Record literal;
  cnf.GrowFromParseTree(final, &ordersSchema, &lineItemSchema, literal);

  // print out the comparison to the screen
  cnf.Print();
  Pipe in1(100);
  Pipe in2(100);
  Pipe out(100);

  Join j;
  j.Run(in1, in2, out, cnf, literal);

  pthread_t thread1;
  struct ThreadData *thread_data1 =
      (struct ThreadData *)malloc(sizeof(struct ThreadData));
  thread_data1->inputPipe = &in1;
  thread_data1->path = (char *)"gtest1.bin";

  pthread_t thread2;
  struct ThreadData *thread_data2 =
      (struct ThreadData *)malloc(sizeof(struct ThreadData));
  thread_data2->inputPipe = &in2;
  thread_data2->path = (char *)"gtest2.bin";

  pthread_create(&thread1, NULL, join_producer, (void *)thread_data1);
  pthread_create(&thread2, NULL, join_producer, (void *)thread_data2);

  Record outRec;
  int counter = 0;
  while (out.Remove(&outRec)) {
    counter++;
  }
  cout << "Removed " << counter << " records from pipe" << endl;

  pthread_join(thread1, NULL);
  pthread_join(thread2, NULL);
  j.WaitUntilDone();
}

}  // namespace dbi
