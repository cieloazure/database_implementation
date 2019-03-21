#include <string>
#include "HeapDBFile.h"
#include "Project.h"
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

struct ThreadData projthreadArg;

void *projproducer(void *arg) {
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

// The fixture for testing class Project.
class ProjectTest : public ::testing::Test {
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

  ProjectTest() {
    // You can do set-up work for each test here.
  }

  ~ProjectTest() override {
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

TEST_F(ProjectTest, TEST_WHETHER_THREAD_IS_INVOKED) {
  Project *op = new Project();
  Pipe *in = new Pipe(100);
  Pipe *out = new Pipe(100);

  projthreadArg.inputPipe = in;
  projthreadArg.path = (char *)"gtest.bin";

  pthread_t thread1;
  pthread_create(&thread1, NULL, projproducer, (void *)&projthreadArg);

  int keepMeSize = 4;
  int keepMeArr[4] = {1, 2, 3, 4};
  int *keepMe = keepMeArr;
  Schema mySchema("catalog", "lineitem");
  op->Run(*in, *out, keepMe, mySchema.GetNumAtts(), keepMeSize);

  Record rec;
  Schema test_schema("catalog", "lineitem_project_test");
  while (out->Remove(&rec)) {
    rec.Print(&test_schema);
  }

  pthread_join(thread1, NULL);
  delete in;
  delete out;
}

}  // namespace dbi
