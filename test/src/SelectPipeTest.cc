#include <string>
#include "SelectPipe.h"
#include "gtest/gtest.h"

extern "C" {
typedef struct yy_buffer_state *YY_BUFFER_STATE;
int yyparse(void);  // defined in y.tab.c
YY_BUFFER_STATE yy_scan_string(const char *str);
void yy_delete_buffer(YY_BUFFER_STATE buffer);
}

extern struct AndList *final;

namespace dbi {

// The fixture for testing class SelectPipe.
class SelectPipeTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

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

  string cnf_string = "(l_orderkey < 25) AND (l_orderkey > 40)";
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

  op->Run(*in, *out, cnf, literal);
}

}  // namespace dbi
