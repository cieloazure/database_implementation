#include "Comparison.h"
#include "gtest/gtest.h"

extern "C" {
typedef struct yy_buffer_state *YY_BUFFER_STATE;
int yyparse(void);  // defined in y.tab.c
YY_BUFFER_STATE yy_scan_string(const char *str);
void yy_delete_buffer(YY_BUFFER_STATE buffer);
}

extern struct AndList *final;

namespace dbi {

// The fixture for testing class Comparison.
class ComparisonTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  ComparisonTest() {
    // You can do set-up work for each test here.
  }

  ~ComparisonTest() override {
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
  void buildQueryOrderMaker(const char cnf_string[]) {
    Schema mySchema("catalog", "lineitem");

    YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
    yyparse();
    yy_delete_buffer(buffer);

    // grow the CNF expression from the parse tree
    CNF cnf;
    Record literal;
    cnf.GrowFromParseTree(final, &mySchema, literal);

    // print out the comparison to the screen
    cnf.Print();

    OrderMaker fileSortOrder(&mySchema);
    fileSortOrder.Print();

    OrderMaker querySortOrder;

    cnf.GetSortOrderAttributes(fileSortOrder, querySortOrder);

    querySortOrder.Print();
  }
};

TEST_F(ComparisonTest, GET_SORT_ORDER_ATTRIBUTES_TEST_1_ATTRIBUTE) {
  const char cnf_string[] = "(l_orderkey = 20)";
  buildQueryOrderMaker(cnf_string);
}

TEST_F(ComparisonTest, GET_SORT_ORDER_ATTRIBUTES_TEST_MULTIPLE_ATTRIBUTES) {
  const char cnf_string[] =
      "(l_orderkey = 20) AND (l_suppkey = 30) AND (l_partkey = 40)";
  buildQueryOrderMaker(cnf_string);
}

TEST_F(ComparisonTest,
       GET_SORT_ORDER_ATTRIBUTES_TEST_MULTIPLE_ATTRIBUTES_NOT_IN_ORDER) {
  const char cnf_string[] =
      "(l_suppkey = 30) AND (l_partkey = 40) AND (l_orderkey = 10)";
  buildQueryOrderMaker(cnf_string);
}

TEST_F(ComparisonTest,
       GET_SORT_ORDER_ATTRIBUTES_TEST_FIRST_ATTRIBUTE_NOT_PRESENT) {
  const char cnf_string[] = "(l_suppkey = 30) AND (l_partkey = 40)";
  buildQueryOrderMaker(cnf_string);
}

TEST_F(ComparisonTest,
       GET_SORT_ORDER_ATTRIBUTES_TEST_SOME_MIDDLE_ATTRIBUTE_NOT_PRESENT) {
  const char cnf_string[] =
      "(l_partkey = 30) AND (l_linenumber = 4) AND (l_orderkey = 10)";
  buildQueryOrderMaker(cnf_string);
}

TEST_F(ComparisonTest,
       GET_SORT_ORDER_ATTRIBUTES_TEST_OTHER_COMPARISON_OPERATIONS) {
  const char cnf_string[] =
      "(l_partkey < 30) AND (l_suppkey = 4) AND (l_orderkey = 10)";
  buildQueryOrderMaker(cnf_string);
}

TEST_F(ComparisonTest,
       GET_SORT_ORDER_ATTRIBUTES_TEST_OTHER_COMPARISON_OPERATIONS_2) {
  const char cnf_string[] =
      "(l_partkey = 30) AND (l_suppkey > 4) AND (l_orderkey = 10)";
  buildQueryOrderMaker(cnf_string);
}

}  // namespace dbi