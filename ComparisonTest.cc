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
};

TEST_F(ComparisonTest, GET_SORT_ORDER_ATTRIBUTES_TEST) {
  Schema mySchema("catalog", "lineitem");

  const char cnf_string[] = "(l_orderkey = 30) AND (l_partkey = 10)";
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

  //   const char cnf_string_2[] = "(l_orderkey) AND (l_partkey)";
  //   YY_BUFFER_STATE buffer_2 = yy_scan_string(cnf_string);
  //   yyparse();
  //   yy_delete_buffer(buffer_2);

  //   Record literal2;
  //   CNF sort_pred;
  //   sort_pred.GrowFromParseTree(final, &mySchema,
  //                               literal2);  // constructs CNF predicate
  //   OrderMaker dummy;
  //   OrderMaker *sortorder = new OrderMaker();
  //   sort_pred.GetSortOrders(*sortorder, dummy);

  //   sortorder->Print();
  //   dummy.Print();
}

}  // namespace dbi