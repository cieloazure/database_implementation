#include "Optimizer.h"
#include "ParseTreePrinter.h"
#include "gtest/gtest.h"

extern "C" {
typedef struct yy_buffer_state *YY_BUFFER_STATE;
int yyparse(void);  // defined in y.tab.c
YY_BUFFER_STATE yy_scan_string(const char *str);
void yy_delete_buffer(YY_BUFFER_STATE buffer);
}

extern struct FuncOperator
    *finalFunction;               // the aggregate function (NULL if no agg)
extern struct TableList *tables;  // the list of tables and aliases in the query
extern struct AndList *boolean;   // the predicate in the WHERE clause
extern struct NameList *groupingAtts;  // grouping atts (NULL if no grouping)
extern struct NameList *
    attsToSelect;  // the set of attributes in the SELECT (NULL if no such atts)

namespace dbi {

// The fixture for testing class Optimizer.
class OptimizerTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  OptimizerTest() {
    // You can do set-up work for each test here.
  }

  ~OptimizerTest() override {
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

TEST_F(OptimizerTest, PARSE_TEST) {
  //   const char cnf_string[] =
  //   "SELECT SUM DISTINCT (a + b) FROM c AS c1"
  //   "WHERE ('foo' > this.that OR 2 = 3) AND (12 > 5)"
  //   "GROUP BY a.f,"
  //   "c.d, g.f";
  const char cnf_string[] =
      "SELECT l_orderkey FROM lineitem AS li WHERE (l_orderkey = 5)";
  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
  yyparse();
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_2) {
  //   const char cnf_string[] =
  //   "SELECT SUM DISTINCT (a + b) FROM c AS c1"
  //   "WHERE ('foo' > this.that OR 2 = 3) AND (12 > 5)"
  //   "GROUP BY a.f,"
  //   "c.d, g.f";
  const char cnf_string[] =
      "SELECT SUM(l_orderkey) FROM lineitem AS li WHERE (l_orderkey = 5)";
  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
  yyparse();
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_3) {
  //   const char cnf_string[] =
  //   "SELECT SUM DISTINCT (a + b) FROM c AS c1"
  //   "WHERE ('foo' > this.that OR 2 = 3) AND (12 > 5)"
  //   "GROUP BY a.f,"
  //   "c.d, g.f";
  const char cnf_string[] =
      "SELECT SUM DISTINCT(l_orderkey) FROM lineitem AS li WHERE (l_orderkey = "
      "5)";
  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
  yyparse();
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_4) {
  //   const char cnf_string[] =
  //   "SELECT SUM DISTINCT (a + b) FROM c AS c1"
  //   "WHERE ('foo' > this.that OR 2 = 3) AND (12 > 5)"
  //   "GROUP BY a.f,"
  //   "c.d, g.f";
  const char cnf_string[] =
      "SELECT SUM DISTINCT(l_orderkey + l_partkey) FROM lineitem AS li WHERE "
      "(l_orderkey = "
      "5)";
  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
  yyparse();
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_5) {
  //   const char cnf_string[] =
  //   "SELECT SUM DISTINCT (a + b) FROM c AS c1"
  //   "WHERE ('foo' > this.that OR 2 = 3) AND (12 > 5)"
  //   "GROUP BY a.f,"
  //   "c.d, g.f";
  const char cnf_string[] =
      "SELECT SUM DISTINCT ((l_orderkey + l_partkey) * l_suppkey) FROM "
      "lineitem "
      "AS li WHERE "
      "(l_orderkey = "
      "5)";
  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
  yyparse();
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_6) {
  //   const char cnf_string[] =
  //   "SELECT SUM DISTINCT (a + b) FROM c AS c1"
  //   "WHERE ('foo' > this.that OR 2 = 3) AND (12 > 5)"
  //   "GROUP BY a.f,"
  //   "c.d, g.f";
  const char cnf_string[] =
      "SELECT SUM DISTINCT ((l_orderkey + l_partkey) * (l_suppkey - "
      "l_itemkey)) FROM "
      "lineitem "
      "AS li WHERE "
      "(l_orderkey = "
      "5)";
  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
  yyparse();
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_7) {
  //   const char cnf_string[] =
  //   "SELECT SUM DISTINCT (a + b) FROM c AS c1"
  //   "WHERE ('foo' > this.that OR 2 = 3) AND (12 > 5)"
  //   "GROUP BY a.f,"
  //   "c.d, g.f";
  const char cnf_string[] =
      "SELECT SUM DISTINCT (- l_orderkey)"
      "FROM "
      "lineitem "
      "AS li WHERE "
      "(l_orderkey = "
      "5)";
  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
  yyparse();
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_8) {
  //   const char cnf_string[] =
  //   "SELECT SUM DISTINCT (a + b) FROM c AS c1"
  //   "WHERE ('foo' > this.that OR 2 = 3) AND (12 > 5)"
  //   "GROUP BY a.f,"
  //   "c.d, g.f";
  const char cnf_string[] =
      "SELECT l_orderkey FROM lineitem AS li, orders AS o WHERE (l_orderkey = "
      "5)";
  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
  yyparse();
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_9) {
  //   const char cnf_string[] =
  //   "SELECT SUM DISTINCT (a + b) FROM c AS c1"
  //   "WHERE ('foo' > this.that OR 2 = 3) AND (12 > 5)"
  //   "GROUP BY a.f,"
  //   "c.d, g.f";
  const char cnf_string[] =
      "SELECT DISTINCT l_orderkey, l_suppkey FROM lineitem AS li WHERE "
      "(l_orderkey = "
      "5)";
  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
  yyparse();
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_10) {
  //   const char cnf_string[] =
  //   "SELECT SUM DISTINCT (a + b) FROM c AS c1"
  //   "WHERE ('foo' > this.that OR 2 = 3) AND (12 > 5)"
  //   "GROUP BY a.f,"
  //   "c.d, g.f";
  const char cnf_string[] =
      "SELECT SUM(l_orderkey), l_suppkey FROM lineitem AS li WHERE "
      "(l_orderkey = "
      "5)";
  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
  yyparse();
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_11) {
  const char cnf_string[] =
      "SELECT SUM DISTINCT(a.b + b),"
      "d.g FROM a AS b WHERE('foo' > this.that OR 2 = 3) AND (12 > 5) GROUP BY "
      "a.f, c.d, g.f";
  //   "SELECT SUM DISTINCT (a + b) FROM c AS c1"
  //   "WHERE ('foo' > this.that OR 2 = 3) AND (12 > 5)"
  //   "GROUP BY a.f,"
  //   "c.d, g.f";
  //   const char cnf_string[] =
  //       "SELECT SUM(l_orderkey), l_suppkey FROM lineitem AS li WHERE "
  //       "(l_orderkey = "
  //       "5)";
  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
  yyparse();
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}
}  // namespace dbi
