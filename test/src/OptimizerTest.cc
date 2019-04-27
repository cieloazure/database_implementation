#include "Optimizer.h"
#include "ParseTreePrinter.h"
#include "gtest/gtest.h"

extern "C"
{
  typedef struct yy_buffer_state *YY_BUFFER_STATE;
  int yyparse(void); // defined in y.tab.c
  YY_BUFFER_STATE yy_scan_string(const char *str);
  void yy_delete_buffer(YY_BUFFER_STATE buffer);
}

extern struct FuncOperator
    *finalFunction;                   // the aggregate function (NULL if no agg)
extern struct TableList *tables;      // the list of tables and aliases in the query
extern struct AndList *boolean;       // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *
    attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)

namespace dbi
{

// The fixture for testing class Optimizer.
class OptimizerTest : public ::testing::Test
{
protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  OptimizerTest()
  {
    // You can do set-up work for each test here.
  }

  ~OptimizerTest() override
  {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  void SetUp() override
  {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  void TearDown() override
  {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests in the test case for Foo.
};

TEST_F(OptimizerTest, PARSE_TEST)
{
  //   const char cnf_string[] =
  //   "SELECT SUM DISTINCT (a + b) FROM c AS c1"
  //   "WHERE ('foo' > this.that OR 2 = 3) AND (12 > 5)"
  //   "GROUP BY a.f,"
  //   "c.d, g.f";
  const char cnf_string[] =
      "SELECT l_orderkey FROM lineitem AS li WHERE (l_orderkey = 5)";
  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
  yyparse();
  std::cout << cnf_string << std::endl;
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_2)
{
  //   const char cnf_string[] =
  //   "SELECT SUM DISTINCT (a + b) FROM c AS c1"
  //   "WHERE ('foo' > this.that OR 2 = 3) AND (12 > 5)"
  //   "GROUP BY a.f,"
  //   "c.d, g.f";
  const char cnf_string[] =
      "SELECT SUM(l_orderkey) FROM lineitem AS li WHERE (l_orderkey = 5)";
  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
  yyparse();
  std::cout << cnf_string << std::endl;
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_3)
{
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
  std::cout << cnf_string << std::endl;
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_4)
{
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
  std::cout << cnf_string << std::endl;
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_5)
{
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
  std::cout << cnf_string << std::endl;
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_6)
{
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
  std::cout << cnf_string << std::endl;
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_7)
{
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
  std::cout << cnf_string << std::endl;
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_8)
{
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
  std::cout << cnf_string << std::endl;
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_9)
{
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
  std::cout << cnf_string << std::endl;
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_10)
{
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
  std::cout << cnf_string << std::endl;
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_11)
{
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
  std::cout << cnf_string << std::endl;
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, PARSE_TEST_12)
{
  const char cnf_string[] =
      "SELECT a,b,c FROM d AS d1,e AS e1 WHERE (d1.a = e1.a)";
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
  std::cout << cnf_string << std::endl;
  ParseTreePrinter::PrintSQL(tables, attsToSelect, groupingAtts, boolean,
                             finalFunction);
}

TEST_F(OptimizerTest, ConstructJoinCNFTest)
{
  std::vector<std::vector<std::string>> joinMatrix;
  std::vector<std::string> row1;
  row1.push_back("");
  row1.push_back("b");
  row1.push_back("");
  row1.push_back("a");
  joinMatrix.push_back(row1);
  std::vector<std::string> row2;
  row2.push_back("b");
  row2.push_back("");
  row2.push_back("c");
  row2.push_back("");
  joinMatrix.push_back(row2);
  std::vector<std::string> row3;
  row3.push_back("");
  row3.push_back("c");
  row3.push_back("");
  row3.push_back("d");
  joinMatrix.push_back(row3);
  std::vector<std::string> row4;
  row4.push_back("a");
  row4.push_back("");
  row4.push_back("d");
  row4.push_back("");
  joinMatrix.push_back(row4);

  Optimizer o;
  std::vector<std::string> relNames;
  relNames.push_back("R");
  relNames.push_back("S");
  relNames.push_back("T");
  relNames.push_back("U");
  o.ConstructJoinCNF(relNames, joinMatrix, "R", "S");
  EXPECT_TRUE(final != NULL);
}

TEST_F(OptimizerTest, PERMUTATIONS_TEST)
{
  Optimizer o;
  auto combs = o.GenerateCombinations(4, 2);
  for (auto comb : combs)
  {
    for (int i = 0; i < comb.size(); i++)
    {
      if (comb[i])
      {
        std::cout << "1";
      }
      else
      {
        std::cout << "0";
      }
    }
    std::cout << std::endl;
  }
}

TEST_F(OptimizerTest, PERMUTATIONS_TEST_2)
{
  Optimizer o;
  auto combs = o.GenerateCombinations(4, 3);
  for (auto comb : combs)
  {
    for (int i = 0; i < comb.size(); i++)
    {
      if (comb[i])
      {
        std::cout << "1";
      }
      else
      {
        std::cout << "0";
      }
    }
    std::cout << std::endl;
  }
}

TEST_F(OptimizerTest, PERMUTATIONS_TEST_3)
{
  Optimizer o;
  auto combs = o.GenerateCombinations(3, 2);
  for (auto comb : combs)
  {
    for (int i = 0; i < comb.size(); i++)
    {
      if (comb[i])
      {
        std::cout << "1";
      }
      else
      {
        std::cout << "0";
      }
    }
    std::cout << std::endl;
  }
}

TEST_F(OptimizerTest, PERMUTATIONS_TEST_4)
{
  Optimizer o;
  auto combs = o.GenerateCombinations(5, 4);
  for (auto comb : combs)
  {
    for (int i = 0; i < comb.size(); i++)
    {
      if (comb[i])
      {
        std::cout << "1";
      }
      else
      {
        std::cout << "0";
      }
    }
    std::cout << std::endl;
  }
}

TEST_F(OptimizerTest, OptimizeOrderOfRelations)
{
  Statistics s;
  char *relName[] = {"R", "S", "T", "U"};

  s.AddRel(relName[0], 1000);
  s.AddAtt(relName[0], "a", 100);
  s.AddAtt(relName[0], "b", 200);

  s.AddRel(relName[1], 1000);
  s.AddAtt(relName[1], "b", 100);
  s.AddAtt(relName[1], "c", 500);

  s.AddRel(relName[2], 1000);
  s.AddAtt(relName[2], "c", 20);
  s.AddAtt(relName[2], "d", 50);

  s.AddRel(relName[3], 1000);
  s.AddAtt(relName[3], "a", 50);
  s.AddAtt(relName[3], "d", 1000);

  std::vector<std::vector<std::string>> joinMatrix;
  std::vector<std::string> row1;
  row1.push_back("");
  row1.push_back("b");
  row1.push_back("");
  row1.push_back("a");
  joinMatrix.push_back(row1);
  std::vector<std::string> row2;
  row2.push_back("b");
  row2.push_back("");
  row2.push_back("c");
  row2.push_back("");
  joinMatrix.push_back(row2);
  std::vector<std::string> row3;
  row3.push_back("");
  row3.push_back("c");
  row3.push_back("");
  row3.push_back("d");
  joinMatrix.push_back(row3);
  std::vector<std::string> row4;
  row4.push_back("a");
  row4.push_back("");
  row4.push_back("d");
  row4.push_back("");
  joinMatrix.push_back(row4);

  std::vector<std::string> relNames;
  relNames.push_back("R");
  relNames.push_back("S");
  relNames.push_back("T");
  relNames.push_back("U");

  Optimizer o;
  o.OptimumOrderingOfJoin(&s, relNames, joinMatrix);
}

TEST_F(OptimizerTest, SeparateJoinsAndSelects)
{
  Optimizer o;
  // Statistics s;
  char *relName[] = {"R", "S", "T", "U"};

  o.currentState->AddRel(relName[0], 1000);
  o.currentState->AddAtt(relName[0], "a", 100);
  o.currentState->AddAtt(relName[0], "b", 200);

  o.currentState->AddRel(relName[1], 1000);
  o.currentState->AddAtt(relName[1], "b", 100);
  o.currentState->AddAtt(relName[1], "c", 500);

  o.currentState->AddRel(relName[2], 1000);
  o.currentState->AddAtt(relName[2], "c", 20);
  o.currentState->AddAtt(relName[2], "d", 50);

  o.currentState->AddRel(relName[3], 1000);
  o.currentState->AddAtt(relName[3], "a", 50);
  o.currentState->AddAtt(relName[3], "d", 1000);

  const char cnf_string[] =
      "SELECT a, b FROM R AS r, S AS s WHERE (r.a = s.b) AND (r.a > 0)";
  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
  yyparse();

  std::vector<std::vector<std::string>> joinList;
  o.SeparateJoinsandSelects(joinList);
}

} // namespace dbi
