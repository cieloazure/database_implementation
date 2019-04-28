#include "ParseTreePrinter.h"
#include "QueryOptimizer.h"
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

// The fixture for testing class QueryOptimizer.
class QueryOptimizerTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  QueryOptimizerTest() {
    // You can do set-up work for each test here.
  }

  ~QueryOptimizerTest() override {
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

TEST_F(QueryOptimizerTest, PARSE_TEST) {
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

TEST_F(QueryOptimizerTest, PARSE_TEST_2) {
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

TEST_F(QueryOptimizerTest, PARSE_TEST_3) {
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

TEST_F(QueryOptimizerTest, PARSE_TEST_4) {
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

TEST_F(QueryOptimizerTest, PARSE_TEST_5) {
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

TEST_F(QueryOptimizerTest, PARSE_TEST_6) {
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

TEST_F(QueryOptimizerTest, PARSE_TEST_7) {
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

TEST_F(QueryOptimizerTest, PARSE_TEST_8) {
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

TEST_F(QueryOptimizerTest, PARSE_TEST_9) {
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

TEST_F(QueryOptimizerTest, PARSE_TEST_10) {
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

TEST_F(QueryOptimizerTest, PARSE_TEST_11) {
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

TEST_F(QueryOptimizerTest, PARSE_TEST_12) {
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

TEST_F(QueryOptimizerTest, ConstructJoinCNFTest) {
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

  QueryOptimizer o;
  std::vector<std::string> relNames;
  relNames.push_back("R");
  relNames.push_back("S");
  relNames.push_back("T");
  relNames.push_back("U");
  o.ConstructJoinCNF(relNames, joinMatrix, "R", "S");
  EXPECT_TRUE(final != NULL);
}

TEST_F(QueryOptimizerTest, PERMUTATIONS_TEST) {
  QueryOptimizer o;
  auto combs = o.GenerateCombinations(4, 2);
  for (auto comb : combs) {
    for (int i = 0; i < comb.size(); i++) {
      if (comb[i]) {
        std::cout << "1";
      } else {
        std::cout << "0";
      }
    }
    std::cout << std::endl;
  }
}

TEST_F(QueryOptimizerTest, PERMUTATIONS_TEST_2) {
  QueryOptimizer o;
  auto combs = o.GenerateCombinations(4, 3);
  for (auto comb : combs) {
    for (int i = 0; i < comb.size(); i++) {
      if (comb[i]) {
        std::cout << "1";
      } else {
        std::cout << "0";
      }
    }
    std::cout << std::endl;
  }
}

TEST_F(QueryOptimizerTest, PERMUTATIONS_TEST_3) {
  QueryOptimizer o;
  auto combs = o.GenerateCombinations(3, 2);
  for (auto comb : combs) {
    for (int i = 0; i < comb.size(); i++) {
      if (comb[i]) {
        std::cout << "1";
      } else {
        std::cout << "0";
      }
    }
    std::cout << std::endl;
  }
}

TEST_F(QueryOptimizerTest, PERMUTATIONS_TEST_4) {
  QueryOptimizer o;
  auto combs = o.GenerateCombinations(5, 4);
  for (auto comb : combs) {
    for (int i = 0; i < comb.size(); i++) {
      if (comb[i]) {
        std::cout << "1";
      } else {
        std::cout << "0";
      }
    }
    std::cout << std::endl;
  }
}

TEST_F(QueryOptimizerTest, OptimizeOrderOfRelations) {
  // Set up statistics
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

  // Set up join matrix
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

  // Set up map of relNameToSchema
  Attribute IA = {(char *)"a", Int};
  Attribute IB = {(char *)"b", Int};
  Attribute IC = {(char *)"c", Int};
  Attribute ID = {(char *)"d", Int};

  Attribute rAtts[] = {IA, IB};
  Schema R("R", 2, rAtts);

  Attribute s1Atts[] = {IB, IC};
  Schema S("S", 2, s1Atts);

  Attribute tAtts[] = {IC, ID};
  Schema T("T", 2, tAtts);

  Attribute uAtts[] = {IA, ID};
  Schema U("U", 2, uAtts);

  std::unordered_map<std::string, Schema *> relNameToSchema;
  relNameToSchema["R"] = &R;
  relNameToSchema["S"] = &S;
  relNameToSchema["T"] = &T;
  relNameToSchema["U"] = &U;

  // call QueryOptimizer
  QueryOptimizer o(&s, &relNameToSchema);
  o.OptimumOrderingOfJoin(relNameToSchema, &s, relNames, joinMatrix);
}

TEST_F(QueryOptimizerTest, SeparateJoinsAndSelects) {
  QueryOptimizer o;
  // Statistics s;
  char *relName[] = {"R", "S", "T", "U"};

  Statistics *currentStats = new Statistics;
  currentStats->AddRel(relName[0], 1000);
  currentStats->AddAtt(relName[0], "a", 100);
  currentStats->AddAtt(relName[0], "b", 200);

  currentStats->AddRel(relName[1], 1000);
  currentStats->AddAtt(relName[1], "b", 100);
  currentStats->AddAtt(relName[1], "c", 500);

  currentStats->AddRel(relName[2], 1000);
  currentStats->AddAtt(relName[2], "c", 20);
  currentStats->AddAtt(relName[2], "d", 50);

  currentStats->AddRel(relName[3], 1000);
  currentStats->AddAtt(relName[3], "a", 50);
  currentStats->AddAtt(relName[3], "d", 1000);

  const char cnf_string[] =
      "SELECT a, b FROM R AS r, S AS s WHERE (r.a = s.b) AND (r.a > 0)";
  YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
  yyparse();

  std::vector<std::vector<std::string>> joinList;
  o.SeparateJoinsandSelects(currentStats, joinList);
}

TEST_F(QueryOptimizerTest, OptimizeOrderOfRelations2) {
  // Set up statistics
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

  // Set up join matrix
  std::vector<std::vector<std::string>> joinMatrix;
  std::vector<std::string> row1;
  row1.push_back("");
  row1.push_back("b");
  row1.push_back("");
  joinMatrix.push_back(row1);
  std::vector<std::string> row2;
  row2.push_back("b");
  row2.push_back("");
  row2.push_back("c");
  joinMatrix.push_back(row2);
  std::vector<std::string> row3;
  row3.push_back("");
  row3.push_back("c");
  row3.push_back("");
  joinMatrix.push_back(row3);

  std::vector<std::string> relNames;
  relNames.push_back("R");
  relNames.push_back("S");
  relNames.push_back("T");

  // Set up map of relNameToSchema
  Attribute IA = {(char *)"a", Int};
  Attribute IB = {(char *)"b", Int};
  Attribute IC = {(char *)"c", Int};
  Attribute ID = {(char *)"d", Int};

  Attribute rAtts[] = {IA, IB};
  Schema R("R", 2, rAtts);

  Attribute s1Atts[] = {IB, IC};
  Schema S("S", 2, s1Atts);

  Attribute tAtts[] = {IC, ID};
  Schema T("T", 2, tAtts);

  Attribute uAtts[] = {IA, ID};
  Schema U("U", 2, uAtts);

  std::unordered_map<std::string, Schema *> relNameToSchema;
  relNameToSchema["R"] = &R;
  relNameToSchema["S"] = &S;
  relNameToSchema["T"] = &T;
  relNameToSchema["U"] = &U;

  // call QueryOptimizer
  QueryOptimizer o(&s, &relNameToSchema);
  o.OptimumOrderingOfJoin(relNameToSchema, &s, relNames, joinMatrix);
}

TEST_F(QueryOptimizerTest, OptimizeOrderOfRelations3) {
  // Set up statistics
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

  // Set up join matrix
  std::vector<std::vector<std::string>> joinMatrix;
  std::vector<std::string> row1;
  row1.push_back("");
  row1.push_back("b");
  joinMatrix.push_back(row1);
  std::vector<std::string> row2;
  row2.push_back("b");
  row2.push_back("");
  joinMatrix.push_back(row2);

  std::vector<std::string> relNames;
  relNames.push_back("R");
  relNames.push_back("S");

  // Set up map of relNameToSchema
  Attribute IA = {(char *)"a", Int};
  Attribute IB = {(char *)"b", Int};
  Attribute IC = {(char *)"c", Int};
  Attribute ID = {(char *)"d", Int};

  Attribute rAtts[] = {IA, IB};
  Schema R("R", 2, rAtts);

  Attribute s1Atts[] = {IB, IC};
  Schema S("S", 2, s1Atts);

  Attribute tAtts[] = {IC, ID};
  Schema T("T", 2, tAtts);

  Attribute uAtts[] = {IA, ID};
  Schema U("U", 2, uAtts);

  std::unordered_map<std::string, Schema *> relNameToSchema;
  relNameToSchema["R"] = &R;
  relNameToSchema["S"] = &S;
  relNameToSchema["T"] = &T;
  relNameToSchema["U"] = &U;

  // call QueryOptimizer
  QueryOptimizer o(&s, &relNameToSchema);
  o.OptimumOrderingOfJoin(relNameToSchema, &s, relNames, joinMatrix);
}

TEST_F(QueryOptimizerTest, GetOptimizedPlanTest) {
  char *relName[] = {"R", "S", "T", "U"};
  const char cnf_string[] =
      "SELECT a, b FROM R AS r, S AS s WHERE (r.b = s.b) AND (r.a > 0)";
  Statistics *currentStats = new Statistics;
  currentStats->AddRel(relName[0], 1000);
  currentStats->AddAtt(relName[0], "a", 100);
  currentStats->AddAtt(relName[0], "b", 200);

  currentStats->AddRel(relName[1], 1000);
  currentStats->AddAtt(relName[1], "b", 100);
  currentStats->AddAtt(relName[1], "c", 500);

  currentStats->AddRel(relName[2], 1000);
  currentStats->AddAtt(relName[2], "c", 20);
  currentStats->AddAtt(relName[2], "d", 50);

  currentStats->AddRel(relName[3], 1000);
  currentStats->AddAtt(relName[3], "a", 50);
  currentStats->AddAtt(relName[3], "d", 1000);

  // Set up map of relNameToSchema
  Attribute IA = {(char *)"a", Int};
  Attribute IB = {(char *)"b", Int};
  Attribute IC = {(char *)"c", Int};
  Attribute ID = {(char *)"d", Int};

  Attribute rAtts[] = {IA, IB};
  Schema R("R", 2, rAtts);

  Attribute s1Atts[] = {IB, IC};
  Schema S("S", 2, s1Atts);

  Attribute tAtts[] = {IC, ID};
  Schema T("T", 2, tAtts);

  Attribute uAtts[] = {IA, ID};
  Schema U("U", 2, uAtts);

  std::unordered_map<std::string, Schema *> relNameToSchema;
  relNameToSchema["R"] = &R;
  relNameToSchema["S"] = &S;
  relNameToSchema["T"] = &T;
  relNameToSchema["U"] = &U;

  QueryOptimizer o(currentStats, &relNameToSchema);
  std::string cnfString(cnf_string);
  QueryPlan *qp = o.GetOptimizedPlan(cnfString);
}
// TEST_F(QueryOptimizerTest, TESTSCHEMA)
// {
//   Attribute IA = {(char *)"int", Int};
//   Attribute SA = {(char *)"string", String};
//   Attribute DA = {(char *)"double", Double};
//   Attribute att3[] = {IA, SA, DA};
//   Schema *schema = new Schema("R1", 3, att3);
// }

TEST_F(QueryOptimizerTest, GenerateTree) {
  char *relName[] = {"R", "S", "T", "U"};
  const char cnf_string[] =
      "SELECT a, b FROM R AS r, S AS s WHERE (r.b = s.b) AND (r.a > 0)";
  Statistics *currentStats = new Statistics;
  currentStats->AddRel(relName[0], 1000);
  currentStats->AddAtt(relName[0], "a", 100);
  currentStats->AddAtt(relName[0], "b", 200);

  currentStats->AddRel(relName[1], 1000);
  currentStats->AddAtt(relName[1], "b", 100);
  currentStats->AddAtt(relName[1], "c", 500);

  currentStats->AddRel(relName[2], 1000);
  currentStats->AddAtt(relName[2], "c", 20);
  currentStats->AddAtt(relName[2], "d", 50);

  currentStats->AddRel(relName[3], 1000);
  currentStats->AddAtt(relName[3], "a", 50);
  currentStats->AddAtt(relName[3], "d", 1000);

  // Set up map of relNameToSchema
  Attribute IA = {(char *)"a", Int};
  Attribute IB = {(char *)"b", Int};
  Attribute IC = {(char *)"c", Int};
  Attribute ID = {(char *)"d", Int};

  Attribute rAtts[] = {IA, IB};
  Schema R("R", 2, rAtts);

  Attribute s1Atts[] = {IB, IC};
  Schema S("S", 2, s1Atts);

  Attribute tAtts[] = {IC, ID};
  Schema T("T", 2, tAtts);

  Attribute uAtts[] = {IA, ID};
  Schema U("U", 2, uAtts);

  std::unordered_map<std::string, Schema *> relNameToSchema;
  relNameToSchema["R"] = &R;
  relNameToSchema["S"] = &S;
  relNameToSchema["T"] = &T;
  relNameToSchema["U"] = &U;

  QueryOptimizer o(currentStats, &relNameToSchema);
  std::string cnfString(cnf_string);
  o.GetOptimizedPlan(cnfString);
}
}  // namespace dbi
