#include <iostream>
#include "Statistics.h"
#include "gtest/gtest.h"

extern "C" {
typedef struct yy_buffer_state *YY_BUFFER_STATE;
int yyparse(void);  // defined in y.tab.c
YY_BUFFER_STATE yy_scan_string(const char *str);
void yy_delete_buffer(YY_BUFFER_STATE buffer);
}

extern struct AndList *final;

using namespace std;

namespace dbi {

class StatisticsTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

 protected:
  StatisticsTest() {
    // You can do set-up work for each test here.
  }

  ~StatisticsTest() override {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  void SetUp() override {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  void TearDown() override {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  void PrintOperand(struct Operand *pOperand) {
    if (pOperand != NULL) {
      cout << pOperand->value << " ";
    } else
      return;
  }

  void PrintComparisonOp(struct ComparisonOp *pCom) {
    if (pCom != NULL) {
      PrintOperand(pCom->left);
      switch (pCom->code) {
        case 1:
          cout << " < ";
          break;
        case 2:
          cout << " > ";
          break;
        case 3:
          cout << " = ";
      }
      PrintOperand(pCom->right);

    } else {
      return;
    }
  }
  void PrintOrList(struct OrList *pOr) {
    if (pOr != NULL) {
      struct ComparisonOp *pCom = pOr->left;
      PrintComparisonOp(pCom);

      if (pOr->rightOr) {
        cout << " OR ";
        PrintOrList(pOr->rightOr);
      }
    } else {
      return;
    }
  }
  void PrintAndList(struct AndList *pAnd) {
    if (pAnd != NULL) {
      struct OrList *pOr = pAnd->left;
      PrintOrList(pOr);
      if (pAnd->rightAnd) {
        cout << " AND ";
        PrintAndList(pAnd->rightAnd);
      }
    } else {
      return;
    }
  }
};

TEST_F(StatisticsTest, ADD_REL_TEST) {
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  statistics.PrintRelationStore();
}

TEST_F(StatisticsTest, ADD_REL_TEST_MULTIPLE_TIMES) {
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 1000));
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 1000));
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 1000));
  statistics.PrintRelationStore();
}

TEST_F(StatisticsTest, ADD_ATT_TEST) {
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 60000));
  statistics.PrintRelationStore();
  statistics.PrintAttributeStore();
}

TEST_F(StatisticsTest, ADD_ATT_TEST_2) {
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey_2", 60000));
  statistics.PrintRelationStore();
  statistics.PrintAttributeStore();
}

TEST_F(StatisticsTest, ADD_ATT_TEST_WHEN_NEG_IS_PASSED) {
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", -1));
  statistics.PrintRelationStore();
  statistics.PrintAttributeStore();
}

TEST_F(StatisticsTest, ADD_ATT_MULTIPLE_TIMES) {
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 1000));
  statistics.PrintRelationStore();
  statistics.PrintAttributeStore();
}

TEST_F(StatisticsTest, ADD_ATT_DIFFERENT_ATTRIBUTES) {
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_suppkey", 1000));
  statistics.PrintRelationStore();
  statistics.PrintAttributeStore();
}

TEST_F(StatisticsTest, ADD_ATT_WITHOUT_RELATION_FIRST) {
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 60000));
}

TEST_F(StatisticsTest, COPY_REL_NORMAL) {
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_suppkey", 1000));
  statistics.CopyRel("lineitem", "lineitem_copy");
  statistics.PrintRelationStore();
  statistics.PrintAttributeStore();
}

TEST_F(StatisticsTest, COPY_REL_WHEN_RELATION_DOES_NOT_EXITS_IN_RELSTORE) {
  Statistics statistics;
  EXPECT_THROW(statistics.CopyRel("lineitem", "lineitem_copy"),
               std::runtime_error);
}

TEST_F(StatisticsTest, COPY_REL_WHEN_AN_ATTRIBUTE_DOES_NOT_EXIST_IN_ATTSTORE) {}

TEST_F(StatisticsTest, WRITE_AND_READ_STATISTICS) {
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_suppkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_linenumber", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_quantity", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_extendedprice", 1000));

  EXPECT_NO_THROW(statistics.AddRel("orders", 15000));
  EXPECT_NO_THROW(statistics.AddAtt("orders", "o_orderkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("orders", "o_custkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("orders", "o_orderstatus", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("orders", "o_totalprice", 1000));

  EXPECT_NO_THROW(statistics.Write("statistics.bin"));
  Statistics newStatistics;
  EXPECT_NO_THROW(newStatistics.Read("statistics.bin"));
  newStatistics.PrintRelationStore();
  newStatistics.PrintAttributeStore();
}

TEST_F(StatisticsTest, COPY_CONSTRUCTOR) {
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_suppkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_linenumber", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_quantity", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_extendedprice", 1000));

  Statistics newStatistics(statistics);
  newStatistics.PrintRelationStore();
  newStatistics.PrintAttributeStore();
}

TEST_F(StatisticsTest, ERROR_CHECK_FOR_PARSE_TREE_ATTRIBUTES_HAS_NO_ERRORS) {
  Statistics s;
  char *relName[] = {"supplier", "partsupp", NULL};
  char *cnf = "(s_suppkey = ps_suppkey)";

  s.AddRel(relName[0], 10000);
  s.AddAtt(relName[0], "s_suppkey", 10000);

  s.AddRel(relName[1], 800000);
  s.AddAtt(relName[1], "ps_suppkey", 10000);

  yy_scan_string(cnf);
  yyparse();

  EXPECT_NO_THROW(s.Estimate(final, relName, 2));
}

TEST_F(StatisticsTest,
       ERROR_CHECKING_FOR_PARSE_TREE_ATTRIBUTES_WILL_HAVE_ERRORS) {
  Statistics s;
  char *relName[] = {"supplier", "partsupp", NULL};
  s.AddRel(relName[0], 10000);
  s.AddRel(relName[1], 800000);

  char *cnf = "(s_suppkey = ps_suppkey)";

  // s.AddAtt(relName[0], "s_suppkey", 10000);
  // s.AddAtt(relName[1], "ps_suppkey", 10000);

  yy_scan_string(cnf);
  yyparse();

  EXPECT_THROW(s.Estimate(final, relName, 2), std::runtime_error);
}

TEST_F(StatisticsTest, DISJOINT_SET) {
  Statistics s;
  char *relName[] = {"supplier", "partsupp", NULL};
  s.AddRel(relName[0], 10000);
  s.AddRel(relName[1], 800000);
  std::string a(relName[0]);
  std::string b(relName[1]);
  s.PrintDisjointSets();
  s.Union(a, b);
  s.PrintDisjointSets();
  s.GetSets();
}

}  // namespace dbi
