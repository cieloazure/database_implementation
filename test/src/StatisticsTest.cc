#include "Statistics.h"
#include "gtest/gtest.h"

namespace dbi{

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
};

TEST_F(StatisticsTest, ADD_REL_TEST) {
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
}

TEST_F(StatisticsTest, ADD_REL_TEST_MULTIPLE_TIMES) {
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 1000));
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 1000));
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 1000));
}

TEST_F(StatisticsTest, ADD_ATT_TEST) {
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 60000));
}

TEST_F(StatisticsTest, ADD_ATT_TEST_WHEN_NEG_IS_PASSED) {
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", -1));
}

TEST_F(StatisticsTest, ADD_ATT_MULTIPLE_TIMES){
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 1000));
}

TEST_F(StatisticsTest, ADD_ATT_DIFFERENT_ATTRIBUTES){
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_suppkey", 1000));
}

TEST_F(StatisticsTest, ADD_ATT_WITHOUT_RELATION_FIRST){
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 60000));
}

TEST_F(StatisticsTest, COPY_REL_NORMAL){
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_suppkey", 1000));
  statistics.CopyRel("lineitem", "lineitem_copy");
}

TEST_F(StatisticsTest, COPY_REL_WHEN_RELATION_DOES_NOT_EXITS_IN_RELSTORE){
  Statistics statistics;
  EXPECT_THROW(statistics.CopyRel("lineitem", "lineitem_copy"), std::runtime_error);
}

TEST_F(StatisticsTest, COPY_REL_WHEN_AN_ATTRIBUTE_DOES_NOT_EXIST_IN_ATTSTORE){
}

TEST_F(StatisticsTest, WRITE_AND_READ_STATISTICS){
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
}

TEST_F(StatisticsTest, COPY_CONSTRUCTOR){
  Statistics statistics;
  EXPECT_NO_THROW(statistics.AddRel("lineitem", 60175));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_orderkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_suppkey", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_linenumber", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_quantity", 1000));
  EXPECT_NO_THROW(statistics.AddAtt("lineitem", "l_extendedprice", 1000));

  Statistics newStatistics(statistics);
}

}
