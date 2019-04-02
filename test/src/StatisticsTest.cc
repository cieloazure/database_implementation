#include "Statistics.h"
#include "gtest/gtest.h"

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
  statistics.AddRel("lineitem", 60175);
  statistics.AddRel("lineitem", 600);
}
