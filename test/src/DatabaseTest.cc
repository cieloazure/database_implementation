#include "Database.h"
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
extern struct NewTable *newTable;

namespace dbi {

// The fixture for testing class QueryOptimizer.
class DatabaseTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  DatabaseTest() {
    // You can do set-up work for each test here.
  }

  ~DatabaseTest() override {
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

TEST_F(DatabaseTest, TEST_CREATE_TABLE_SCHEMA) {
  const char cnf_string[] =
      "CREATE TABLE mytable (att1 INTEGER, att2 DOUBLE, att3 STRING) AS "
      "HEAP;";
  yy_scan_string(cnf_string);
  yyparse();
  EXPECT_TRUE(newTable != NULL);
  std::cout << newTable->tName << std::endl;
  std::string s1(newTable->tName);
  EXPECT_EQ(s1, "mytable");
  std::cout << newTable->fileType << std::endl;
  std::string s2(newTable->fileType);
  EXPECT_EQ(s2, "HEAP");

  struct SchemaAtts *head = newTable->schemaAtts;
  while (head != NULL) {
    std::cout << head->attName << head->attType << std::endl;
    head = head->next;
  }
}
}  // namespace dbi
