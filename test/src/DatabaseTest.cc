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
extern struct BulkLoad *bulkLoadInfo;
extern char *whichTableToDrop;
extern char *whereToGiveOutput;
extern char *whichTableToUpdateStatsFor;

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
    std::cout << head->attName << " " << head->attType << std::endl;
    head = head->next;
  }
}

TEST_F(DatabaseTest, TEST_CREATE_TABLE_SCHEMA_2) {
  const char cnf_string[] =
      "CREATE TABLE mytable (att1 INTEGER, att2 DOUBLE, att3 STRING) AS "
      "SORTED ON (att1, att2);";
  yy_scan_string(cnf_string);
  yyparse();
  EXPECT_TRUE(newTable != NULL);
  std::cout << newTable->tName << std::endl;
  std::string s1(newTable->tName);
  EXPECT_EQ(s1, "mytable");
  std::cout << newTable->fileType << std::endl;
  std::string s2(newTable->fileType);
  EXPECT_EQ(s2, "SORTED");

  struct SchemaAtts *head = newTable->schemaAtts;
  while (head != NULL) {
    std::cout << head->attName << " " << head->attType << std::endl;
    head = head->next;
  }

  struct SortAtts *head2 = newTable->sortAtts;
  while (head2 != NULL) {
    std::cout << head2->name << std::endl;
    head2 = head2->next;
  }
}

TEST_F(DatabaseTest, TEST_INSERT_INTO_QUERY) {
  const char cnf_string[] = "INSERT 'myFile' INTO mytable;";
  yy_scan_string(cnf_string);
  yyparse();
  EXPECT_TRUE(bulkLoadInfo != NULL);
  std::cout << bulkLoadInfo->fName << std::endl;
  std::cout << bulkLoadInfo->tName << std::endl;
}

TEST_F(DatabaseTest, TEST_PARSE_OF_DROP) {
  const char cnf_string[] = "DROP TABLE myTable;";
  yy_scan_string(cnf_string);
  yyparse();
  std::cout << whichTableToDrop << std::endl;
}

TEST_F(DatabaseTest, TEST_PARSE_OF_OUTPUT) {
  const char cnf_string[] = "SET OUTPUT STDOUT;";
  yy_scan_string(cnf_string);
  yyparse();
  std::cout << whereToGiveOutput << std::endl;
}

TEST_F(DatabaseTest, TEST_PARSE_OF_OUTPUT_2) {
  const char cnf_string[] = "SET OUTPUT 'myFile';";
  yy_scan_string(cnf_string);
  yyparse();
  std::cout << whereToGiveOutput << std::endl;
}

TEST_F(DatabaseTest, TEST_PARSE_OF_OUTPUT_3) {
  const char cnf_string[] = "SET OUTPUT NONE;";
  yy_scan_string(cnf_string);
  yyparse();
  std::cout << whereToGiveOutput << std::endl;
}

TEST_F(DatabaseTest, TEST_PARSE_OF_UPDATE_STATS) {
  const char cnf_string[] = "UPDATE STATISTICS FOR myTable;";
  yy_scan_string(cnf_string);
  yyparse();
  std::cout << whichTableToUpdateStatsFor << std::endl;
}

TEST_F(DatabaseTest, TEST_CREATE_DATABASE) {
  Database d;
  const char cnf_string[] =
      "CREATE TABLE mytable123 (att1 INTEGER, att2 DOUBLE, att3 STRING) AS "
      "HEAP;";
  std::string ctquery(cnf_string);
  d.ExecuteCommand(ctquery);
}

TEST_F(DatabaseTest, TEST_CREATE_DATABASE_2) {
  Database d;
  const char cnf_string[] =
      "CREATE TABLE mytablesort456 (att1 INTEGER, att2 DOUBLE, att3 STRING) AS "
      "SORTED ON (att1, att2);";
  std::string ctquery(cnf_string);
  d.ExecuteCommand(ctquery);
}

TEST_F(DatabaseTest, TEST_BULK_LOAD) {
  Database d;
  const char cnf_string[] =
      "CREATE TABLE new_region (r_regionkey INTEGER, r_name DOUBLE,r_comment "
      "STRING) AS "
      "HEAP;";
  std::string ctquery(cnf_string);
  d.ExecuteCommand(ctquery);

  const char cnf_string2[] = "INSERT 'data_files/1G/region.tbl' INTO new_region;";
  std::string iquery(cnf_string2);
  d.ExecuteCommand(iquery);
}
}  // namespace dbi
