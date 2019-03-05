#include "DBFile.h"
#include "File.h"
#include "SortedDBFile.h"
#include "gtest/gtest.h"

extern "C" {
typedef struct yy_buffer_state *YY_BUFFER_STATE;
int yyparse(void);  // defined in y.tab.c
YY_BUFFER_STATE yy_scan_string(const char *str);
void yy_delete_buffer(YY_BUFFER_STATE buffer);
}

struct SortInfo {
  OrderMaker *sortOrder;
  int runLength;

  SortInfo(OrderMaker *so, int rl) {
    sortOrder = so;
    runLength = rl;
  }
};

Schema mySchema("catalog", "lineitem");

extern struct AndList *final;

namespace dbi {

// The fixture for testing class SortedDBFile.
class SortedDBFileTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  SortedDBFileTest() {
    // You can do set-up work for each test here.
  }

  ~SortedDBFileTest() override {
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
    remove("gtest.bin");
    remove("gtest.header");
  }

  static void SetUpTestCase() {
    DBFile *sortedFile = new DBFile();
    OrderMaker *o = new OrderMaker(&mySchema);
    SortInfo *si = new SortInfo(o, 3);
    fType type = sorted;
    if (sortedFile->Create("gtest.bin", type, (void *)si)) {
      const char *loadpath = "data_files/lineitem.tbl";

      Schema mySchema("catalog", "lineitem");
      sortedFile->Load(mySchema, loadpath);
      sortedFile->Close();
    }
  }

  static void TearDownTestCase() {}

  // Objects declared here can be used by all tests in the test case for Foo.
  OrderMaker *o = new OrderMaker(&mySchema);
  SortInfo *si = new SortInfo(o, 3);
};

TEST_F(SortedDBFileTest, CREATE_SUCCESS) {
  DBFile *sortFile = new DBFile();
  fType t = sorted;
  EXPECT_TRUE(sortFile->Create("gtest.bin", t, (void *)si));
  delete sortFile;
}

TEST_F(SortedDBFileTest, CREATE_FAILURE_WHEN_FILE_NAME_IS_TOO_LARGE) {
  string a = "a";
  int MAX_FILE_NAME_SIZE = 255;
  string s;
  for (int i = 0; i < MAX_FILE_NAME_SIZE; i++) {
    s.append(a);
  }
  s += ".bin";

  DBFile *sortFile = new DBFile();
  fType t = sorted;
  EXPECT_FALSE(sortFile->Create(s.c_str(), t, (void *)si));
  delete sortFile;
}

TEST_F(SortedDBFileTest, CREATE_FAILURE_WHEN_FILENAME_IS_AN_EMPTY_STRING) {
  DBFile *sortFile = new DBFile();
  fType t = sorted;
  EXPECT_FALSE(sortFile->Create("", t, (void *)si));
  delete sortFile;
}

TEST_F(SortedDBFileTest, CREATE_FAILURE_WHEN_FILE_TYPE_IS_INVALID) {
  DBFile *sortFile = new DBFile();
  EXPECT_FALSE(sortFile->Create("gtest.bin", (fType)4, (void *)si));
  delete sortFile;
}

TEST_F(SortedDBFileTest, OPEN_SUCCESS) {
  DBFile *sortFile = new DBFile();
  fType t = sorted;
  if (sortFile->Create("gtest.bin", t, (void *)si)) {
    sortFile->Close();
    EXPECT_TRUE(sortFile->Open("gtest.bin"));
  }
  delete sortFile;
}

TEST_F(SortedDBFileTest, OPEN_FAILURE_MISSING_METADATA_FILE) {
  DBFile *sortFile = new DBFile();
  fType t = sorted;
  if (sortFile->Create("gtest.bin", t, (void *)si)) {
    sortFile->Close();
    remove("gtest.header");
    EXPECT_FALSE(sortFile->Open("gtest.bin"));
  }
  delete sortFile;
}

TEST_F(SortedDBFileTest, OPEN_FAILURE_ON_INVALID_FILE_TYPE_IN_METADATA_FILE) {
  DBFile *sortFile = new DBFile();
  fType t = sorted;
  if (sortFile->Create("gtest.bin", t, (void *)si)) {
    sortFile->Close();
    int fd = open("gtest.header", O_RDWR, S_IRUSR | S_IWUSR);
    lseek(fd, 0, SEEK_SET);
    int invalid_value = 4;
    write(fd, &invalid_value, sizeof(fType));
    EXPECT_FALSE(sortFile->Open("gtest.bin"));
  }
  delete sortFile;
}

TEST_F(SortedDBFileTest, OPEN_FAILURE_WHEN_A_FILE_DOES_NOT_EXISTS) {
  DBFile *sortFile = new DBFile();
  EXPECT_FALSE(sortFile->Open("gtest2.bin"));
  delete sortFile;
}

TEST_F(SortedDBFileTest, OPEN_FAILURE_WHEN_A_FILE_NAME_IS_INVALID) {
  DBFile *sortFile = new DBFile();
  fType t = sorted;
  if (sortFile->Create("gtest.bin", t, (void *)si)) {
    sortFile->Close();
    EXPECT_FALSE(sortFile->Open(""));
  }
  delete sortFile;
}

TEST_F(SortedDBFileTest, MOVE_FIRST_WHEN_GET_NEXT_HAVE_BEEN_CALLED_BEFORE) {
  SortedDBFile *sortedFile = new SortedDBFile();
  fType t = sorted;
  if (sortedFile->Create("gtest.bin", t, (void *)si)) {
    const char *loadpath = "data_files/lineitem.tbl";

    Schema mySchema("catalog", "lineitem");
    sortedFile->Load(mySchema, loadpath);

    FILE *load_file = fopen(loadpath, "r");
    Record *temp_source_file_record = new Record();
    Record *temp_sorted_file_record = new Record();
    Record *first = new Record();
    for (int i = 0; i < 10; i++) {
      temp_source_file_record->SuckNextRecord(&mySchema, load_file);
      if (i == 3) {
        first->Copy(temp_source_file_record);
      }
      sortedFile->GetNext(*temp_sorted_file_record);
    }

    sortedFile->MoveFirst();
    sortedFile->GetNext(*temp_sorted_file_record);

    ComparisonEngine comp;
    OrderMaker order(&mySchema);

    temp_sorted_file_record->Print(&mySchema);
    first->Print(&mySchema);
    EXPECT_TRUE(comp.Compare(temp_sorted_file_record, first, &order) == 0);

    delete temp_source_file_record;
    delete temp_sorted_file_record;
    delete first;
    fclose(load_file);
    sortedFile->Close();
  }
}

TEST_F(SortedDBFileTest, MOVE_FIRST_ON_FIRST_RECORD) {
  SortedDBFile *sortedFile = new SortedDBFile();
  fType t = sorted;
  if (sortedFile->Create("gtest.bin", t, (void *)si)) {
    const char *loadpath = "data_files/lineitem.tbl";

    Schema mySchema("catalog", "lineitem");
    sortedFile->Load(mySchema, loadpath);

    FILE *load_file = fopen(loadpath, "r");
    Record *temp_source_file_record = new Record();
    Record *temp_sorted_file_record = new Record();
    Record *first = new Record();
    for (int i = 0; i < 1; i++) {
      temp_source_file_record->SuckNextRecord(&mySchema, load_file);
      if (i == 0) {
        first->Copy(temp_source_file_record);
      }
      // No Get Next is being called
    }

    // Just opened a file and moving first
    sortedFile->MoveFirst();
    sortedFile->GetNext(*temp_sorted_file_record);

    ComparisonEngine comp;
    OrderMaker order(&mySchema);

    temp_sorted_file_record->Print(&mySchema);
    first->Print(&mySchema);
    EXPECT_TRUE(comp.Compare(temp_sorted_file_record, first, &order) == 0);

    delete temp_source_file_record;
    delete temp_sorted_file_record;
    delete first;
    fclose(load_file);
    sortedFile->Close();
  }
}

TEST_F(SortedDBFileTest, MOVE_FIRST_WITH_NO_RECORDS) {
  SortedDBFile *sortedFile = new SortedDBFile();
  fType t = sorted;
  if (sortedFile->Create("gtest.bin", t, (void *)si)) {
    // Just opened a file and moving first
    Record *temp_sorted_file_record = new Record();
    sortedFile->MoveFirst();
    EXPECT_EQ(0, sortedFile->GetNext(*temp_sorted_file_record));
    delete temp_sorted_file_record;
    sortedFile->Close();
  }
}

TEST_F(SortedDBFileTest, MOVE_FIRST_WITHOUT_CREATION) {
  SortedDBFile *sortedFile = new SortedDBFile();
  EXPECT_THROW(sortedFile->MoveFirst(), runtime_error);
}

TEST_F(SortedDBFileTest, LOAD_TEST) {}

TEST_F(SortedDBFileTest, GET_NEXT_WITH_PARAMETERS) {
  DBFile *sortedFile = new DBFile();
  if (sortedFile->Open("gtest.bin")) {
    const char cnf_string[] = "(l_orderkey = 11814)";
    YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
    yyparse();
    yy_delete_buffer(buffer);

    // grow the CNF expression from the parse tree
    CNF cnf;
    Record literal;
    cnf.GrowFromParseTree(final, &mySchema, literal);

    // print out the comparison to the screen
    cnf.Print();

    // temp3->Print(&mySchema);
    Record temp;
    EXPECT_TRUE(sortedFile->GetNext(temp, cnf, literal));
    sortedFile->Close();
  }
}

TEST_F(SortedDBFileTest, GET_NEXT_WITH_PARAMETERS_WHEN_A_RECORD_IS_NOT_FOUND) {
  DBFile *sortedFile = new DBFile();
  if (sortedFile->Open((const char *)"gtest_sorted_and_loaded.bin")) {
    const char cnf_string[] = "(l_orderkey = 8)";
    YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
    yyparse();
    yy_delete_buffer(buffer);

    // grow the CNF expression from the parse tree
    CNF cnf;
    Record literal;
    cnf.GrowFromParseTree(final, &mySchema, literal);

    // print out the comparison to the screen
    cnf.Print();

    // temp3->Print(&mySchema);
    Record temp;
    EXPECT_FALSE(sortedFile->GetNext(temp, cnf, literal));
    sortedFile->Close();
  }
}

TEST_F(SortedDBFileTest,
       GET_NEXT_WITH_PARAMETERS_WHEN_CNF_INCLUDES_OTHER_OPERATORS_AS_WELL) {
  DBFile *sortedFile = new DBFile();
  if (sortedFile->Open("gtest_sorted_and_loaded.bin")) {
    const char cnf_string[] = "(l_orderkey = 7) AND (l_partkey < 1000)";
    YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
    yyparse();
    yy_delete_buffer(buffer);

    // grow the CNF expression from the parse tree
    CNF cnf;
    Record literal;
    cnf.GrowFromParseTree(final, &mySchema, literal);

    // print out the comparison to the screen
    cnf.Print();

    Record temp;
    int status;
    while ((status = sortedFile->GetNext(temp, cnf, literal))) {
      temp.Print(&mySchema);
      EXPECT_TRUE(status);
    }

    sortedFile->Close();
  }
}

TEST_F(
    SortedDBFileTest,
    GET_NEXT_WITH_PARAMETERS_WITH_CNF_HAVING_ATTRIBUTES_OUT_OF_ORDER_WITH_FILE_SORT_ORDER) {
  DBFile *sortedFile = new DBFile();
  if (sortedFile->Open("gtest_sorted_and_loaded.bin")) {
    const char cnf_string[] =
        "(l_partkey = 1285) AND (l_orderkey = 3) AND (l_suppkey = 60)";
    YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
    yyparse();
    yy_delete_buffer(buffer);

    // grow the CNF expression from the parse tree
    CNF cnf;
    Record literal;
    cnf.GrowFromParseTree(final, &mySchema, literal);

    // print out the comparison to the screen
    cnf.Print();

    Record temp;
    EXPECT_TRUE(sortedFile->GetNext(temp, cnf, literal));
    temp.Print(&mySchema);
    sortedFile->Close();
  }
}

TEST_F(SortedDBFileTest,
       GET_NEXT_WITH_PARAMETERS_WHEN_QUERY_ORDERMAKER_IS_EMPTY) {
  DBFile *sortedFile = new DBFile();
  if (sortedFile->Open("gtest_sorted_and_loaded.bin")) {
    const char cnf_string[] = "(l_partkey = 1285) AND (l_suppkey = 60)";
    YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
    yyparse();
    yy_delete_buffer(buffer);

    // grow the CNF expression from the parse tree
    CNF cnf;
    Record literal;
    cnf.GrowFromParseTree(final, &mySchema, literal);

    // print out the comparison to the screen
    cnf.Print();

    Record temp;
    EXPECT_FALSE(sortedFile->GetNext(temp, cnf, literal));
    temp.Print(&mySchema);
    sortedFile->Close();
  }
}

TEST_F(SortedDBFileTest,
       GET_NEXT_WITH_PARAMETERS_WHEN_BINARY_SEARCH_IS_UNSUCCESSFUL) {
  DBFile *sortedFile = new DBFile();
  if (sortedFile->Open("gtest_sorted_and_loaded.bin")) {
    const char cnf_string[] =
        "(l_partkey = 1286) AND (l_orderkey = 3) AND (l_suppkey = 60)";
    YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
    yyparse();
    yy_delete_buffer(buffer);

    // grow the CNF expression from the parse tree
    CNF cnf;
    Record literal;
    cnf.GrowFromParseTree(final, &mySchema, literal);

    // print out the comparison to the screen
    cnf.Print();

    Record temp;
    EXPECT_FALSE(sortedFile->GetNext(temp, cnf, literal));
    temp.Print(&mySchema);
    sortedFile->Close();
  }
}

TEST_F(
    SortedDBFileTest,
    GET_NEXT_WITH_PARAMETERS_WHEN_REPEATEDLY_CALLED_WITHOUT_CALL_TO_ANY_OTHER_FUNCTION_IN_BETWEEN_WITH_BINARY_SEARCH_RETURNING_FALSE_PREVIOUSLY) {
}

TEST_F(
    SortedDBFileTest,
    GET_NEXT_WITH_PARAMETERS_WHEN_REPEATEDLY_CALLED_WITHOUT_CALL_TO_ANY_OTHER_FUNCTION_IN_BETWEEN_WITH_QUERY_ORDER_MAKER_BEING_EMPTY_PREVIOUSLY) {
}

TEST_F(
    SortedDBFileTest,
    GET_NEXT_WITH_PARAMETERS_WHEN_REPEATEDLY_CALLED_WITHOUT_CALL_TO_ANY_OTHER_FUNCTION_IN_BETWEEN) {
}

}  // namespace dbi