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
    if (sortedFile->Create("preloaded_gtest.bin", type, (void *)si)) {
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

  bool checkSortedOrder(char *fpath) {
    DBFile dbFile;
    dbFile.Open(fpath);
    dbFile.MoveFirst();
    ComparisonEngine ceng;

    int err = 0;
    int i = 0;
    Record rec[2];
    Record *last = new Record();
    bool lastEmpty = true;
    Record *prev = new Record();
    bool prevEmpty = true;

    while (dbFile.GetNext(rec[i % 2])) {
      if (!lastEmpty) {
        prev->Copy(last);
        prevEmpty = false;
      }
      if (lastEmpty) {
        lastEmpty = false;
      }
      last->Copy(&rec[i % 2]);

      if (!prevEmpty && !lastEmpty) {
        if (ceng.Compare(prev, last, o) == 1) {
          err++;
        }
      }
      i++;
    }
    dbFile.Close();

    cout << " consumer: removed " << i << " recs from the pipe\n";
    cout << " consumer: " << (i - err) << " recs out of " << i
         << " recs in sorted order \n";

    if (err) {
      return false;
    } else {
      return true;
    }
  }

  void GetNextParamsRepeatedQuery(DBFile *sortedFile, string cnf_string) {
    Schema mySchema("catalog", "lineitem");

    YY_BUFFER_STATE buffer = yy_scan_string(cnf_string.c_str());
    yyparse();
    yy_delete_buffer(buffer);

    // grow the CNF expression from the parse tree
    CNF cnf;
    Record literal;
    cnf.GrowFromParseTree(final, &mySchema, literal);

    // print out the comparison to the screen
    cnf.Print();

    Record *temp = new Record();
    EXPECT_TRUE(sortedFile->GetNext(*temp, cnf, literal));
  }
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
  } else {
    FAIL();
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
  } else {
    FAIL();
  }
  delete sortFile;
}

TEST_F(SortedDBFileTest, MOVE_FIRST_WHEN_GET_NEXT_HAVE_BEEN_CALLED_BEFORE) {
  SortedDBFile *sortedFile = new SortedDBFile();
  fType t = sorted;
  if (sortedFile->Open("preloaded_gtest.bin")) {
    const char *loadpath = "data_files/lineitem.tbl";

    Schema mySchema("catalog", "lineitem");

    FILE *load_file = fopen(loadpath, "r");
    Record *temp_source_file_record = new Record();
    Record *temp_sorted_file_record = new Record();
    Record *first = new Record();
    for (int i = 0; i < 10; i++) {
      temp_source_file_record->SuckNextRecord(&mySchema, load_file);
      // For line item the first record in sorted is the 4 record in the table
      // file
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
  } else {
    FAIL();
  }
}

TEST_F(SortedDBFileTest, MOVE_FIRST_ON_FIRST_RECORD) {
  SortedDBFile *sortedFile = new SortedDBFile();
  fType t = sorted;
  if (sortedFile->Open("preloaded_gtest.bin")) {
    const char *loadpath = "data_files/lineitem.tbl";

    Schema mySchema("catalog", "lineitem");

    FILE *load_file = fopen(loadpath, "r");
    Record *temp_source_file_record = new Record();
    Record *temp_sorted_file_record = new Record();
    Record *first = new Record();
    for (int i = 0; i < 10; i++) {
      temp_source_file_record->SuckNextRecord(&mySchema, load_file);
      if (i == 3) {
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
  } else {
    FAIL();
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
  } else {
    FAIL();
  }
}

TEST_F(SortedDBFileTest, MOVE_FIRST_WITHOUT_CREATION) {
  SortedDBFile *sortedFile = new SortedDBFile();
  EXPECT_THROW(sortedFile->MoveFirst(), runtime_error);
}

TEST_F(SortedDBFileTest, LOAD_WITH_NO_OPEN_DBFILE) {
  SortedDBFile *sortedFile = new SortedDBFile();
  Schema mySchema("catalog", "lineitem");
  const char *tpch_dir = "data_files/lineitem.tbl";
  EXPECT_THROW(sortedFile->Load(mySchema, tpch_dir), runtime_error);
}

TEST_F(SortedDBFileTest, LOAD_WITH_NO_EXISTING_TABLEFILE) {
  SortedDBFile *sortedFile = new SortedDBFile();
  fType t = sorted;
  if (sortedFile->Create("gtest.bin", t, (void *)si) == 1) {
    Schema mySchema("catalog", "lineitem");
    const char *tpch_dir = "data_files/does-not-exists.tbl";
    EXPECT_THROW(sortedFile->Load(mySchema, tpch_dir), runtime_error);
    sortedFile->Close();
  } else {
    FAIL();
  }
}

TEST_F(SortedDBFileTest, LOAD_SUCCESS) {
  SortedDBFile *sortedFile = new SortedDBFile();
  if (sortedFile->Create("gtest.bin", sorted, (void *)si) == 1) {
    Schema mySchema("catalog", "lineitem");
    const char *tpch_dir = "data_files/lineitem.tbl";
    FILE *f = fopen(tpch_dir, "r");
    Record temp;
    int expected_count = 0;
    while (temp.SuckNextRecord(&mySchema, f)) {
      expected_count++;
    }

    sortedFile->Load(mySchema, tpch_dir);

    int actual_count = 0;
    while (sortedFile->GetNext(temp)) {
      actual_count++;
    }

    EXPECT_EQ(expected_count, actual_count);
    sortedFile->Close();

    EXPECT_TRUE(checkSortedOrder("gtest.bin"));
  } else {
    FAIL();
  }
}

TEST_F(SortedDBFileTest, GET_NEXT_WHEN_THERE_ARE_NO_RECORDS_IN_THE_FILE) {
  SortedDBFile *sortedFile = new SortedDBFile();
  fType t = sorted;
  if (sortedFile->Create("gtest.bin", t, (void *)si)) {
    Record temp;
    EXPECT_FALSE(sortedFile->GetNext(temp));
    sortedFile->Close();
  } else {
    FAIL();
  }
}

TEST_F(SortedDBFileTest, GET_NEXT_WHEN_THERE_ARE_RECORDS_IN_THE_FILE) {
  SortedDBFile *sortedFile = new SortedDBFile();
  if (sortedFile->Open("preloaded_gtest.bin")) {
    Record temp;
    EXPECT_TRUE(sortedFile->GetNext(temp));
    sortedFile->Close();
  } else {
    FAIL();
  }
}

TEST_F(SortedDBFileTest, GET_NEXT_WHEN_YOU_REACH_THE_END_OF_THE_FILE) {
  SortedDBFile *sortedFile = new SortedDBFile();
  if (sortedFile->Open("preloaded_gtest.bin")) {
    sortedFile->MoveFirst();
    int LINE_ITEM_RECORDS = 60175;
    Record temp;
    int count = 0;
    while (count < LINE_ITEM_RECORDS) {
      sortedFile->GetNext(temp);
      count++;
    }

    EXPECT_FALSE(sortedFile->GetNext(temp));
    sortedFile->Close();
  } else {
    FAIL();
  }
}

TEST_F(SortedDBFileTest, GET_NEXT_WITH_PARAMETERS) {
  DBFile *sortedFile = new DBFile();
  if (sortedFile->Open("preloaded_gtest.bin")) {
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
  } else {
    FAIL();
  }
}

TEST_F(SortedDBFileTest, GET_NEXT_WITH_PARAMETERS_WHEN_A_RECORD_IS_NOT_FOUND) {
  DBFile *sortedFile = new DBFile();
  if (sortedFile->Open("preloaded_gtest.bin")) {
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
  if (sortedFile->Open("preloaded_gtest.bin")) {
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
  if (sortedFile->Open("preloaded_gtest.bin")) {
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
    sortedFile->Close();
  }
}

TEST_F(
    SortedDBFileTest,
    GET_NEXT_WITH_PARAMETERS_WHEN_QUERY_ORDERMAKER_IS_EMPTY_BUT_RECORD_EXISTS) {
  DBFile *sortedFile = new DBFile();
  if (sortedFile->Open("preloaded_gtest.bin")) {
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
    EXPECT_TRUE(sortedFile->GetNext(temp, cnf, literal));
    sortedFile->Close();
  }
}

TEST_F(
    SortedDBFileTest,
    GET_NEXT_WITH_PARAMETERS_WHEN_QUERY_ORDERMAKER_IS_EMPTY_BUT_RECORD_DOES_NOT_EXISTS) {
  DBFile *sortedFile = new DBFile();
  if (sortedFile->Open("preloaded_gtest.bin")) {
    const char cnf_string[] = "(l_partkey = 1286) AND (l_suppkey = 60)";
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
    sortedFile->Close();
  }
}

TEST_F(SortedDBFileTest,
       GET_NEXT_WITH_PARAMETERS_WHEN_BINARY_SEARCH_IS_UNSUCCESSFUL) {
  DBFile *sortedFile = new DBFile();
  if (sortedFile->Open("preloaded_gtest.bin")) {
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
    sortedFile->Close();
  }
}

TEST_F(
    SortedDBFileTest,
    GET_NEXT_WITH_PARAMETERS_WHEN_REPEATEDLY_CALLED_WITHOUT_CALL_TO_ANY_OTHER_FUNCTION_IN_BETWEEN_WITH_BINARY_SEARCH_RETURNING_A_RECORD_PREVIOUSLY) {
  DBFile *sortedFile = new DBFile();
  if (sortedFile->Open("preloaded_gtest.bin")) {
    const char cnf_string[] = "(l_orderkey = 60000)";
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
    int count = 0;
    while (sortedFile->GetNext(temp, cnf, literal)) {
      count++;
    }
    sortedFile->Close();
    EXPECT_EQ(6, count);
  } else {
    FAIL();
  }
}

TEST_F(
    SortedDBFileTest,
    GET_NEXT_WITH_PARAMETERS_WHEN_REPEATEDLY_CALLED_WITHOUT_CALL_TO_ANY_OTHER_FUNCTION_IN_BETWEEN_WITH_BINARY_SEARCH_RETURNING_FALSE_PREVIOUSLY) {
  DBFile *sortedFile = new DBFile();
  if (sortedFile->Open("preloaded_gtest.bin")) {
    const char cnf_string[] = "(l_orderkey = 60001)";
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
    int count = 0;
    int loop_counter = 0;
    while (loop_counter < 10) {
      if (sortedFile->GetNext(temp, cnf, literal)) {
        count++;
      }
      loop_counter++;
    }
    sortedFile->Close();
    EXPECT_EQ(0, count);
  } else {
    FAIL();
  }
}

TEST_F(SortedDBFileTest,
       GET_NEXT_WITH_PARAMETERS_RECORD_EXITS_BUT_IT_IS_BEFORE_CURRENT_RECORD) {
  DBFile *sortedFile = new DBFile();
  if (sortedFile->Open("preloaded_gtest.bin")) {
    const char cnf_string[] = "(l_orderkey = 59969)";
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

    const char cnf_string_2[] = "(l_orderkey = 1)";
    YY_BUFFER_STATE buffer_2 = yy_scan_string(cnf_string_2);
    yyparse();
    yy_delete_buffer(buffer_2);

    // grow the CNF expression from the parse tree
    cnf.GrowFromParseTree(final, &mySchema, literal);

    // print out the comparison to the screen
    cnf.Print();

    EXPECT_FALSE(sortedFile->GetNext(temp, cnf, literal));
    sortedFile->Close();

  } else {
    FAIL();
  }
}

TEST_F(SortedDBFileTest,
       GET_NEXT_WITH_PARAMETERS_QUERYING_VARIOUS_PARTS_OF_THE_FILE) {
  DBFile *sortedFile = new DBFile();
  if (sortedFile->Open("preloaded_gtest.bin")) {
    // Bottom of the file
    string cnf_string("(l_orderkey = 60000)");
    GetNextParamsRepeatedQuery(sortedFile, cnf_string);

    sortedFile->MoveFirst();

    // Top of the file
    cnf_string = "(l_orderkey = 1)";
    GetNextParamsRepeatedQuery(sortedFile, cnf_string);

    sortedFile->Close();
  } else {
    FAIL();
  }
}

// TEST_F(SortedDBFileTest,
//        GET_NEXT_WITH_PARAMETERS_RECORD_EXITS_AND_IT_IS_AFTER_CURRENT_RECORD)
//        {
//   DBFile *sortedFile = new DBFile();
//   if (sortedFile->Open("preloaded_gtest.bin")) {
//     const char cnf_string[] = "(l_orderkey = 59969)";
//     YY_BUFFER_STATE buffer = yy_scan_string(cnf_string);
//     yyparse();
//     yy_delete_buffer(buffer);

//     // grow the CNF expression from the parse tree
//     CNF cnf;
//     Record literal;
//     cnf.GrowFromParseTree(final, &mySchema, literal);

//     // print out the comparison to the screen
//     cnf.Print();

//     Record temp;
//     EXPECT_TRUE(sortedFile->GetNext(temp, cnf, literal));

//     const char cnf_string_2[] = "(l_orderkey = 60000)";
//     YY_BUFFER_STATE buffer_2 = yy_scan_string(cnf_string_2);
//     yyparse();
//     yy_delete_buffer(buffer_2);

//     // grow the CNF expression from the parse tree
//     cnf.GrowFromParseTree(final, &mySchema, literal);

//     // print out the comparison to the screen
//     cnf.Print();

//     EXPECT_TRUE(sortedFile->GetNext(temp, cnf, literal));
//     sortedFile->Close();

//   } else {
//     FAIL();
//   }
// }

// TEST_F(SortedDBFileTest,
//        GET_NEXT_WITH_PARAMETERS_WITH_CHANGING_CNF)
//        {}
}  // namespace dbi