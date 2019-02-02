#include "DBFile.h"
#include "gtest/gtest.h"

extern "C" {
typedef struct yy_buffer_state *YY_BUFFER_STATE;
int yyparse(void);  // defined in y.tab.c
YY_BUFFER_STATE yy_scan_string(const char *str);
void yy_delete_buffer(YY_BUFFER_STATE buffer);
}

extern struct AndList *final;

namespace dbi {

// The fixture for testing class Foo.
class DBFileTest : public ::testing::Test
{
protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  DBFileTest()
  {
    // You can do set-up work for each test here.
  }

  ~DBFileTest() override
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
    remove("gtest.bin");
    remove("gtest.header");
  }

  // Objects declared here can be used by all tests in the test case for Foo.
};

/* Unit tests */
TEST_F(DBFileTest, CreateSuccess) {
  DBFile *heapFile = new DBFile();
  fType t = heap;
  EXPECT_TRUE(heapFile->Create("gtest.bin", t, NULL));
  delete heapFile;
}

TEST_F(DBFileTest, CreateFailureWhenFileNameIsTooLarge)
{
  string a = "a";
  int MAX_FILE_NAME_SIZE = 255;
  string s;
  for (int i = 0; i < MAX_FILE_NAME_SIZE; i++)
  {
    s.append(a);
  }
  s += ".bin";

  DBFile *heapFile = new DBFile();
  fType t = heap;
  EXPECT_FALSE(heapFile->Create(s.c_str(), t, NULL));
  delete heapFile;
}

TEST_F(DBFileTest, CreateFailureWhenFileNameIsAnEmptyString)
{
  DBFile *heapFile = new DBFile();
  fType t = heap;
  EXPECT_FALSE(heapFile->Create("", t, NULL));
  delete heapFile;
}

TEST_F(DBFileTest, CreateFailureWhenFileIsInvalid)
{
  DBFile *heapFile = new DBFile();
  EXPECT_FALSE(heapFile->Create("gtest.bin", (fType)4, NULL));
  delete heapFile;
}

TEST_F(DBFileTest, OpenSuccess)
{
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, NULL))
  {
    heapFile->Close();
    EXPECT_TRUE(heapFile->Open("gtest.bin"));
  }
  delete heapFile;
}

TEST_F(DBFileTest, OpenFailureMissingMetadataFile)
{
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, NULL))
  {
    heapFile->Close();
    remove("gtest.header");
    EXPECT_FALSE(heapFile->Open("gtest.bin"));
  }
  delete heapFile;
}

TEST_F(DBFileTest, OpenFailureOnInvalidFileTypeInMetadataFile)
{
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, NULL))
  {
    heapFile->Close();
    int fd = open("gtest.header", O_RDWR, S_IRUSR | S_IWUSR);
    lseek(fd, 0, SEEK_SET);
    int invalid_value = 4;
    write(fd, &invalid_value, sizeof(fType));
    EXPECT_FALSE(heapFile->Open("gtest.bin"));
  }
  delete heapFile;
}

TEST_F(DBFileTest, OpenFailureWhenAFileDoesNotExists)
{
  DBFile *heapFile = new DBFile();
  EXPECT_FALSE(heapFile->Open("gtest2.bin"));
  delete heapFile;
}

TEST_F(DBFileTest, OpenFailureWhenAFileNameIsInvalid)
{
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, NULL))
  {
    heapFile->Close();
    EXPECT_FALSE(heapFile->Open(""));
  }
  delete heapFile;
}

TEST_F(DBFileTest, MoveFirstWhenGetNextHaveBeenCalledBefore)
{
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, NULL))
  {
    const char *loadpath = "data_files/lineitem.tbl";

    Schema mySchema("catalog", "lineitem");
    heapFile->Load(mySchema, loadpath);

    FILE *load_file = fopen(loadpath, "r");
    Record *temp_source_file_record = new Record();
    Record *temp_heap_file_record = new Record();
    Record *first = new Record();
    for (int i = 0; i < 3; i++)
    {
      temp_source_file_record->SuckNextRecord(&mySchema, load_file);
      if (i == 0)
      {
        first->Copy(temp_source_file_record);
      }
      heapFile->GetNext(*temp_heap_file_record);
    }

    heapFile->MoveFirst();
    heapFile->GetNext(*temp_heap_file_record);

    ComparisonEngine comp;
    OrderMaker order(&mySchema);

    temp_heap_file_record->Print(&mySchema);
    first->Print(&mySchema);
    EXPECT_TRUE(comp.Compare(temp_heap_file_record, first, &order) == 0);

    delete temp_source_file_record;
    delete temp_heap_file_record;
    delete first;
    fclose(load_file);
    heapFile->Close();
  }
}

TEST_F(DBFileTest, MoveFirstOnFirstRecord)
{
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, NULL))
  {
    const char *loadpath = "data_files/lineitem.tbl";

    Schema mySchema("catalog", "lineitem");
    heapFile->Load(mySchema, loadpath);

    FILE *load_file = fopen(loadpath, "r");
    Record *temp_source_file_record = new Record();
    Record *temp_heap_file_record = new Record();
    Record *first = new Record();
    for (int i = 0; i < 1; i++)
    {
      temp_source_file_record->SuckNextRecord(&mySchema, load_file);
      if (i == 0)
      {
        first->Copy(temp_source_file_record);
      }
      // No Get Next is being called
    }

    // Just opened a file and moving first
    heapFile->MoveFirst();
    heapFile->GetNext(*temp_heap_file_record);

    ComparisonEngine comp;
    OrderMaker order(&mySchema);

    temp_heap_file_record->Print(&mySchema);
    first->Print(&mySchema);
    EXPECT_TRUE(comp.Compare(temp_heap_file_record, first, &order) == 0);

    delete temp_source_file_record;
    delete temp_heap_file_record;
    delete first;
    fclose(load_file);
    heapFile->Close();
  }
}

TEST_F(DBFileTest, MoveFirstWithNoRecords)
{
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, NULL))
  {
    // Just opened a file and moving first
    Record *temp_heap_file_record = new Record();
    heapFile->MoveFirst();
    EXPECT_EQ(0, heapFile->GetNext(*temp_heap_file_record));
    delete temp_heap_file_record;
    heapFile->Close();
  }
}

TEST_F(DBFileTest, MoveFirstWithoutCreation)
{
  DBFile *heapFile = new DBFile();
  EXPECT_THROW(heapFile->MoveFirst(), runtime_error);
}

TEST_F(DBFileTest, SuccessfullyAddRecordIntoBuffer)
{
  DBFile *heapFile = new DBFile();
  Record *temp = new Record();
  cout << "1";
  fType t = heap;
  if (heapFile->Create("gtest.tbl", t, NULL))
  {
    const char *loadpath = "data_files/lineitem.tbl";
    FILE *load_file = fopen(loadpath, "r");
    Schema mySchema("catalog", "lineitem");

    int num_recs = heapFile->GetNumRecsInBuffer();

    temp->SuckNextRecord(&mySchema, load_file);
    cout << "2";
    heapFile->Add(*temp);

    EXPECT_GT(heapFile->GetNumRecsInBuffer(), num_recs);

    delete temp;
    heapFile->Close();
    fclose(load_file);
  }
}

TEST_F(DBFileTest, LoadWithNoOpenDBFile)
{
  DBFile *heapFile = new DBFile();
  Schema mySchema("catalog", "lineitem");
  const char *tpch_dir = "data_files/lineitem.tbl";
  EXPECT_THROW(heapFile->Load(mySchema, tpch_dir), runtime_error);
}

TEST_F(DBFileTest, LoadWithNoExistingTableFile)
{
  DBFile *heapFile = new DBFile();
  if (heapFile->Create("gtest.bin", heap, NULL) == 1) {
    Schema mySchema("catalog", "lineitem");
    const char *tpch_dir = "data_files/does-not-exists.tbl";
    EXPECT_THROW(heapFile->Load(mySchema, tpch_dir), runtime_error);
    heapFile->Close();
  }
}

TEST_F(DBFileTest, LoadSuccess)
{
  DBFile *heapFile = new DBFile();
  if (heapFile->Create("gtest.bin", heap, NULL) == 1)
  {
    Schema mySchema("catalog", "lineitem");
    const char *tpch_dir = "data_files/lineitem.tbl";
    FILE *f = fopen(tpch_dir, "r");
    Record temp;
    int expected_count = 0;
    while (temp.SuckNextRecord(&mySchema, f))
    {
      expected_count++;
    }

    heapFile->Load(mySchema, tpch_dir);

    int actual_count = 0;
    while (heapFile->GetNext(temp))
    {
      actual_count++;
    }

    EXPECT_EQ(expected_count, actual_count);
    heapFile->Close();
  }
}

TEST_F(DBFileTest, AddWhenItJustAddsToBufferWithoutCreatingANewPage) {
  DBFile *heapFile = new DBFile();
  if (heapFile->Create("gtest.bin", heap, NULL))
  {
    Schema mySchema("catalog", "lineitem");
    const char *tpch_dir = "data_files/lineitem.tbl";
    FILE *f = fopen(tpch_dir, "r");
    Record temp;
    temp.SuckNextRecord(&mySchema, f);
    heapFile->Add(temp);
    int actual_count = 0;
    while (heapFile->GetNext(temp))
    {
      actual_count++;
    }

    EXPECT_EQ(1, actual_count);
    heapFile->Close();
  }
}

TEST_F(DBFileTest, AddWhenFirstFlushTakesPlace) {
  DBFile *heapFile = new DBFile();
  if (heapFile->Create("gtest.bin", heap, NULL))
  {
    Schema mySchema("catalog", "lineitem");
    const char *tpch_dir = "data_files/lineitem.tbl";
    FILE *f = fopen(tpch_dir, "r");
    Record temp;

    int expected_count = 0;
    while (temp.SuckNextRecord(&mySchema, f))
    {
      if (heapFile->WillBufferBeFull(temp))
      {
        break;
      }
      else
      {
        expected_count++;
        heapFile->Add(temp);
      }
    }

    cout << expected_count << endl;
    cout << "Buffer is almost full now! Adding another record to flush the "
            "buffer"
         << endl;
    heapFile->Add(temp);

    int actual_count = 0;
    while (heapFile->GetNext(temp))
    {
      actual_count++;
    }

    EXPECT_EQ(expected_count + 1, actual_count);
    int actual_pages = 2;
    // Expecting an additional empty page
    EXPECT_EQ(actual_pages + 1, heapFile->GetNumPagesInFile());
    heapFile->Close();
  }
}

TEST_F(DBFileTest, AddWhenSubsequentFlushesTakesPlace) {
  DBFile *heapFile = new DBFile();
  if (heapFile->Create("gtest.bin", heap, NULL))
  {
    Schema mySchema("catalog", "lineitem");
    const char *tpch_dir = "data_files/lineitem.tbl";
    FILE *f = fopen(tpch_dir, "r");
    Record temp;

    int expected_count = 0;
    int flush_count = 0;
    while (temp.SuckNextRecord(&mySchema, f))
    {
      if (heapFile->WillBufferBeFull(temp))
      {
        flush_count++;
        if (flush_count == 2)
        {
          break;
        }
        else
        {
          expected_count++;
          heapFile->Add(temp);
          cout << "Buffer is full for the first time! But continuing to test "
                  "the subsequent flush"
               << endl;
        }
      }
      else
      {
        expected_count++;
        heapFile->Add(temp);
      }
    }

    cout << expected_count << endl;
    cout << "Buffer is almost full again! Adding another record to flush the "
            "buffer"
         << endl;
    heapFile->Add(temp);

    int actual_count = 0;
    while (heapFile->GetNext(temp))
    {
      actual_count++;
    }

    EXPECT_EQ(expected_count + 1, actual_count);
    int actual_pages = 3;
    // Expecting an additional empty page
    EXPECT_EQ(actual_pages + 1, heapFile->GetNumPagesInFile());
    heapFile->Close();
  }
}

TEST_F(DBFileTest, AddWhenAnExistingPartiallyFilledPageIsUtilized)
{
  DBFile *heapFile = new DBFile();
  if (heapFile->Create("gtest.bin", heap, NULL))
  {
    Schema mySchema("catalog", "lineitem");
    const char *tpch_dir = "data_files/lineitem.tbl";
    FILE *f = fopen(tpch_dir, "r");

    Record temp;
    int expected_count = 0;
    while (temp.SuckNextRecord(&mySchema, f) && expected_count <= 10)
    {
      expected_count++;
      heapFile->Add(temp);
    }

    Record temp2;
    cout << "Adding the partially filled page back to the file and reading"
         << endl;
    heapFile->GetNext(temp2);
    cout << "And now writing again to get the partially filled page back and "
            "verify no new page was added or created"
         << endl;

    if (temp.SuckNextRecord(&mySchema, f))
    {
      heapFile->Add(temp);
    }

    heapFile->MoveFirst();
    int actual_count = 0;
    while (heapFile->GetNext(temp2))
    {
      actual_count++;
    }

    EXPECT_EQ(expected_count + 1, actual_count);
    int actual_pages = 1;
    // Expecting an additional empty page
    EXPECT_EQ(actual_pages + 1, heapFile->GetNumPagesInFile());
    heapFile->Close();
  }
}

TEST_F(DBFileTest, CloseNormal)
{
  DBFile *heapFile = new DBFile();
  fType t = heap;

  if (heapFile->Create("gtest.bin", t, NULL))
  {
    heapFile->Close();
  }

  EXPECT_EQ(heapFile->GetCurrentReadPageIndex(), -1);
  EXPECT_EQ(heapFile->GetCurrentWritePageIndex(), -1);
}

TEST_F(DBFileTest, CloseWhenFlushBuffer)
{
  DBFile *heapFile = new DBFile();
  fType t = heap;
  if (heapFile->Create("gtest.bin", t, NULL))
  {
    Schema mySchema("catalog", "lineitem");
    const char *tpch_dir = "data_files/lineitem.tbl";
    FILE *f = fopen(tpch_dir, "r");
    Record temp;

    int expected_count = 0;
    while (temp.SuckNextRecord(&mySchema, f))
    {
      if (heapFile->WillBufferBeFull(temp))
      {
        break;
      }
      else
      {
        expected_count++;
        heapFile->Add(temp);
      }
    }
  }

  int count = heapFile->GetNumRecsInBuffer();
  heapFile->Close();

  EXPECT_EQ(heapFile->GetNumRecsInBuffer(), 0);
  EXPECT_GT(count, heapFile->GetNumRecsInBuffer());
}

TEST_F(DBFileTest, GetNextWhenThereAreNoRecordsInTheFile) {
  DBFile *heapFile = new DBFile();
  if (heapFile->Create("gtest.bin", heap, NULL)) {
    Record temp;
    EXPECT_FALSE(heapFile->GetNext(temp));
    heapFile->Close();
  }
}

TEST_F(DBFileTest, GetNextWhenThereAreRecordsInTheFile) {
  DBFile *heapFile = new DBFile();
  if (heapFile->Create("gtest.bin", heap, NULL)) {
    const char *loadpath = "data_files/lineitem.tbl";

    Schema mySchema("catalog", "lineitem");
    heapFile->Load(mySchema, loadpath);
    Record temp;
    EXPECT_TRUE(heapFile->GetNext(temp));
    heapFile->Close();
  }
}

TEST_F(DBFileTest, GetNextWhenYouReachTheEndOfTheFile) {
  DBFile *heapFile = new DBFile();
  if (heapFile->Create("gtest.bin", heap, NULL)) {
    const char *loadpath = "data_files/lineitem.tbl";

    Schema mySchema("catalog", "lineitem");
    heapFile->Load(mySchema, loadpath);
    int LINE_ITEM_RECORDS = 60175;
    Record temp;
    int count = 0;
    while (count < LINE_ITEM_RECORDS) {
      heapFile->GetNext(temp);
      count++;
    }

    EXPECT_FALSE(heapFile->GetNext(temp));
    heapFile->Close();
  }
}

TEST_F(DBFileTest,
       GetNextWhenThereAreNoMoreRecordsLeftInTheBufferAndThePageRollsOver) {
  DBFile *heapFile = new DBFile();
  if (heapFile->Create("gtest.bin", heap, NULL)) {
    const char *loadpath = "data_files/lineitem.tbl";

    Schema mySchema("catalog", "lineitem");
    heapFile->Load(mySchema, loadpath);
    Record temp;
    heapFile->GetNext(temp);
    int count = 0;
    int number_of_records_in_buffer = heapFile->GetNumRecsInBuffer();
    while (count < number_of_records_in_buffer) {
      heapFile->GetNext(temp);
      count++;
    }

    EXPECT_TRUE(heapFile->GetNext(temp));
    heapFile->Close();
  }
}

TEST_F(DBFileTest, GetNextWithParametersGivenCondition) {
  DBFile *heapFile = new DBFile();
  if (heapFile->Create("gtest.bin", heap, NULL)) {
    const char *loadpath = "data_files/lineitem.tbl";

    Schema mySchema("catalog", "lineitem");
    heapFile->Load(mySchema, loadpath);

    const char cnf_string[] = "(l_orderkey > 25) AND (l_orderkey < 40)";
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
    EXPECT_TRUE(heapFile->GetNext(temp, cnf, literal));
    heapFile->Close();
  }
}

TEST_F(DBFileTest, GetNextWithParametersORCondition) {
  DBFile *heapFile = new DBFile();
  if (heapFile->Create("gtest.bin", heap, NULL)) {
    const char *loadpath = "data_files/lineitem.tbl";

    Schema mySchema("catalog", "lineitem");
    heapFile->Load(mySchema, loadpath);

    const char cnf_string[] = "(l_orderkey < 10  OR l_orderkey > 40)";
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
    EXPECT_TRUE(heapFile->GetNext(temp, cnf, literal));
    heapFile->Close();
  }
}

TEST_F(DBFileTest, GetNextWithParametersGivenConditionGetAllRecords) {
  DBFile *heapFile = new DBFile();
  if (heapFile->Create("gtest.bin", heap, NULL)) {
    const char *loadpath = "data_files/lineitem.tbl";

    Schema mySchema("catalog", "lineitem");
    heapFile->Load(mySchema, loadpath);

    const char cnf_string[] = "(l_orderkey > 25) AND (l_orderkey < 40)";
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
    int actual_count = 0;
    while (heapFile->GetNext(temp, cnf, literal)) {
      actual_count++;
    };
    int expected_count = 30;
    EXPECT_EQ(expected_count, actual_count);
    heapFile->Close();
  }
}

TEST_F(DBFileTest, CloseWithoutOpening) {
  DBFile *heapFile = new DBFile();
  EXPECT_FALSE(heapFile->Close());
}

// Integration tests
TEST_F(DBFileTest, AddAfterASeriesOfGetNext) {}
TEST_F(DBFileTest, GetNextAfterASeriesOfAdd) {}
TEST_F(DBFileTest, AddAfterClosingTheFileAndOpeningItAgain) {}
TEST_F(DBFileTest, GetNextAfterClosingTheFileAndOpeningItAgain) {}
}  // namespace dbi
