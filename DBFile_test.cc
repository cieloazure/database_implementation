#include "DBFile.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"
#include <fstream>
#include <iostream>
#include <stdlib.h>

#include "gtest/gtest.h"

namespace {
TEST(DBFileTest, CreateHeapFile) {
  DBFile *heapFile = new DBFile();
  fType f = heap;
  EXPECT_EQ(1, heapFile->Create("test.tbl", heap, NULL));
}
} // namespace
