#include <iostream>
#include <stdlib.h>
#include <fstream>
#include "TwoWayList.h"
#include "Record.h"
#include "File.h"
#include "Schema.h"
#include "DBFile.h"

#include "gtest/gtest.h"

namespace{
  TEST(DBFileTest, CreateHeapFile){
      DBFile* heapFile = new DBFile();
      fType f = heap;
      EXPECT_EQ(1, heapFile -> Create("test.tbl", heap, NULL));
  }
}

