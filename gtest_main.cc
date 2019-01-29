#include "DBFile.h"
#include "gtest/gtest.h"

TEST(DBFile, CreateSuccess) {
  DBFile *db = new DBFile();
  fType t = heap;
  ASSERT_TRUE(db->Create("gtest.tbl", t, NULL));
}

TEST(DBFile, CreateFailure) {
  string a = "a";
  int MAX_FILE_NAME_SIZE = 255;
  string s;
  for (int i = 0; i < 255; i++) {
    s.append(a);
  }
  s += ".tbl";

  DBFile *db = new DBFile();
  fType t = heap;

  ASSERT_FALSE(db->Create(s.c_str(), t, NULL));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
