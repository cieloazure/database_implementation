#include <limits.h>
#include "sample1.h"
#include "gtest/gtest.h"
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

