#ifndef DEFS_H
#define DEFS_H

#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072
#define verbose true

#include <unistd.h>

unsigned long long getTotalSystemMemory() {
  long page_size = sysconf(_SC_PAGE_SIZE);
  long pages = sysconf(_SC_PHYS_PAGES);
  unsigned long long mem = pages * page_size;
  long max_pages = mem / PAGE_SIZE;
  return max_pages;
}

#define MAX_PAGES_THAT_CAN_FIT_IN_MEMORY getTotalSystemMemory()

enum Target { Left, Right, Literal };
enum CompOperator { LessThan, GreaterThan, Equals };
enum Type { Int, Double, String };

unsigned int Random_Generate();

#endif