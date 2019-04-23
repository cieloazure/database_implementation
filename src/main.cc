
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <list>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include "ParseTree.h"
#include "Optimizer.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

int main () {
	char *fileName = "Statistics.txt";
	Optimizer o;
	yyparse();
	o.Read(fileName);
	o.ReadParserDatastructures();
	
}


