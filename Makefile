#CC = clang++ -fsanitize=address -O1 -fno-omit-frame-pointer -g 
# CC = g++ -O2 -Wno-deprecated -std=c++11 -fprofile-arcs -ftest-coverage 
CC = g++ -std=c++11 -fprofile-arcs -ftest-coverage -fsanitize=address
TEST = clang++ -fsanitize=address -fno-omit-frame-pointer -g -std=c++11 -stdlib=libc++ -fprofile-arcs -ftest-coverage


tag = -i

ifdef linux
tag = -n
endif

gtest_main.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o SortedDBFile.o DBFile.o Pipe.o HeapDBFileTest.o SortedDBFileTest.o TwoWayListTest.o FileTest.o BigQTest.o ComparisonTest.o y.tab.o lex.yy.o gtest_main.o 
	$(TEST) -o gtest_main.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o SortedDBFile.o DBFile.o Pipe.o HeapDBFileTest.o SortedDBFileTest.o TwoWayListTest.o FileTest.o BigQTest.o ComparisonTest.o y.tab.o lex.yy.o gtest_main.o -ll -lgtest -lpthread  

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o y.tab.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o y.tab.o lex.yy.o test.o -ll
	
test2_1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o HeapDBFile.o Pipe.o y.tab.o lex.yy.o test2_1.o
	$(CC) -o test2_1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o HeapDBFile.o Pipe.o y.tab.o lex.yy.o test2_1.o -ll -lpthread

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o Pipe.o BigQ.o y.tab.o lex.yy.o main.o 
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapDBFile.o Pipe.o BigQ.o y.tab.o lex.yy.o main.o -ll

test2_1.o: test2_1.cc
	$(CC) -g -c test2_1.cc
	
test.o: test.cc
	$(CC) -g -c test.cc

main.o: main.cc
	$(CC) -g -c main.cc
	
Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
HeapDBFile.o: HeapDBFile.cc
	$(CC) -g -c HeapDBFile.cc

SortedDBFile.o: SortedDBFile.cc
	$(CC) -g -c SortedDBFile.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

gtest_main.o: gtest_main.cc
	$(TEST) -g -c gtest_main.cc

HeapDBFileTest.o: HeapDBFileTest.cc
	$(TEST) -g -c HeapDBFileTest.cc

SortedDBFileTest.o: SortedDBFileTest.cc
	$(TEST) -g -c SortedDBFileTest.cc

TwoWayListTest.o: TwoWayListTest.cc
	$(TEST) -g -c TwoWayListTest.cc

FileTest.o: FileTest.cc
	$(TEST) -g -c FileTest.cc

BigQTest.o: BigQTest.cc
	$(TEST) -g -c BigQTest.cc

ComparisonTest.o: ComparisonTest.cc
	$(TEST) -g -c ComparisonTest.cc
clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
	rm -f *.header
	rm -f *.tbl
	rm -f *.gcda
	rm -f *.gcov
	rm -f *.gcno

test_clean:
	rm -f *.gcda
	rm -f *.gcov
	rm -f *.gcno