#CC = clang++ -fsanitize=address -O1 -fno-omit-frame-pointer -g 
CC = g++ -O2 -Wno-deprecated -fprofile-arcs -ftest-coverage 
TEST = g++ -std=c++11 -stdlib=libc++ -fprofile-arcs -ftest-coverage 

tag = -i

ifdef linux
tag = -n
endif

gtest_main.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o DBFileTest.o y.tab.o lex.yy.o gtest_main.o 
	$(TEST) -o gtest_main.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o DBFileTest.o y.tab.o lex.yy.o gtest_main.o -ll -lgtest -lpthread  

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test.o -ll
	
main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o main.o 
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o main.o -ll
	
test.o: test.cc
	$(CC) -g -c test.cc

main.o: main.cc
	$(CC) -g -c main.cc
	
Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag)  -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

gtest_main.o: gtest_main.cc
	$(TEST) -g -c gtest_main.cc

DBFileTest.o: DBFileTest.cc
	$(TEST) -g -c DBFileTest.cc

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
