IDIR=include
CC=g++
GCC=gcc
CCOMPILEFLAGS=-std=c++11 -fprofile-arcs -ftest-coverage -fsanitize=address 

ODIR=obj
$(shell mkdir -p obj)

SOURCEDIR=src

LIBDIR=lib
LIBIDIR=lib/include
LIBFLAGS=-I$(LIBIDIR)

CFLAGS=-I$(IDIR) -I$(LIBIDIR)

tag = -i
ifdef linux
tag = -n
endif

$(ODIR)/%.o: $(SOURCEDIR)/%.cc 
	$(CC) $(CCOMPILEFLAGS) -c -o $@ $< $(CFLAGS)

$(ODIR)/y.tab.c: $(LIBDIR)/Parser.y
	yacc -d -o $@ $< 
	sed $(tag) -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" $@

$(ODIR)/y.tab.o: $(ODIR)/y.tab.c
	$(CC) -c -o $@ $< -I$(LIBIDIR)

OBJECTS = $(ODIR)/y.tab.o

$(ODIR)/lex.yy.c: $(LIBDIR)/Lexer.l
	lex  -o $@ $<

$(ODIR)/lex.yy.o: $(ODIR)/lex.yy.c
	$(GCC) -c -o $@ $< -I$(LIBIDIR)

OBJECTS += $(ODIR)/lex.yy.o

SOURCES=$(wildcard $(SOURCEDIR)/*.cc)
OBJECTS += $(patsubst $(SOURCEDIR)/%.cc, $(ODIR)/%.o, $(SOURCES))

TEST = clang++ 
TESTCOMPILEFLAGS=-fsanitize=address -fno-omit-frame-pointer -g -std=c++11 -stdlib=libc++ -fprofile-arcs -ftest-coverage
TESTSOURCESDIR=test/src
TESTIDIR=test/include
TESTFLAGS = -I$(IDIR) -I$(TESTIDIR) -I$(SOURCEDIR) -I$(LIBIDIR)
TESTODIR=obj/test
$(shell mkdir -p obj/test)

$(TESTODIR)/%.o: $(TESTSOURCESDIR)/%.cc
	$(TEST) $(TESTCOMPILEFLAGS) -c -o $@ $< $(TESTFLAGS)

TESTSOURCES=$(wildcard $(TESTSOURCESDIR)/*.cc)
TESTOBJECTS=$(patsubst $(TESTSOURCESDIR)/%.cc, $(TESTODIR)/%.o, $(TESTSOURCES))

all: $(OBJECTS)
alltest: $(OBJECTS) $(TESTOBJECTS)

BIN=bin
$(shell mkdir -p bin/data_files)
$(shell cp data_files/* bin/data_files)
$(shell cp data_files/catalog bin/)

TESTLINKFLAGS=-ll -lgtest -lpthread  

GIVENTEST=$(wildcard $(TESTSOURCESDIR)/test*)
GIVENTESTOBJECTS=$(patsubst $(TESTSOURCESDIR)/%.cc, $(TESTODIR)/%.o, $(GIVENTEST))
FILTEREDTESTOBJECTS=$(filter-out $(GIVENTESTOBJECTS), $(TESTOBJECTS))

MAIN=$(ODIR)/main.o
FILTERED_OBJECTS=$(filter-out $(MAIN), $(OBJECTS))

test: alltest
	$(TEST) $(TESTCOMPILEFLAGS) -o $(BIN)/test.out $(FILTERED_OBJECTS) $(FILTEREDTESTOBJECTS) $(TESTLINKFLAGS) $(TESTFLAGS)

clean:
	rm -f $(ODIR)/*.o 
	rm -f $(ODIR)/*.h 
	rm -f $(ODIR)/*.c 
	rm -f $(ODIR)/*.c-e
	rm -f $(ODIR)/*.gcno
	rm -rf $(ODIR)/test
	rm -f *~ core 
	rm -f $(INCDIR)/*~ 
	rm -rf bin/