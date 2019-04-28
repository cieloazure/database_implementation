#ifndef TEST1_H
#define TEST1_H
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include "DBFile.h"
#include "Record.h"
using namespace std;

extern "C" {
int yyparse(void);  // defined in y.tab.c
}

extern struct AndList *final;

class relation {
 private:
  const char *rname;
  const char *prefix;
  char rpath[100];
  Schema *R;

 public:
  relation(const char *_name, Schema *_schema, const char *_prefix)
      : rname(_name), R(_schema), prefix(_prefix) {
    sprintf(rpath, "%s%s.bin", prefix, rname);
  }
  const char *name() { return rname; }
  const char *path() { return rpath; }
  Schema *schema() { return R; }
  void info() {
    cout << " relation info\n";
    cout << "\t name: " << name() << endl;
    cout << "\t path: " << path() << endl;
  }

  void get_cnf(CNF &cnf_pred, Record &literal) {
    cout << " Enter CNF predicate (when done press ctrl-D):\n\t";
    if (yyparse() != 0) {
      std::cout << "Can't parse your CNF.\n";
      exit(1);
    }
    cnf_pred.GrowFromParseTree(final, schema(),
                               literal);  // constructs CNF predicate
  }
};

char *supplier = (char *)"supplier";
char *partsupp = (char *)"partsupp";
char *part = (char *)"part";
char *nation = (char *)"nation";
char *customer = (char *)"customer";
char *orders = (char *)"orders";
char *region = (char *)"region";
char *lineitem = (char *)"lineitem";

relation *s, *p, *ps, *n, *li, *r, *o, *c;

void setup(char *catalog_path, const char *dbfile_dir, const char *tpch_dir) {
  cout << " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
  cout << " catalog location: \t" << catalog_path << endl;
  cout << " tpch files dir: \t" << tpch_dir << endl;
  cout << " heap files dir: \t" << dbfile_dir << endl;
  cout << " \n\n";

  s = new relation(supplier, new Schema(catalog_path, supplier), dbfile_dir);
  ps = new relation(partsupp, new Schema(catalog_path, partsupp), dbfile_dir);
  p = new relation(part, new Schema(catalog_path, part), dbfile_dir);
  n = new relation(nation, new Schema(catalog_path, nation), dbfile_dir);
  li = new relation(lineitem, new Schema(catalog_path, lineitem), dbfile_dir);
  r = new relation(region, new Schema(catalog_path, region), dbfile_dir);
  o = new relation(orders, new Schema(catalog_path, orders), dbfile_dir);
  c = new relation(customer, new Schema(catalog_path, customer), dbfile_dir);
}

void cleanup() { delete s, p, ps, n, li, r, o, c; }

#endif
