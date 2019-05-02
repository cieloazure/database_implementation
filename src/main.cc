#include <iostream>
#include "QueryOptimizer.h"
#include "Schema.h"
#include "Statistics.h"

using namespace std;

int main() {
  // Load Schemas
  Schema lineitem("catalog", "lineitem");
  Schema orders("catalog", "orders");
  Schema supplier("catalog", "supplier");
  Schema partsupp("catalog", "partsupp");
  Schema customer("catalog", "customer");
  Schema part("catalog", "part");
  Schema region("catalog", "region");
  Schema nation("catalog", "nation");

  std::unordered_map<std::string, RelationTuple *> relNameToRelTuple;

  DBFile *dbFile7 = new DBFile();
  if (!dbFile7->Create("region", heap, NULL)) {
    std::cout << "Error!" << std::endl;
    exit(1);
  }
  dbFile7->Load(region, "data_files/1G/region.tbl");
  // dbFile7->Close();
  // if (!dbFile7->Open("region")) {
  //   std::cout << "Error!" << std::endl;
  //   exit(1);
  // }
  // DBFile dbFile7_1;
  // if (!dbFile7_1.Open("region")) {
  //   std::cout << "Error!" << std::endl;
  //   exit(1);
  // }
  relNameToRelTuple["region"] = new RelationTuple(&region, dbFile7);

  DBFile *dbFile = new DBFile();
  dbFile->Create("lineitem", heap, NULL);
  dbFile->Load(lineitem, "data_files/1G/lineitem.tbl");
  relNameToRelTuple["lineitem"] = new RelationTuple(&lineitem, dbFile);

  DBFile *dbFile2 = new DBFile();
  dbFile2->Create("orders", heap, NULL);
  dbFile2->Load(orders, "data_files/1G/orders.tbl");
  relNameToRelTuple["orders"] = new RelationTuple(&orders, dbFile2);

  DBFile *dbFile3 = new DBFile();
  dbFile3->Create("supplier", heap, NULL);
  dbFile3->Load(supplier, "data_files/1G/supplier.tbl");
  relNameToRelTuple["supplier"] = new RelationTuple(&supplier, dbFile3);

  DBFile *dbFile4 = new DBFile();
  dbFile4->Create("partsupp", heap, NULL);
  dbFile4->Load(partsupp, "data_files/1G/partsupp.tbl");
  relNameToRelTuple["partsupp"] = new RelationTuple(&partsupp, dbFile4);

  DBFile *dbFile5 = new DBFile();
  dbFile5->Create("customer", heap, NULL);
  dbFile5->Load(customer, "data_files/1G/customer.tbl");
  relNameToRelTuple["customer"] = new RelationTuple(&customer, dbFile5);

  DBFile *dbFile6 = new DBFile();
  dbFile6->Create("part", heap, NULL);
  dbFile6->Load(part, "data_files/1G/part.tbl");
  relNameToRelTuple["part"] = new RelationTuple(&part, dbFile6);

  DBFile *dbFile8 = new DBFile();
  dbFile8->Create("nation", heap, NULL);
  dbFile8->Load(nation, "data_files/1G/nation.tbl");
  relNameToRelTuple["nation"] = new RelationTuple(&nation, dbFile8);

  // Load Statistics
  char *relName[] = {"supplier", "partsupp", "lineitem", "orders",
                     "customer", "nation",   "region",   "part"};
  int tuples[] = {100, 8000, 6001215, 1500000, 150000, 25, 5, 2000};
  Statistics s;
  for (int i = 0; i < 8; i++) {
    s.AddRel(relName[i], tuples[i]);
  }
  s.AddAtt(relName[0], "s_suppkey", 10000);
  s.AddAtt(relName[0], "s_nationkey", 25);
  s.AddAtt(relName[0], "s_name", 10000);

  s.AddAtt(relName[1], "ps_suppkey", 10000);
  s.AddAtt(relName[1], "ps_partkey", 200000);

  s.AddAtt(relName[2], "l_returnflag", 3);
  s.AddAtt(relName[2], "l_discount", 11);
  s.AddAtt(relName[2], "l_shipmode", 7);
  s.AddAtt(relName[2], "l_orderkey", 1500000);
  s.AddAtt(relName[2], "l_shipinstruct", 4);
  s.AddAtt(relName[2], "l_shipmode", 7);
  s.AddAtt(relName[2], "l_partkey", 200000);
  s.AddAtt(relName[2], "l_suppkey", -1);

  s.AddAtt(relName[3], "o_custkey", 150000);
  s.AddAtt(relName[3], "o_orderdate", -1);
  s.AddAtt(relName[3], "o_orderkey", -1);

  s.AddAtt(relName[4], "c_custkey", 150000);
  s.AddAtt(relName[4], "c_nationkey", 25);
  s.AddAtt(relName[4], "c_mktsegment", 5);

  s.AddAtt(relName[5], "n_nationkey", 25);
  s.AddAtt(relName[5], "n_regionkey", 5);
  s.AddAtt(relName[5], "n_name", -1);

  s.AddAtt(relName[6], "r_regionkey", 5);
  s.AddAtt(relName[6], "r_name", 5);

  s.AddAtt(relName[7], "p_partkey", 200000);
  s.AddAtt(relName[7], "p_size", 50);
  s.AddAtt(relName[7], "p_name", 199996);
  s.AddAtt(relName[7], "p_container", 40);

  char *relName2[] = {"R", "S", "T", "U"};
  s.AddRel(relName2[0], 1000);
  s.AddAtt(relName2[0], "a", 100);
  s.AddAtt(relName2[0], "b", 200);
  s.AddRel(relName2[1], 1000);
  s.AddAtt(relName2[1], "b", 100);
  s.AddAtt(relName2[1], "c", 500);
  s.AddRel(relName2[2], 1000);
  s.AddAtt(relName2[2], "c", 20);
  s.AddAtt(relName2[2], "d", 50);
  s.AddRel(relName2[3], 1000);
  s.AddAtt(relName2[3], "a", 50);
  s.AddAtt(relName2[3], "d", 1000);

  Attribute IA = {(char *)"a", Int};
  Attribute IB = {(char *)"b", Int};
  Attribute IC = {(char *)"c", Int};
  Attribute ID = {(char *)"d", Int};

  Attribute rAtts[] = {IA, IB};
  Schema R("R", 2, rAtts);
  Attribute s1Atts[] = {IB, IC};
  Schema S("S", 2, s1Atts);
  Attribute tAtts[] = {IC, ID};
  Schema T("T", 2, tAtts);
  Attribute uAtts[] = {IA, ID};
  Schema U("U", 2, uAtts);

  relNameToRelTuple["R"] = new RelationTuple(&R);
  relNameToRelTuple["S"] = new RelationTuple(&S);
  relNameToRelTuple["T"] = new RelationTuple(&T);
  relNameToRelTuple["U"] = new RelationTuple(&U);

  // Initialize query optimizer
  QueryOptimizer optimizer(&s, &relNameToRelTuple);

  // Get query from user
  std::cout << "Welcome to Database Implementation Demo v0.1" << std::endl;
  std::cout << std::endl;
  std::cout << "Commands end with `;`" << std::endl;
  std::cout << std::endl;
  std::cout << "Type `help` for help" << std::endl;
  std::cout << "Type `Ctrl-D` or `Ctrl-C` to exit" << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;
  while (true) {
    cout << "dbi>  ";
    std::string query;
    std::string line;
    // std::cin.flush();
    bool enter = false;
    while (true) {
      if (enter) {
        query += "  ";
        std::cout << "  ->  ";
      }
      std::getline(std::cin, line);
      if (query == "" && line == "") {
        break;
      }
      if (std::cin.eof()) {
        break;
      }
      query += line;
      if (query[query.size() - 1] == ';') {
        break;
      }
      enter = true;
    }
    if (std::cin.eof()) {
      std::cout << "Bye!" << std::endl;
      break;
    }
    if (query.size() > 0) {
      std::cout << "--------------DEBUG MODE--------------" << std::endl;
      std::cout << "Given Query:" << query << std::endl;
      // Run optimization to get QueryPlan
      cout << "Query Plan:" << std::endl;
      QueryPlan *qp = optimizer.GetOptimizedPlan(query);
      qp->Print();
      qp->SetOutput(StdOut);
      qp->Execute();
    }
  }
  // std::string line;
  // while (true) {
  //   cout << "Enter:";
  //   getline(cin, line);
  //   if (cin.eof()) {
  //     cout << "bye";
  //     break;
  //   }
  //   cout << line;
  // }
}
