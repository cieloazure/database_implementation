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

  std::unordered_map<std::string, Schema *> relNameToSchema;
  relNameToSchema["lineitem"] = &lineitem;
  relNameToSchema["orders"] = &orders;
  relNameToSchema["supplier"] = &supplier;
  relNameToSchema["partsupp"] = &partsupp;
  relNameToSchema["customer"] = &customer;
  relNameToSchema["part"] = &part;
  relNameToSchema["region"] = &region;
  relNameToSchema["nation"] = &nation;

  // Load Statistics
  char *relName[] = {"supplier", "partsupp", "lineitem", "order",
                     "customer", "nation",   "region",   "part"};
  int tuples[] = {100, 8000, 6001215, 1500000, 150000, 25, 5, 2000};
  Statistics s;
  for (int i = 0; i < 8; i++) {
    s.AddRel(relName[i], tuples[i]);
  }
  s.AddAtt(relName[0], "s_suppkey", 10000);
  s.AddAtt(relName[0], "s_nationkey", 25);

  s.AddAtt(relName[1], "ps_suppkey", 10000);
  s.AddAtt(relName[1], "ps_partkey", 200000);

  s.AddAtt(relName[2], "l_returnflag", 3);
  s.AddAtt(relName[2], "l_discount", 11);
  s.AddAtt(relName[2], "l_shipmode", 7);
  s.AddAtt(relName[2], "l_orderkey", 1500000);
  s.AddAtt(relName[2], "l_shipinstruct", 4);
  s.AddAtt(relName[2], "l_shipmode", 7);
  s.AddAtt(relName[2], "l_partkey", 200000);

  s.AddAtt(relName[3], "o_custkey", 150000);
  s.AddAtt(relName[3], "o_orderdate", -1);

  s.AddAtt(relName[4], "c_custkey", 150000);
  s.AddAtt(relName[4], "c_nationkey", 25);
  s.AddAtt(relName[4], "c_mktsegment", 5);

  s.AddAtt(relName[5], "n_nationkey", 25);
  s.AddAtt(relName[5], "n_regionkey", 5);

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

  relNameToSchema["R"] = &R;
  relNameToSchema["S"] = &S;
  relNameToSchema["T"] = &T;
  relNameToSchema["U"] = &U;

  // Initialize query optimizer
  QueryOptimizer optimizer(&s, &relNameToSchema);

  // Get query from user
  cout << "Enter Query: ";
  std::string query;
  std::string line;
  while (getline(cin, line)) {
    if (line == "\n") {
      break;
    }
    query += line;
  }
  cout << "Query:" << query << std::endl;
  // Run optimization to get QueryPlan
  optimizer.GetOptimizedPlan(query);
}