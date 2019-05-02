
#ifndef SCHEMA_H
#define SCHEMA_H

#include <stdio.h>

#include "Record.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "Schema.h"

struct att_pair {
  char *name;
  Type type;
};
struct Attribute {
  char *name;
  Type myType;
};

class OrderMaker;
class Schema {
  // gives the attributes in the schema
  int numAtts;
  Attribute *myAtts;

  // gives the physical location of the binary file storing the relation
  char *fileName;

  friend class Record;

  void Init(char *fName, int num_atts, Attribute *atts);

  void Copy(Schema *s);

  void GetDifference(Schema *s, OrderMaker o, int *diff);

 public:
  // gets the set of attributes, but be careful with this, since it leads
  // to aliasing!!!
  Attribute *GetAtts();

  // returns the number of attributes
  int GetNumAtts();

  // this finds the position of the specified attribute in the schema
  // returns a -1 if the attribute is not present in the schema
  int Find(char *attName);

  std::pair<int, int> FindWithStatus(char *attName);

  // this finds the type of the given attribute
  Type FindType(char *attName);

  Schema(char *relName);

  // this reads the specification for the schema in from a file
  Schema(char *fName, char *relName);

  // this composes a schema instance in-memory
  Schema(char *fName, int num_atts, Attribute *atts);

  // this composes a schema instance in-memory using ordermaker
  // Useful for group by operation when we need to get a schema based on
  // grouping attributes
  Schema(char *fName, OrderMaker *o, Schema *s);

  // Create a schema by combining two schemas with an ordermaker(join)
  Schema(char *fName, Schema *s1, Schema *s2, OrderMaker *s2OrderMaker);

  // Create a schema by combining two schemas without an ordermaker(cartesian
  // product)
  Schema(char *fName, Schema *s1, Schema *s2);

  // this composes a schema from another schema
  // copy constructor
  Schema(char *fName, Schema *other);

  ~Schema();

  void AddAttribute(Attribute a);

  void DifferenceWithOrderMaker(OrderMaker o, int *diff);

  bool IsQualifiedAtt(std::string value);
  std::pair<std::string, std::string> SplitQualifiedAtt(std::string value);

  void Print(std::string prefix);
};

#endif
