#include "Schema.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>

int Schema ::Find(char *attName) {
  std::string attNameStr(attName);
  if (IsQualifiedAtt(attNameStr)) {
    std::pair<std::string, std::string> relAttPair =
        SplitQualifiedAtt(attNameStr);
    char *relName = (char *)relAttPair.first.c_str();
    attName = (char *)relAttPair.second.c_str();
    char *joinNodeName = "join_schema";
    if (strcmp(fileName, joinNodeName) != 0 && strcmp(fileName, relName) != 0) {
      return -1;
    }
  }

  for (int i = 0; i < numAtts; i++) {
    if (!strcmp(attName, myAtts[i].name)) {
      return i;
    }
  }

  // if we made it here, the attribute was not found
  return -1;
}

std::pair<int, int> Schema ::FindWithStatus(char *attName) {
  std::string attNameStr(attName);
  bool join = false;
  if (IsQualifiedAtt(attNameStr)) {
    std::pair<std::string, std::string> relAttPair =
        SplitQualifiedAtt(attNameStr);
    char *relName = (char *)relAttPair.first.c_str();
    attName = (char *)relAttPair.second.c_str();
    char *joinNodeName = "join_schema";
    if (strcmp(fileName, joinNodeName) == 0) {
      join = true;
    }
    if (strcmp(fileName, joinNodeName) != 0 && strcmp(fileName, relName) != 0) {
      return std::make_pair(-1, join);
    }
  }

  for (int i = 0; i < numAtts; i++) {
    if (!strcmp(attName, myAtts[i].name)) {
      return std::make_pair(i, join);
    }
  }

  // if we made it here, the attribute was not found
  return std::make_pair(-1, join);
}

Type Schema ::FindType(char *attName) {
  std::string attNameStr(attName);
  if (IsQualifiedAtt(attNameStr)) {
    std::pair<std::string, std::string> relAttPair =
        SplitQualifiedAtt(attNameStr);
    char *relName = (char *)relAttPair.first.c_str();
    attName = (char *)relAttPair.second.c_str();
    char *joinNodeName = "join_schema";
    if (strcmp(fileName, joinNodeName) != 0 && strcmp(fileName, relName) != 0) {
      return Int;
    }
  }

  for (int i = 0; i < numAtts; i++) {
    if (!strcmp(attName, myAtts[i].name)) {
      return myAtts[i].myType;
    }
  }

  // if we made it here, the attribute was not found
  return Int;
}

int Schema ::GetNumAtts() { return numAtts; }

Attribute *Schema ::GetAtts() { return myAtts; }

void Schema ::Init(char *fpath, int num_atts, Attribute *atts) {
  fileName = strdup(fpath);
  numAtts = num_atts;
  myAtts = new Attribute[numAtts];
  for (int i = 0; i < numAtts; i++) {
    if (atts[i].myType == Int) {
      myAtts[i].myType = Int;
    } else if (atts[i].myType == Double) {
      myAtts[i].myType = Double;
    } else if (atts[i].myType == String) {
      myAtts[i].myType = String;
    } else {
      cout << "Bad attribute type for " << atts[i].myType << "\n";
      delete[] myAtts;
      exit(1);
    }
    myAtts[i].name = strdup(atts[i].name);
  }
}

Schema::Schema(char *relName) { fileName = strdup(relName); }

Schema ::Schema(char *fpath, int num_atts, Attribute *atts) {
  Init(fpath, num_atts, atts);
}

Schema ::Schema(char *fpath, OrderMaker *o, Schema *s) {
  int n = o->numAtts;
  Attribute atts[n];
  for (int i = 0; i < n; i++) {
    atts[i] = s->myAtts[o->whichAtts[i]];
  }
  Init(fpath, o->numAtts, atts);
}

Schema ::Schema(char *fName, char *relName) {
  FILE *foo = fopen(fName, "r");

  // this is enough space to hold any tokens
  char space[200];

  fscanf(foo, "%s", space);
  int totscans = 1;

  // see if the file starts with the correct keyword
  if (strcmp(space, "BEGIN")) {
    cout << "Unfortunately, this does not seem to be a schema file.\n";
    exit(1);
  }

  while (1) {
    // check to see if this is the one we want
    fscanf(foo, "%s", space);
    totscans++;
    if (strcmp(space, relName)) {
      // it is not, so suck up everything to past the BEGIN
      while (1) {
        // suck up another token
        if (fscanf(foo, "%s", space) == EOF) {
          cerr << "Could not find the schema for the specified relation.\n";
          exit(1);
        }

        totscans++;
        if (!strcmp(space, "BEGIN")) {
          break;
        }
      }

      // otherwise, got the correct file!!
    } else {
      break;
    }
  }

  // suck in the file name
  fscanf(foo, "%s", space);
  totscans++;
  fileName = strdup(space);

  // count the number of attributes specified
  numAtts = 0;
  while (1) {
    fscanf(foo, "%s", space);
    if (!strcmp(space, "END")) {
      break;
    } else {
      fscanf(foo, "%s", space);
      numAtts++;
    }
  }

  // now actually load up the schema
  fclose(foo);
  foo = fopen(fName, "r");

  // go past any un-needed info
  for (int i = 0; i < totscans; i++) {
    fscanf(foo, "%s", space);
  }

  // and load up the schema
  myAtts = new Attribute[numAtts];
  for (int i = 0; i < numAtts; i++) {
    // read in the attribute name
    fscanf(foo, "%s", space);
    myAtts[i].name = strdup(space);

    // read in the attribute type
    fscanf(foo, "%s", space);
    if (!strcmp(space, "Int")) {
      myAtts[i].myType = Int;
    } else if (!strcmp(space, "Double")) {
      myAtts[i].myType = Double;
    } else if (!strcmp(space, "String")) {
      myAtts[i].myType = String;
    } else {
      cout << "Bad attribute type for " << myAtts[i].name << "\n";
      exit(1);
    }
  }

  fclose(foo);
  fileName = strdup(relName);
}

Schema ::~Schema() {
  delete[] myAtts;
  myAtts = 0;
}

void Schema::Copy(Schema *other) {
  numAtts = other->numAtts;
  myAtts = new Attribute[numAtts];
  for (int i = 0; i < 0; i++) {
    myAtts[i].name = strdup(other->myAtts[i].name);
    switch (other->myAtts[i].myType) {
      case Int:
        myAtts[i].myType = Int;
        break;
      case Double:
        myAtts[i].myType = Double;
        break;
      case String:
        myAtts[i].myType = String;
        break;
    }
  }
}
Schema ::Schema(char *fName, Schema *other) {
  fileName = strdup(fName);
  Copy(other);
}

void Schema ::AddAttribute(Attribute newAtt) {
  numAtts++;
  Attribute *newAtts = new Attribute[numAtts];
  newAtts[0].name = strdup(newAtt.name);
  switch (newAtt.myType) {
    case Int:
      newAtts[0].myType = Int;
      break;
    case Double:
      newAtts[0].myType = Double;
      break;
    case String:
      newAtts[0].myType = String;
      break;
  }

  for (int i = 1; i < numAtts; i++) {
    newAtts[i].name = strdup(myAtts[i - 1].name);
    switch (myAtts[i - 1].myType) {
      case Int:
        newAtts[i].myType = Int;
        break;
      case Double:
        newAtts[i].myType = Double;
        break;
      case String:
        newAtts[i].myType = String;
        break;
    }
  }

  myAtts = newAtts;
}

void Schema::GetDifference(Schema *s, OrderMaker o, int *diff) {
  int newNumAtts = s->GetNumAtts() - o.GetNumAtts();

  auto isAttributeInOrderMaker = [&o](int att) -> bool {
    for (int i = 0; i < o.numAtts; i++) {
      if (o.whichAtts[i] == att) {
        return true;
      }
    }
    return false;
  };

  int newAttsIndex = 0;
  for (int i = 0; i < s->GetNumAtts(); i++) {
    if (!isAttributeInOrderMaker(i)) {
      diff[newAttsIndex] = i;
      newAttsIndex++;
    }
  }
}
void Schema ::DifferenceWithOrderMaker(OrderMaker o, int *diff) {
  GetDifference(this, o, diff);
}

Schema ::Schema(char *fName, Schema *s1, Schema *s2, OrderMaker *s2OrderMaker) {
  fileName = strdup(fName);

  // Initialize sizes
  int diffSize = s2->GetNumAtts() - s2OrderMaker->GetNumAtts();
  numAtts = s1->GetNumAtts() + diffSize;
  myAtts = new Attribute[numAtts];

  // Copy s1
  for (int i = 0; i < s1->GetNumAtts(); i++) {
    myAtts[i].name = strdup(s1->myAtts[i].name);
    switch (s1->myAtts[i].myType) {
      case Int:
        myAtts[i].myType = Int;
        break;
      case Double:
        myAtts[i].myType = Double;
        break;
      case String:
        myAtts[i].myType = String;
        break;
    }
  }

  int *diff = new int[diffSize];
  GetDifference(s2, *s2OrderMaker, diff);

  auto uniqueAttribute = [&diff, &diffSize](int att) -> bool {
    for (int i = 0; i < diffSize; i++) {
      if (diff[i] == att) {
        return true;
      }
    }
    return false;
  };

  // Copy s2 only those contained in diff
  int contIndex = s1->GetNumAtts();
  for (int i = 0; i < s2->GetNumAtts(); i++) {
    if (uniqueAttribute(i)) {
      myAtts[contIndex].name = strdup(s2->myAtts[i].name);
      switch (s2->myAtts[i].myType) {
        case Int:
          myAtts[contIndex].myType = Int;
          break;
        case Double:
          myAtts[contIndex].myType = Double;
          break;
        case String:
          myAtts[contIndex].myType = String;
          break;
      }
      contIndex++;
    }
  }
}

Schema ::Schema(char *fName, Schema *s1, Schema *s2) {
  fileName = strdup(fName);

  // Initialize sizes
  numAtts = s1->GetNumAtts() + s2->GetNumAtts();
  myAtts = new Attribute[numAtts];

  // Copy s1
  int i = 0;
  for (; i < s1->GetNumAtts(); i++) {
    myAtts[i].name = strdup(s1->myAtts[i].name);
    switch (s1->myAtts[i].myType) {
      case Int:
        myAtts[i].myType = Int;
        break;
      case Double:
        myAtts[i].myType = Double;
        break;
      case String:
        myAtts[i].myType = String;
        break;
    }
  }

  // Copy s2
  for (int j = 0; j < s2->GetNumAtts(); j++) {
    myAtts[i].name = strdup(s2->myAtts[j].name);
    switch (s2->myAtts[j].myType) {
      case Int:
        myAtts[i].myType = Int;
        break;
      case Double:
        myAtts[i].myType = Double;
        break;
      case String:
        myAtts[i].myType = String;
        break;
    }
    i++;
  }
}

bool Schema::IsQualifiedAtt(std::string value) {
  return value.find('.', 0) != std::string::npos;
}

std::pair<std::string, std::string> Schema::SplitQualifiedAtt(
    std::string value) {
  size_t idx = value.find('.', 0);
  std::string rel;
  std::string att;
  if (idx == std::string::npos) {
    att = value;
  } else {
    rel = value.substr(0, idx);
    att = value.substr(idx + 1, value.length());
  }
  std::pair<std::string, std::string> retPair;
  retPair.first = rel;
  retPair.second = att;
  return retPair;
}

void Schema::Print(std::string prefixtabs) {
  std::cout << prefixtabs << "Schema name:" << fileName << std::endl;
  std::cout << prefixtabs << "Schema number of atts:" << numAtts << std::endl;
  std::cout << prefixtabs << "Schema atts details:" << std::endl;
  std::cout << prefixtabs << "\t\tIndex\t\t\tName\t\t\tType\n";
  for (int i = 0; i < numAtts; i++) {
    std::cout << prefixtabs << "\t\t" << i << "\t\t\t" << myAtts[i].name
              << "\t\t\t";
    switch (myAtts[i].myType) {
      case Int:
        cout << "INT\n";
        break;
      case Double:
        cout << "DOUBLE\n";
        break;
      case String:
        cout << "STRING\n";
        break;
    }
  }
}

Schema::Schema(char *fName, Schema *other, std::vector<int> keepMe) {
  numAtts = keepMe.size();
  fileName = strdup(fName);
  myAtts = new Attribute[numAtts];

  Attribute *hisAtts = other->GetAtts();
  int myIdx = 0;
  for (int idx : keepMe) {
    Attribute hisAtt = hisAtts[idx];
    myAtts[myIdx].name = strdup(hisAtt.name);
    myAtts[myIdx].myType = hisAtt.myType;
    myIdx++;
  }
}
