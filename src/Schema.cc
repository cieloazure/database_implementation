#include "Schema.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>

int Schema ::Find(char *attName) {
  for (int i = 0; i < numAtts; i++) {
    if (!strcmp(attName, myAtts[i].name)) {
      return i;
    }
  }

  // if we made it here, the attribute was not found
  return -1;
}

Type Schema ::FindType(char *attName) {
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
}

Schema ::~Schema() {
  delete[] myAtts;
  myAtts = 0;
}

Schema ::Schema(Schema *other) {
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