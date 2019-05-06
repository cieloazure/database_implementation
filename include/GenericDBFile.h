#ifndef GENERICDBFILE_H
#define GENERICDBFILE_H
#include "Record.h"

typedef enum { heap, sorted, tree } fType;
typedef enum { reading, writing, idle } modeType;

class GenericDBFile {
  // Abstract base class of HeapDBFile and SortedDBFile
  // IF a new type of DBFile emerges it will have to inherit this class
 public:
  virtual int Create(const char *fpath, fType file_type, void *startup) = 0;
  virtual int Open(const char *fpath) = 0;
  virtual void Add(Record &addme) = 0;
  virtual int GetNext(Record &fetchme) = 0;
  virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;
  virtual int Close() = 0;
  virtual void MoveFirst() = 0;
  virtual void Load(Schema &myschema, const char *loadpath) = 0;
};

#endif