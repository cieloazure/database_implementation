#ifndef DBFILE_H
#define DBFILE_H
#include "GenericDBFile.h"

class DBFile {
 private:
  GenericDBFile *dbFile;

 public:
  DBFile();
  ~DBFile();

  int Create(const char *fpath, fType file_type, void *startup);
  int Open(const char *fpath);
  int Close();
  void Load(Schema &myschema, const char *loadpath);
  void MoveFirst();
  void Add(Record &addme);
  int GetNext(Record &fetchme);
  int GetNext(Record &fetchme, CNF &cnf, Record &literal);

 private:
  string GetMetaDataFileName(
      const char *file_path); /* Create a name of the metadata file based on the
                                 file opened */

  bool CheckIfCorrectFileType(fType type);
  GenericDBFile *GetDBFileInstance(fType type);
  fType GetFileType(const char *fpath);
};
#endif