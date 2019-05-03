#ifndef DBFILE_H
#define DBFILE_H
#include <string>
#include "GenericDBFile.h"

class DBFile {
 private:
  GenericDBFile *dbFile;

 public:
  DBFile();
  ~DBFile();

  // Create a DBFile of given type
  // If file_type is sorted startup will have structure of SortInfo type
  // If the file already exists, it is overridded.
  int Create(const char *fpath, fType file_type, void *startup);
  // Open an already created file
  int Open(const char *fpath);
  // Close the file and handle transition of related variables
  int Close();
  // Bulk load content into DBFile from loadpath having a specific structure
  // with myschema
  void Load(Schema &myschema, const char *loadpath);
  // Go to the beginning of the DBFile
  void MoveFirst();
  // Add a record to DBFile
  void Add(Record &addme);
  // Get the next record from DBFile
  int GetNext(Record &fetchme);
  // Get the next record from DBFile satisfying some condition
  int GetNext(Record &fetchme, CNF &cnf, Record &literal);

 private:
  std::string GetMetaDataFileName(
      const char *file_path); /* Create a name of the metadata file based on the
                                 file opened */

  bool CheckIfCorrectFileType(fType type);
  GenericDBFile *GetDBFileInstance(fType type);
  fType GetFileType(const char *fpath);
};
#endif