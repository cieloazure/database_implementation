#ifndef SORTEDDBFILE_H
#define SORTEDDBFILE_H

#include <string.h>
#include <fstream>
#include <iostream>
#include "BigQ.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "File.h"
#include "GenericDBFile.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"

class SortedDBFile : public GenericDBFile {
 private:
  const char *file_path; /* The destination of the file */
  fType type;            /* Type of file (enum) */
  File *persistent_file; /* File instance in which pages are to be stored to */
  Page *read_buffer;     /* Page buffer to only read pages from the sorted file,
                            which unlike heapDbfile is not used to writing to the
                            file, that job is done by bigq */
  File *new_persistent_file; /* File instance in which pages are to be stored
                                for merging logic */
  BigQ *bigq;
  int metadata_file_descriptor; /* Metadata file which has information about the
                                   file */
  int new_metadata_file_descriptor; /* Metadata file which has information about
                                   the file */
  off_t current_write_page_index;   /* The page to which next record is to be
                                     added to */
  off_t current_read_page_index;    /* The page from which next record is to be
                                     read*/
  int current_read_page_offset;     /* The record # in the page to be read */
  bool dirty; /* A flag describing whether the bigq is to be merged with one on
                 disk or no */
  modeType mode; /* The mode of the file right now i.e. reading or writing */
  bool is_open;  /* A Flag variable to indicate whether a file is open or not */

  /* SortedDBFile specific member variables */
  int runLength;
  OrderMaker *sortOrder;
  int count;
  Pipe *input;
  Pipe *output;
  bool cachedGetNextFlag;
  OrderMaker queryOrderMaker;
  OrderMaker literalOrderMaker;
  bool merging;

  //   HeapDBFile *mergeFile;

 public:
  SortedDBFile();
  ~SortedDBFile();
  int Create(const char *fpath, fType file_type, void *startup);
  int Open(const char *fpath);
  int Close();
  void Load(Schema &myschema, const char *loadpath);
  void MoveFirst();
  void Add(Record &addme);
  int GetNext(Record &fetchme);
  int GetNext(Record &fetchme, CNF &cnf, Record &literal);

 private:
  char *GetMetaDataFileName(
      const char *file_path); /* Create a name of the metadata file based on the
                                 file opened */
  void CheckIfFilePresent();  /* Check if a file is opened */
  bool CheckIfCorrectFileType(fType type);
  bool CheckIfFileNameIsValid(const char *file_name);
  int BinarySearchFile(off_t *foundPage, int *foundOffset,
                       File *persistent_file, OrderMaker *queryOrderMaker,
                       OrderMaker *literalOrderMaker, Record *literal,
                       off_t page_offset, int record_offset);
  int BinarySearchPage(Page *buffer, OrderMaker *queryOrderMaker,
                       OrderMaker *literalOrderMaker, Record *literal);

  void CopyBufferToPage(Page *from, Page *to);

  /* New Merge file function */
  int MergeBigqRecords();
};
#endif