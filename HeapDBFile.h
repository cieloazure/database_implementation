#ifndef HEAPDBFILE_H
#define HEAPDBFILE_H

#include <string.h>
#include <fstream>
#include <iostream>
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "GenericDBFile.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"

// typedef enum { heap, sorted, tree } fType;
// typedef enum { reading, writing, idle } modeType;

class HeapDBFile : public GenericDBFile {
 private:
  const char *file_path; /* The destination of the file */
  fType type;            /* Type of file (enum) */
  File *persistent_file; /* File instance in which pages are to be stored to */
  int metadata_file_descriptor; /* Metadata file which has information about the
                                   file */

  off_t current_write_page_index; /* The page to which next record is to be
                                   added to */
  off_t current_read_page_index;  /* The page from which next record is to be
                                   read*/
  int current_read_page_offset;   /* The record # in the page to be read */

  Page *buffer; /* A buffer to manage read/write operations */

  bool dirty; /* A flag describing whether the buffer is to be written to disk
                 or no */

  modeType mode; /* The mode of the file right now i.e. reading or writing */

  bool is_open; /* A Flag variable to indicate whether a file is open or not */

  int count;

 public:
  HeapDBFile();
  ~HeapDBFile();
  int Create(const char *fpath, fType file_type, void *startup);
  int Open(const char *fpath);
  int Close();
  void Load(Schema &myschema, const char *loadpath);
  void MoveFirst();
  void Add(Record &addme);
  int GetNext(Record &fetchme);
  int GetNext(Record &fetchme, CNF &cnf, Record &literal);

  int ConvertToSortedFile(const char *sorted_file_name);
  /* TODO */
  /* Test Utility methods */
  /* Need appropriate visiblity */
  int GetNumRecsInBuffer();
  bool WillBufferBeFull(Record &to_be_added);
  off_t GetNumPagesInFile();
  off_t GetCurrentReadPageIndex();
  off_t GetCurrentWritePageIndex();
  File *GetPersistentFileInstance();

 private:
  char *GetMetaDataFileName(
      const char *file_path); /* Create a name of the metadata file based on the
                                 file opened */
  int CopyBufferToPage(
      Page *buffer,
      Page *flush_to_page, /* Move all records from buffer to flush_to_page */
      bool empty_flush_to_page_flag);
  void FlushBuffer(); /* Logic to manage the instance variables when a buffer is
                  flushed */
  void CheckIfFilePresent(); /* Check if a file is opened */
  bool CheckIfCorrectFileType(fType type);
  bool CheckIfFileNameIsValid(const char *file_name);
};
#endif
