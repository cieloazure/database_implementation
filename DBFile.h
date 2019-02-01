#ifndef DBFILE_H
#define DBFILE_H

#include <string.h>
#include <fstream>
#include <iostream>
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"

typedef enum { heap, sorted, tree } fType;
typedef enum { r, w } modeType;

class DBFile {
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

  bool is_open; /* Flag to indicate whether is file is opened or not */

 public:
  /*
  Constructor

  Initializes -
    - `persistent_file` : A file instance variable
    - `buffer`: A one page buffer for read and writes
    - `dirty`: A flag which indicates whether a write has occured or not
  */
  DBFile();

  ~DBFile();

  /*
  Create(const char *f_path, fType f_type, void *startup)

  Creates a persistent file to store pages with records in it.

  Arguments:
    - `f_path`:  const char * : Destination of the file to be created in string
    - `f_type`: enum fType  : Indiacates type of file which may be one of the
  values of enum fType defined in DBFile.h
    - `startup`: void * : -

  Returns:
    -  Integer indicating whether the file creation was successful or not
    - `1` : on success
    - `0` : on failure
  */
  int Create(const char *fpath, fType file_type, void *startup);

  /*
  Open

  Arguments:
    - `f_path`: const char * : Destination of an already existing file

  Returns:
    - Integer indicatin whether it was successful in opening the file or not
    - `1` is for success and `0` is for failure
  */
  int Open(const char *fpath);

  /*
  Close

  First flushes the buffer to the persistent file. Deallocates memmory required
  for instance variables

  Arguments: None

  Returns : None

  Throws: runtime_error : If an attempt is made to close a file which is not
  open or created
  */
  int Close();

  /* To be implemented */
  void Load(Schema &myschema, const char *loadpath);

  /*
  MoveFirst

  Move the file pointer to the first record in the file.

  Argument: None

  Returns: None

  Throws: runtime_error : If an attempt is made to move to the first record of a
  file which is not opened or created
  */
  void MoveFirst();

  /*
    Add

    Adds a record to the file. The position of the record depends on the type of
    file.
        * Heap File : Add record to the end of the file

    Arguments:
        - addme: Record &: Reference to the record: The record to be added. The
    record will be consumed after this operation

    Returns: None

    Throws: runtime_error : If an attempt is made to add a record to a file
    which is not opened or created
   */
  void Add(Record &addme);

  /*
    GetNext

    Moves the figurative pointer to the next record in the file and returns the
    record pointed by it.

    Arguments:
        - fetchme: Record &: Reference to the record: The record in which the
    next record in the file is to be read into

    Returns:
        - Integer: Indicating whether any records are left.
            `1` : records are left
            `0` : no records are left

    Throws: runtime_error : If an attempt is made to get the next record from a
    file which is not opened or created
   */
  int GetNext(Record &fetchme);

  /* To be implemented */
  int GetNext(Record &fetchme, CNF &cnf, Record &literal, Schema &mySchema);

 private:
  char *GetMetaDataFileName(
      const char *file_path); /* Create a name of the metadata file based on the
                                 file opened */
  int FlushBufferToPage(
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
