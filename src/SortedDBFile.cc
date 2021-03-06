#include "SortedDBFile.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "File.h"
#include "HeapDBFile.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"

extern struct AndList *final;

struct sortInfo {
  OrderMaker *sortOrder;
  int runLength;
};

SortedDBFile::SortedDBFile() {
  persistent_file = new File();
  read_buffer = new Page();
  dirty = false;
  is_open = false;
  mode = idle;
  sortOrder = new OrderMaker();
  cachedGetNextFlag = false;
}

SortedDBFile::~SortedDBFile() { delete persistent_file; }

int SortedDBFile::Create(const char *f_path, fType f_type, void *startup) {
  try {
    // Set the type of file
    type = f_type;
    CheckIfCorrectFileType(type);

    // Set file_path for persistent file
    file_path = f_path;
    CheckIfFileNameIsValid(f_path);

    // Open persistent file
    persistent_file->Open(0, (char *)file_path);

    // Set current page index to -1 as no pages exist yet
    // -1 represents an invalid value
    current_write_page_index = -1;
    current_read_page_index = -1;
    current_read_page_offset = -1;

    // Open metadata file
    int file_mode = O_TRUNC | O_RDWR | O_CREAT;
    metadata_file_descriptor =
        open(GetMetaDataFileName(file_path), file_mode, S_IRUSR | S_IWUSR);

    // Check if the metadata file has opened
    if (metadata_file_descriptor < 0) {
      printf("Oh dear, something went wrong with open()! %s\n",
             strerror(errno));
      string err("Error creating metadata file for ");
      err += file_path;
      err += "\n";
      throw runtime_error(err);
    }

    // Write metadata variables
    lseek(metadata_file_descriptor, 0, SEEK_SET);
    write(metadata_file_descriptor, &type, sizeof(fType));

    // Write current_page_index to meta data file
    write(metadata_file_descriptor, &current_write_page_index, sizeof(off_t));

    // Write sortInfo to meta data file
    sortInfo *s = (sortInfo *)startup;
    runLength = s->runLength;
    sortOrder = s->sortOrder;

    write(metadata_file_descriptor, &runLength, sizeof(int));
    sortOrder->Serialize(metadata_file_descriptor);

    // No exception occured
    is_open = true;
    mode = idle;
    return 1;
  } catch (runtime_error &e) {
    cerr << e.what() << endl;
    return 0;
  } catch (...) {
    return 0;
  }
}

int SortedDBFile::Open(const char *f_path) {
  try {
    file_path = f_path;
    // Open persistent file
    persistent_file->Open(1, (char *)file_path);
    // Set is open to true if no exception occured in persistent_file
    is_open = true;
    // Read current_write_page_index from metadata file descriptor

    int file_mode = O_RDWR;
    metadata_file_descriptor =
        open(GetMetaDataFileName(file_path), file_mode, S_IRUSR | S_IWUSR);

    // Check if the metadata file has opened
    if (metadata_file_descriptor < 0) {
      printf("Oh dear, something went wrong with open()! %s\n",
             strerror(errno));
      string err("Error opening metadata file for ");
      err += file_path;
      err += "\n";
      throw runtime_error(err);
    }

    lseek(metadata_file_descriptor, sizeof(fType), SEEK_SET);
    read(metadata_file_descriptor, &current_write_page_index, sizeof(off_t));
    read(metadata_file_descriptor, &runLength, sizeof(int));
    sortOrder->UnSerialize(metadata_file_descriptor);

    // Debug sort order
    sortOrder->Print();

    MoveFirst();
    return 1;
  } catch (runtime_error &e) {
    cerr << e.what() << endl;
    return 0;
  } catch (...) {
    return 0;
  }
}

void SortedDBFile::MoveFirst() {
  CheckIfFilePresent();
  cachedGetNextFlag = false;
  mode = idle;
  if (current_write_page_index >= 0) {
    // Set current_read_page_index
    current_read_page_index = -1;
    int num_records = 0;
    while (num_records <= 0 &&
           current_read_page_index <= current_write_page_index) {
      current_read_page_index++;
      Page *check_page = new Page();
      persistent_file->GetPage(check_page, current_read_page_index);
      num_records = check_page->GetNumRecords();
    }

    // Set other instance variables
    if (num_records <= 0 ||
        current_read_page_index > current_write_page_index) {
      // None of the pages have records hence resetting the
      // current_write_page_index as well
      current_write_page_index = -1;
      current_read_page_index = -1;
      current_read_page_offset = -1;
      lseek(metadata_file_descriptor, sizeof(fType), SEEK_SET);
      write(metadata_file_descriptor, &current_write_page_index, sizeof(off_t));
    } else {
      current_read_page_offset = -1;
    }
  }
}

int SortedDBFile::Close() {
  try {
    CheckIfFilePresent();
    if (dirty && !merging) {
      MergeBigqRecords();
      delete bigq;
      delete input;
      delete output;
      dirty = false;
      cachedGetNextFlag = false;
    }
    // Close the persistent file
    persistent_file->Close();

    // Close the metadata file
    close(metadata_file_descriptor);

    // Set current_write_page_index to an invalid value
    current_write_page_index = -1;

    // Set current_read_page_index to an invalid value
    current_read_page_index = -1;

    // Set current_read_page_offset to an invalid value
    current_read_page_offset = -1;

    // Set file path to NULL to indicate the file operations has fininshed
    file_path = NULL;

    return 1;
  } catch (runtime_error r) {
    cerr << "Trying to close a file which is not opened" << endl;
    return 0;
  } catch (...) {
    return 0;
  }
}

void SortedDBFile::Load(Schema &myschema, const char *loadpath) {
  CheckIfFilePresent();
  Record *temp = new Record();
  FILE *table_file = fopen(loadpath, "r");

  if (table_file == NULL) {
    string err = "Error opening table file for load";
    err += loadpath;
    throw runtime_error(err);
  }

  count = 0;
  std::cout << "Loaded:" << endl;
  while (temp->SuckNextRecord(&myschema, table_file) == 1) {
    if (temp != NULL) {
      count++;
      std::cout << "\r" << count;
      Add(*temp);
    }
  }
  std::cout << endl;
  if (!merging) {
    MergeBigqRecords();
    delete bigq;
    delete input;
    delete output;
    dirty = false;
    cachedGetNextFlag = false;
  }
  std::cout << "Bulk Loaded " << count << " records" << std::endl;
}

void SortedDBFile::Add(Record &addme) {
  CheckIfFilePresent();

  if (!dirty) {
    input = new Pipe(100);
    output = new Pipe(100);
    bigq = new BigQ(*input, *output, *sortOrder, runLength);
    mode = writing;
    dirty = true;
  }

  // insert into the bigq input pipe.
  Record *copy = new Record();
  copy->Copy(&addme);
  input->Insert(copy);
}

int SortedDBFile::MergeBigqRecords() {
  merging = true;

  input->ShutDown();
  if (dirty) {
    MoveFirst();
    File *mergeFile = new File();
    mergeFile->Open(0, "mergeFile.bin");
    Page *mergedPageBuffer = new Page();
    off_t mergeFilePageIndex = 0;

    auto addPageToMergeFile = [&mergeFile, &mergedPageBuffer,
                               &mergeFilePageIndex, this]() -> void {
      Page *toBeAdded = new Page();
      CopyBufferToPage(
          mergedPageBuffer,
          toBeAdded); /* Copy is required to avoid double free error */
      // create a new page
      mergeFile->AddPage(toBeAdded, mergeFilePageIndex);
      mergeFilePageIndex++;
    };

    auto addRecToMergeFile = [&mergeFile, &mergedPageBuffer,
                              &addPageToMergeFile,
                              this](Record *smallest) -> void {
      if (mergedPageBuffer->Append(smallest) == 0) {
        addPageToMergeFile();

        // add the record which failed adding on the page back on the new page
        mergedPageBuffer->Append(smallest);
      }
    };

    ComparisonEngine compEngine;

    Record *fileRec = new Record();
    Record *bigqRec = new Record();

    // while bigq is not empty or sorted file is not empty
    // merge from queue and file
    int qcount = 0;
    int fcount = 0;
    bool qHasElement = output->Remove(bigqRec);
    bool fileHasElement = GetNext(*fileRec);
    while (qHasElement && fileHasElement) {
      int status = compEngine.Compare(bigqRec, fileRec, sortOrder);
      Record *smallest = new Record();
      if (status >= 0) {
        smallest->Copy(fileRec);
        fileRec = new Record();
        fileHasElement = GetNext(*fileRec);
        fcount++;
      } else if (status < 0) {
        smallest->Copy(bigqRec);
        bigqRec = new Record();
        qHasElement = output->Remove(bigqRec);
        qcount++;
      }

      addRecToMergeFile(smallest);
    }

    Record *leftOver = new Record();

    // file is empty
    if (!fileHasElement) {
      leftOver->Copy(bigqRec);
      addRecToMergeFile(leftOver);
      bigqRec = new Record();
      while (output->Remove(bigqRec)) {
        leftOver->Copy(bigqRec);
        addRecToMergeFile(leftOver);
        bigqRec = new Record();
        qcount++;
      }
    }

    // queue is empty
    if (!qHasElement) {
      leftOver->Copy(bigqRec);
      addRecToMergeFile(leftOver);
      fileRec = new Record();
      while (GetNext(*fileRec)) {
        leftOver->Copy(fileRec);
        addRecToMergeFile(leftOver);
        fileRec = new Record();
        fcount++;
      }
      addRecToMergeFile(fileRec);
    }

    addPageToMergeFile();

    current_write_page_index = mergeFilePageIndex - 1;  // or no -1? check
    lseek(metadata_file_descriptor, sizeof(fType), SEEK_SET);
    write(metadata_file_descriptor, &current_write_page_index, sizeof(off_t));

    // Open the mergedfile instance
    mergeFile->Close();
    persistent_file->Close();
    if (rename("mergeFile.bin", file_path) != 0) {
      cerr << "Error renaming file!" << endl;
    }
    persistent_file->Open(1, (char *)file_path);

    // Set the current_read_page_index as well
    MoveFirst();

    // Clean up work
    merging = false;
  }
}

int SortedDBFile::GetNext(Record &fetchme) {
  CheckIfFilePresent();

  // Merge the files before changing mode
  if (dirty && !merging) {
    MergeBigqRecords();
    delete bigq;
    delete input;
    delete output;
    dirty = false;
    cachedGetNextFlag = false;
    MoveFirst();
  }

  // If records in all pages have been read
  if (current_read_page_index < 0 ||
      current_read_page_index > current_write_page_index) {
    return 0;
  }

  // Get the Page current_read_page_index from the file
  if (mode == writing || mode == idle) {
    persistent_file->GetPage(read_buffer, current_read_page_index);
    if (!merging) mode = reading;
  }

  // Increment to get the next record on the page no `current_read_page_index`
  current_read_page_offset++;

  // If there are no more records on the page `current_read_page_index` get next
  // page
  while (current_read_page_offset >= read_buffer->GetNumRecords() &&
         current_read_page_index <= current_write_page_index) {
    current_read_page_index++;
    // If there are no more pages remaining in the file
    // All records have been read
    if (current_read_page_index > current_write_page_index) {
      return 0;
    }
    current_read_page_offset = 0;
    persistent_file->GetPage(read_buffer, current_read_page_index);
  }

  // Read the next record now from appropriate page
  read_buffer->ReadNext(fetchme, current_read_page_offset);
  return 1;
}

int SortedDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
  // if dirty
  if (dirty && !merging) {
    MergeBigqRecords();
    delete bigq;
    delete input;
    delete output;
    dirty = false;
    cachedGetNextFlag = false;
  }
  ComparisonEngine comp;

  auto fastSearchUtility = [&cnf, &literal, this, &comp, &fetchme](
                               OrderMaker queryOrderMaker,
                               OrderMaker literalOrderMaker) -> bool {
    while (GetNext(fetchme) != 0) {
      if (comp.Compare(&literal, &literalOrderMaker, &fetchme,
                       &queryOrderMaker) == 0) {
        if (comp.Compare(&fetchme, &literal, &cnf)) {
          return 1;
        }
      } else {
        return 0;
      }
    }
    return 0;
  };

  auto normalSearchUtility = [this, &literal, &cnf, &comp, &fetchme]() -> bool {
    while (GetNext(fetchme) != 0) {
      if (comp.Compare(&fetchme, &literal, &cnf)) {
        return 1;
      }
    }
    return 0;
  };

  if (!cachedGetNextFlag) {
    cnf.BuildQueryOrderMaker(*sortOrder, queryOrderMaker, literalOrderMaker);
    if (!queryOrderMaker.IsEmpty()) {
      // queryOrderMaker is not empty
      // cnf matches our file sortorder
      // we can use our file sortorder to make this query faster
      off_t foundPage;
      int foundOffset;
      if (BinarySearchFile(&foundPage, &foundOffset, persistent_file,
                           &queryOrderMaker, &literalOrderMaker, &literal,
                           current_read_page_index, current_read_page_offset)) {
        // Binary search is successful with queryOrderMaker
        current_read_page_index = foundPage;
        // To reuse GetNext(&fetchme) to get the next record on this page we
        // decrement the offset from where we found so that it will be
        // incremented again in GetNext()
        current_read_page_offset = foundOffset - 1;

        // If a binary search is successful with the queryOrderMaker it makes
        // sense to cache the queryOrderMaker as it is likely that the next call
        // to getnext with same parameters will have a match as well
        cachedGetNextFlag = true;

        return fastSearchUtility(queryOrderMaker, literalOrderMaker);
      } else {
        // Binary search was unsuccessful with queryOrderMaker
        return 0;
      }
    } else {
      // queryOrderMaker is empty
      // speedup in search is not possible

      return normalSearchUtility();
    }
  } else {
    // query parameters are cached
    return fastSearchUtility(queryOrderMaker, literalOrderMaker);
  }

  // Error value, should not be present
  return -1;
}

int SortedDBFile::BinarySearchFile(off_t *foundPage, int *foundOffset,
                                   File *persistent_file,
                                   OrderMaker *queryOrderMaker,
                                   OrderMaker *literalOrderMaker,
                                   Record *literal, off_t page_offset,
                                   int record_offset) {
  ComparisonEngine compEngine;
  off_t lower = page_offset;
  off_t higher = persistent_file->GetLength() - 2;
  while (lower <= higher) {
    off_t mid = lower + (higher - lower) / 2;
    Page *buffer = new Page();
    persistent_file->GetPage(buffer, mid);
    int numRecsOnPage = buffer->GetNumRecords();

    // check if the record is on this page
    // Schema mySchema("catalog", "lineitem");
    Record *firstRecOnPage = new Record();
    buffer->ReadNext(*firstRecOnPage, record_offset);
    // firstRecOnPage->Print(&mySchema);

    Record *lastRecOnPage = new Record();
    buffer->ReadNext(*lastRecOnPage, numRecsOnPage - 1);
    // lastRecOnPage->Print(&mySchema);

    // check conditions

    // mid check
    // check if the first or the last record from this page is equal to
    // literal
    int firstRecStatus = compEngine.Compare(literal, literalOrderMaker,
                                            firstRecOnPage, queryOrderMaker);
    int lastRecStatus = compEngine.Compare(literal, literalOrderMaker,
                                           lastRecOnPage, queryOrderMaker);

    if (firstRecStatus >= 0 && lastRecStatus <= 0) {
      // record is on this page or does not exist
      if ((*foundOffset = BinarySearchPage(buffer, queryOrderMaker,
                                           literalOrderMaker, literal)) != -1) {
        *foundPage = mid;
        return 1;
      } else {
        return 0;
      }
    } else if (lastRecStatus > 0) {
      // record is in second half
      // change lower to mid + 1
      lower = mid + 1;
    } else if (firstRecStatus < 0) {
      // record is in first half
      // change higher to mid - 1
      higher = mid - 1;
    }
  }

  return 0;
}

int SortedDBFile::BinarySearchPage(Page *buffer, OrderMaker *queryOrderMaker,
                                   OrderMaker *literalOrderMaker,
                                   Record *literal) {
  int lower = 0;
  int higher = buffer->GetNumRecords();
  ComparisonEngine compEngine;
  while (lower <= higher) {
    int mid = lower + (higher - lower) / 2;
    Record *midRec = new Record();
    buffer->ReadNext(*midRec, mid);
    int midRecStatus =
        compEngine.Compare(literal, literalOrderMaker, midRec, queryOrderMaker);
    if (midRecStatus == 0) {
      Record *currRec = new Record();
      Record *prevRec = new Record();

      // Move up to find the first record in case of duplicated records
      int current = mid;
      buffer->ReadNext(*currRec, current);

      int previous = current - 1;
      buffer->ReadNext(*prevRec, previous);

      ComparisonEngine comp;
      while (previous >= 0 &&
             comp.Compare(prevRec, currRec, queryOrderMaker) == 0) {
        current = previous;
        buffer->ReadNext(*currRec, current);
        previous = previous - 1;
        buffer->ReadNext(*prevRec, previous);
      }

      return current;
    } else if (midRecStatus > 0) {
      lower = mid + 1;
    } else if (midRecStatus < 0) {
      higher = mid - 1;
    }
  }

  return -1;
}

char *SortedDBFile::GetMetaDataFileName(const char *file_path) {
  string f_path_str(file_path);
  string metadata_file_extension(".header");
  size_t dot_pos = f_path_str.rfind('.');
  string metadata_file_name =
      f_path_str.substr(0, dot_pos) + metadata_file_extension;
  return (char *)metadata_file_name.c_str();
}

void SortedDBFile::CheckIfFilePresent() {
  if (!is_open) {
    throw runtime_error(
        "File destination is not open or created, Please create a file or open "
        "an existing one");
  }
}

bool SortedDBFile::CheckIfCorrectFileType(fType type) {
  switch (type) {
    case sorted:
      return true;
      break;

    default:
      throw runtime_error("File type is incorrect or not supported");
  }
  return false;
}

bool SortedDBFile::CheckIfFileNameIsValid(const char *file_name) {
  // TODO: check for file extension?
  if (file_name == NULL || *file_name == '\0') {
    throw runtime_error("File name is invalid");
  } else {
    return true;
  }
  return false;
}

void SortedDBFile::CopyBufferToPage(Page *from, Page *to) {
  Record to_be_copied;
  while (from->GetFirst(&to_be_copied) != 0) {
    to->Append(&to_be_copied);
  }
}