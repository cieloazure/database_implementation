#include "SortedDBFile.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "File.h"
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
  buffer = new Page();
  new_file_buffer = new Page();
  dirty = false;
  is_open = false;
  mode = idle;
  sortOrder = new OrderMaker();
  input = new Pipe(100);
  output = new Pipe(100);
  cachedGetNextFlag = false;
}

SortedDBFile::~SortedDBFile() {
  delete persistent_file;
  delete input;
  delete output;
}

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

int SortedDBFile::CreateNew(File *new_file, const char *f_path, fType f_type,
                            void *startup) {
  try {
    // Set the type of file
    type = f_type;
    CheckIfCorrectFileType(type);

    // Set file_path for persistent file
    old_file_path = file_path;
    file_path = f_path;
    CheckIfFileNameIsValid(f_path);

    // Open persistent file
    new_file->Open(0, (char *)file_path);

    // Set current page index to -1 as no pages exist yet
    // -1 represents an invalid value
    newf_write_page_index = -1;
    newf_read_page_index = -1;
    newf_read_page_offset = -1;

    // Open metadata file
    int file_mode = O_TRUNC | O_RDWR | O_CREAT;
    new_metadata_file_descriptor =
        open(GetMetaDataFileName(file_path), file_mode, S_IRUSR | S_IWUSR);

    // Check if the metadata file has opened
    if (new_metadata_file_descriptor < 0) {
      string err("Error creating metadata file for ");
      err += file_path;
      err += "\n";
      throw runtime_error(err);
    }

    // Write metadata variables
    lseek(new_metadata_file_descriptor, 0, SEEK_SET);
    write(new_metadata_file_descriptor, &type, sizeof(fType));

    // Write current_page_index to meta data file
    write(new_metadata_file_descriptor, &newf_write_page_index, sizeof(off_t));

    write(new_metadata_file_descriptor, &runLength, sizeof(int));
    sortOrder->Serialize(new_metadata_file_descriptor);

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
    if (dirty) {
      // TODO: Implement this method for SortedDBFile
      FlushBuffer();
      dirty = false;
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
    old_file_path = NULL;

    return 1;
  } catch (runtime_error r) {
    cerr << "Trying to close a file which is not opened" << endl;
    return 0;
  } catch (...) {
    return 0;
  }
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
    case heap:
    case sorted:
    case tree:
      return true;
      break;

    default:
      throw runtime_error("File type is incorrect or not supported");
  }
  return false;
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
  FlushBuffer();
  cout << "Bulk Loaded " << count << " records" << endl;
}

void SortedDBFile::Add(Record &addme) {
  CheckIfFilePresent();

  if (mode == writing) {
    // insert into the bigq input pipe.
    Record *copy = new Record();
    copy->Copy(&addme);
    input->Insert(copy);

  } else {
    // init BigQ member object and then insert into the bigq pipe.
    bigq_file = new BigQ(*input, *output, *sortOrder, runLength);
    Record *copy = new Record();
    copy->Copy(&addme);
    input->Insert(copy);
    mode = writing;
  }
}

void SortedDBFile::AddForMerge(Record &addme) {
  CheckIfFilePresent();

  if (new_file_buffer->Append(&addme) == 0) {
    FlushBufferForMerge();
    new_file_buffer->Append(&addme); /* Need to append again as the previous
                             append was unsuccessful */
  }
}

void SortedDBFile::FlushBufferForMerge() {
  int prev_write_page_index = newf_write_page_index;

  Page *flush_to_page = new Page();
  // Check if any pages have been alloted before
  if (newf_write_page_index < 0) {
    // No pages have been alloted before
    // This is the first page
    newf_write_page_index++;

    // Set read page and read index
    newf_read_page_index = newf_write_page_index;
    current_read_page_index = newf_write_page_index;
    current_write_page_index = newf_write_page_index;
    newf_read_page_offset = -1;
  } else {
    // Pages have been alloted; Check if last page has space
    new_persistent_file->GetPage(flush_to_page, newf_write_page_index);
  }

  // Flush buffer to pages till the buffer is empty
  // The file page size may be smaller than the buffer page size hence
  // The entire buffer may not fit on the page
  bool empty_flush_to_page_flag = false;
  while (CopyBufferToPage(new_file_buffer, flush_to_page,
                          empty_flush_to_page_flag) == 0) {
    new_persistent_file->AddPage(flush_to_page, newf_write_page_index);
    newf_write_page_index++;
    current_write_page_index++;
    empty_flush_to_page_flag = true;
  }

  new_persistent_file->AddPage(flush_to_page, newf_write_page_index);
  // If new page(s) was required
  if (prev_write_page_index != newf_write_page_index) {
    // Update the metadata for the file
    lseek(new_metadata_file_descriptor, sizeof(fType), SEEK_SET);
    write(new_metadata_file_descriptor, &newf_write_page_index, sizeof(off_t));
  }
}

int SortedDBFile::MergeBigqRecords() {
  input->ShutDown();

  bool old_file = false;
  bool bigq = false;

  Record *old_file_rec;
  Record bigq_rec;

  ComparisonEngine ceng;
  int comparator = -2;

  if (output->Remove(&bigq_rec)) {
    bigq = true;
  }

  int count = 0;

  // perform merging only if there's data in bigq
  if (bigq) {
    if (GetNextForMerge(*old_file_rec)) {
      old_file = true;
    }
    new_persistent_file = new File();
    if (CreateNew(new_persistent_file, "gtest_new.bin", sorted, NULL)) {
      while (bigq || old_file) {
        if (bigq && old_file) {
          comparator = ceng.Compare(&bigq_rec, old_file_rec, sortOrder);

          switch (comparator) {
            case -1:
              // bigq is smaller
              AddForMerge(bigq_rec);
              count++;
              bigq = false;
              break;
            case 0:
              // both are equal
              AddForMerge(bigq_rec);
              AddForMerge(*old_file_rec);
              count += 2;
              bigq = false;
              old_file_rec = NULL;
              break;
            case 1:
              // old_file_rec is smaller
              AddForMerge(*old_file_rec);
              count++;
              old_file_rec = NULL;
              break;
          }
        } else if (!bigq) {
          AddForMerge(*old_file_rec);
          count++;
          old_file_rec = NULL;
        } else if (!old_file) {
          AddForMerge(bigq_rec);
          count++;
          bigq = false;
        }

        // refill variables with next records
        if (!bigq) {
          if (output->Remove(&bigq_rec)) {
            bigq = true;
          }
        }

        if (!old_file_rec) {
          if (GetNextForMerge(*old_file_rec)) {
            old_file = true;
          }
        }

        cout << "Added " << count << " records." << endl;
      }

      if (new_file_buffer->GetNumRecords() > 0) {
        FlushBufferForMerge();
      }

      // after merging, rename newfile to oldfile.
      // new_persistent_file->Close();
      // persistent_file->Close();

      if (remove(old_file_path) != 0) {
        cout << "Error deleting old file" << endl;
      }

      if (rename("gtest_new.bin", old_file_path) != 0) {
        cout << "Error renaming new file" << endl;
      }

      // what happens to metadata now?
      // put newf_write_page into metadata file, rest variables will be 0.
      // lseek(metadata_file_descriptor, sizeof(fType), SEEK_SET);
      // write(metadata_file_descriptor, &newf_write_page_index, sizeof(off_t));
      // and just open persistent_file again with file path.
      // persistent_file = new_persistent_file;
      new_persistent_file->Close();
      delete persistent_file;
      persistent_file = new File();
      // persistent_file->curLength = new_persistent_file->curLength;
      persistent_file->Open(1, (char *)old_file_path);
    } else {
      cout << "ERROR: Could not create new output file." << endl;
      return 0;
    }
  }
}

int SortedDBFile::GetNextForMerge(Record &fetchme) {
  CheckIfFilePresent();
  // If the buffer is dirty the records are to be written out to file before
  // being read

  // If records in all pages have been read
  if (current_read_page_index < 0 ||
      current_read_page_index > current_write_page_index) {
    return 0;
  }

  // Get the Page current_read_page_index from the file
  if (mode == writing || mode == idle) {
    persistent_file->GetPage(buffer, current_read_page_index);
    mode = reading;
  }

  // Increment to get the next record on the page no `current_read_page_index`
  current_read_page_offset++;
  // If there are no more records on the page `current_read_page_index` get next
  // page
  while (current_read_page_offset >= buffer->GetNumRecords() &&
         current_read_page_index <= current_write_page_index) {
    current_read_page_index++;
    // If there are no more pages remaining in the file
    // All records have been read
    if (current_read_page_index > current_write_page_index) {
      return 0;
    }
    current_read_page_offset = 0;
    persistent_file->GetPage(buffer, current_read_page_index);
  }

  // Read the next record now from appropriate page
  buffer->ReadNext(fetchme, current_read_page_offset);
  return 1;
}

int SortedDBFile::CopyBufferToPage(Page *buffer, Page *flush_to_page,
                                   bool empty_flush_to_page_flag) {
  // Is it a new page and hence previous flush has already been written to file
  if (empty_flush_to_page_flag) {
    flush_to_page->EmptyItOut();
  }

  Record to_be_copied;
  while (buffer->GetFirst(&to_be_copied) != 0) {
    if (flush_to_page->Append(&to_be_copied) == 0) {
      buffer->Append(
          &to_be_copied); /* Need to append again to the buffer as the
                             GetFirst() will remove the record from buffer */
      return 0;
    }
  }
  return 1;
}

void SortedDBFile::FlushBuffer() {
  int prev_write_page_index = current_write_page_index;

  Page *flush_to_page = new Page();
  // Check if any pages have been alloted before
  if (current_write_page_index < 0) {
    // No pages have been alloted before
    // This is the first page
    current_write_page_index++;

    // Set read page and read index
    current_read_page_index = current_write_page_index;
    current_read_page_offset = -1;
  } else {
    // Pages have been alloted; Check if last page has space
    persistent_file->GetPage(flush_to_page, current_write_page_index);
  }

  // Flush buffer to pages till the buffer is empty
  // The file page size may be smaller than the buffer page size hence
  // The entire buffer may not fit on the page
  bool empty_flush_to_page_flag = false;
  while (CopyBufferToPage(buffer, flush_to_page, empty_flush_to_page_flag) ==
         0) {
    persistent_file->AddPage(flush_to_page, current_write_page_index);
    current_write_page_index++;
    empty_flush_to_page_flag = true;
  }

  persistent_file->AddPage(flush_to_page, current_write_page_index);
  // If new page(s) was required
  if (prev_write_page_index != current_write_page_index) {
    // Update the metadata for the file
    lseek(metadata_file_descriptor, sizeof(fType), SEEK_SET);
    write(metadata_file_descriptor, &current_write_page_index, sizeof(off_t));
  }
}

int SortedDBFile::GetNext(Record &fetchme) {
  CheckIfFilePresent();

  // Merge the files before changing mode
  MergeBigqRecords();

  // If records in all pages have been read
  if (current_read_page_index < 0 ||
      current_read_page_index > current_write_page_index) {
    return 0;
  }

  // Get the Page current_read_page_index from the file
  if (mode == writing || mode == idle) {
    persistent_file->GetPage(buffer, current_read_page_index);
    mode = reading;
  }

  // Increment to get the next record on the page no `current_read_page_index`
  current_read_page_offset++;

  // If there are no more records on the page `current_read_page_index` get next
  // page
  while (current_read_page_offset >= buffer->GetNumRecords() &&
         current_read_page_index <= current_write_page_index) {
    current_read_page_index++;
    // If there are no more pages remaining in the file
    // All records have been read
    if (current_read_page_index > current_write_page_index) {
      return 0;
    }
    current_read_page_offset = 0;
    persistent_file->GetPage(buffer, current_read_page_index);
  }

  // Read the next record now from appropriate page
  buffer->ReadNext(fetchme, current_read_page_offset);
  return 1;
}

int SortedDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
  ComparisonEngine comp;
  if (!cachedGetNextFlag) {
    cnf.BuildQueryOrderMaker(*sortOrder, queryOrderMaker);
    if (!queryOrderMaker.IsEmpty()) {
      // queryOrderMaker is not empty
      // cnf matches our file sortorder
      // we can use our file sortorder to make this query faster
      Record *putItHere;
      off_t foundPage;
      int foundOffset;
      if (BinarySearchFile(putItHere, &foundPage, &foundOffset, persistent_file,
                           &queryOrderMaker, &literal, current_read_page_index,
                           current_read_page_offset)) {
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

        while (GetNext(fetchme) != 0) {
          if (comp.Compare(&fetchme, &literal, &queryOrderMaker) == 0) {
            if (comp.Compare(&fetchme, &literal, &cnf)) {
              return 1;
            }
          } else {
            return 0;
          }
        }
        return 0;
      } else {
        // Binary search was unsuccessful with queryOrderMaker
        return 0;
      }
    } else {
      // queryOrderMaker is empty
      // speedup in search is not possible
      while (GetNext(fetchme) != 0) {
        if (comp.Compare(&fetchme, &literal, &cnf) == 0) {
          return 1;
        }
      }
      return 0;
    }
  } else {
    while (GetNext(fetchme) != 0) {
      if (comp.Compare(&fetchme, &literal, &queryOrderMaker) == 0) {
        if (comp.Compare(&fetchme, &literal, &cnf)) {
          return 1;
        }
      } else {
        return 0;
      }
    }
  }

  return -1;
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

int SortedDBFile::GetNumRecsInBuffer() { return buffer->GetNumRecords(); }

bool SortedDBFile::WillBufferBeFull(Record &to_be_added) {
  return buffer->IsPageFull(&to_be_added);
}

off_t SortedDBFile::GetNumPagesInFile() { return persistent_file->GetLength(); }

off_t SortedDBFile::GetCurrentReadPageIndex() {
  return current_read_page_index;
}

off_t SortedDBFile::GetCurrentWritePageIndex() {
  return current_write_page_index;
}

int SortedDBFile::BinarySearchFile(Record *putItHere, off_t *foundPage,
                                   int *foundOffset, File *persistent_file,
                                   OrderMaker *queryOrderMaker, Record *literal,
                                   off_t page_offset, int record_offset) {
  ComparisonEngine compEngine;
  off_t lower = page_offset;
  off_t higher = persistent_file->GetLength() - 1;
  while (lower <= higher) {
    off_t mid = lower + (higher - lower) / 2;
    Page *buffer = new Page();
    persistent_file->GetPage(buffer, mid);
    int numRecsOnPage = buffer->GetNumRecords();

    // TODO: sorting the buffer first
    // TODO: Ideally page should be sorted in the file itself
    // TODO: will be removed after add and load functions are implemented/tested
    // ************ DANGER ************************
    // ***************** QUICKFIX, to be removed **********
    // ####### DONT FORGET TO REMOVE THIS LINE
    // buffer->Sort(*sortOrder);

    // check if the record is on this page
    Record *firstRecOnPage = new Record();
    buffer->ReadNext(*firstRecOnPage, record_offset);

    Schema mySchema("catalog", "lineitem");
    firstRecOnPage->Print(&mySchema);

    Record *lastRecOnPage = new Record();
    buffer->ReadNext(*lastRecOnPage, numRecsOnPage - 1);
    lastRecOnPage->Print(&mySchema);

    // check conditions

    // mid check
    // check if the first or the last record from this page is equal to
    // literal
    int firstRecStatus =
        compEngine.Compare(literal, firstRecOnPage, queryOrderMaker);
    int lastRecStatus =
        compEngine.Compare(literal, lastRecOnPage, queryOrderMaker);

    if (firstRecStatus == 0) {
      putItHere = firstRecOnPage;
      *foundOffset = 0;
      *foundPage = mid;
      return 1;
    } else if (lastRecStatus == 0) {
      putItHere = lastRecOnPage;
      *foundOffset = numRecsOnPage - 1;
      *foundPage = mid;
      return 1;
    } else {
      if (firstRecStatus > 0 && lastRecStatus < 0) {
        // record is on this page or does not exist
        if ((*foundOffset = BinarySearchPage(putItHere, buffer, queryOrderMaker,
                                             literal))) {
          *foundPage = mid;
          return 1;
        } else {
          return -1;
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
  }

  return -1;
}

int SortedDBFile::BinarySearchPage(Record *putItHere, Page *buffer,
                                   OrderMaker *queryOrderMaker,
                                   Record *literal) {
  int lower = 0;
  int higher = buffer->GetNumRecords();
  ComparisonEngine compEngine;
  while (lower <= higher) {
    int mid = lower + (higher - lower) / 2;
    Record *midRec = new Record();
    buffer->ReadNext(*midRec, mid);
    int midRecStatus = compEngine.Compare(literal, midRec, queryOrderMaker);
    if (midRecStatus == 0) {
      // TODO
      // Check if the previous record is also equal to query order maker
      // In that case move up to find the first record which matches
      putItHere = midRec;
      return mid;
    } else if (midRecStatus > 0) {
      lower = mid + 1;
    } else if (midRecStatus < 0) {
      higher = mid - 1;
    }
  }

  return -1;
}
