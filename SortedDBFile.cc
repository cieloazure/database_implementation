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
  dirty = false;
  is_open = false;
  mode = idle;
  sortOrder = new OrderMaker();
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

    lseek(metadata_file_descriptor, sizeof(fType), SEEK_SET);
    read(metadata_file_descriptor, &current_write_page_index, sizeof(off_t));
    read(metadata_file_descriptor, &runLength, sizeof(int));
    sortOrder->UnSerialize(metadata_file_descriptor);

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
      //   FlushBuffer();
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

void SortedDBFile::Load(Schema &myschema, const char *loadpath) {}

void SortedDBFile::Add(Record &addme) {}

int SortedDBFile::GetNext(Record &fetchme) { return -1; }

int SortedDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
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