#include "DBFile.h"
#include <unistd.h>
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile() {
  // dbFileInstance = new File();
  persistent_file = new File();
}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
  try {
    // Set the type of file
    type = f_type;

    // Set file_path for persistent file
    file_path = f_path;

    // Open persistent file
    persistent_file->Open(0, (char *)file_path);

    // Set current page index to -1 as no pages exist yet
    current_write_page_index = -1;
    current_read_page_index = -1;
    current_read_page_offset = -1;

    // Open metadata file
    int mode = O_TRUNC | O_RDWR | O_CREAT;
    metadata_file_descriptor =
        open(GetMetaDataFileName(file_path), mode, S_IRUSR | S_IWUSR);

    // Check if the metadata file has opened
    if (metadata_file_descriptor < 0) {
      string err("Error creating metadata file for ");
      err += file_path;
      err += " does not exist\n";
      throw runtime_error(err);
    }

    // Write metadata variables
    // Write current_page_index to meta data file
    lseek(metadata_file_descriptor, 0, SEEK_SET);
    write(metadata_file_descriptor, &type, sizeof(fType));
    write(metadata_file_descriptor, &current_write_page_index, sizeof(int));

    // No exception occured
    return 1;
  } catch (runtime_error &e) {
    cerr << e.what() << endl;
    return 0;
  } catch (...) {
    return 0;
  }
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {}

int DBFile::Open(const char *f_path) {
  try {
    // Set the file_path for persistent file
    file_path = f_path;

    // Open persistent file
    persistent_file->Open(1, (char *)file_path);

    // Open metadata file
    int mode = O_RDWR;
    metadata_file_descriptor =
        open(GetMetaDataFileName(file_path), mode, S_IRUSR | S_IWUSR);

    // Check if the metadata file exists
    if (metadata_file_descriptor < 0) {
      string err("Metadata file for ");
      err += file_path;
      err += " does not exist\n";
      throw invalid_argument(err);
    }

    // Read the current_page_index from metadata file
    lseek(metadata_file_descriptor, 0, SEEK_SET);

    // Initialize variables from meta data file
    // Current Meta Data variables  ->
    read(metadata_file_descriptor, &type, sizeof(fType));
    // It is a heap file
    if (type == 0) {
      // Read the current_write_page_index from metadata file and set the
      // instance variable
      read(metadata_file_descriptor, &current_write_page_index, sizeof(int));
      MoveFirst();
    }

    // No exception occured
    return 1;
  } catch (invalid_argument &e) {
    cerr << e.what() << endl;
    return 0;
  } catch (...) {
    return 0;
  }
}

void DBFile::MoveFirst() {
  if (current_write_page_index >= 0) {
    current_read_page_index = -1;
    int num_records = 0;
    while (num_records <= 0 &&
           current_read_page_index <= current_write_page_index) {
      current_read_page_index++;
      Page *check_page = new Page();
      persistent_file->GetPage(check_page, current_read_page_index);
      num_records = check_page->GetNumRecords();
    }

    if (num_records <= 0 ||
        current_read_page_index > current_write_page_index) {
      // None of the pages have records hence resetting the
      // current_write_page_index as well
      current_write_page_index = -1;
      current_read_page_index = -1;
      current_read_page_offset = -1;
      lseek(metadata_file_descriptor, sizeof(int), SEEK_SET);
      write(metadata_file_descriptor, &current_write_page_index, sizeof(int));
    } else {
      current_read_page_offset = -1;
    }
  }
}

int DBFile::Close() {
  try {
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
  } catch (...) {
    return 0;
  }
}

void DBFile::Add(Record &rec) {
  // This page will be added with the new record
  Page *add_me;
  int prev_write_page_index = current_write_page_index;

  // Check if any pages have been alloted before
  if (current_write_page_index < 0) {
    // No pages have been alloted before
    // This is the first page
    current_write_page_index++;

    // Set read page and read index
    current_read_page_index = current_write_page_index;
    current_read_page_offset = -1;

    Page *new_page = new Page();
    if (new_page->Append(&rec) == 0) {
      cerr << "Page Full! Likely an error as we just created a page";
    }
    add_me = new_page;
  } else {
    // Pages have been alloted; Check if last page has space
    Page *fetched_page = new Page();
    persistent_file->GetPage(fetched_page, current_write_page_index);

    // The fetched page is full
    // Allocate a new page
    if (fetched_page->Append(&rec) == 0) {
      persistent_file->AddPage(fetched_page, current_write_page_index);
      current_write_page_index++;
      Page *new_page = new Page();
      if (new_page->Append(&rec) == 0) {
        cerr << "Page full! Likely an error as we just created a page";
      }
      add_me = new_page;
    } else {
      add_me = fetched_page;
    }
  }

  // Add the new or last page in the persistent file
  persistent_file->AddPage(add_me, current_write_page_index);

  // If a new page was required
  if (prev_write_page_index != current_write_page_index) {
    // Update the metadata for the file
    lseek(metadata_file_descriptor, sizeof(int), SEEK_SET);
    write(metadata_file_descriptor, &current_write_page_index, sizeof(int));
  }
}

int DBFile::GetNext(Record &fetchme) {
  // If records in all pages have been read
  if (current_read_page_index > current_write_page_index) {
    return 0;
  }

  // Get the Page current_read_page_index from the file
  Page *readPage = new Page();
  persistent_file->GetPage(readPage, current_read_page_index);

  current_read_page_offset++;
  if (current_read_page_offset >= readPage->GetNumRecords()) {
    current_read_page_index++;
    if (current_read_page_index > current_write_page_index) {
      return 0;
    }
    current_read_page_offset = 0;
    persistent_file->GetPage(readPage, current_read_page_index);
  } else {
    readPage->ReadNext(fetchme, current_read_page_offset);
  }
  return 1;

  /*
  // Next record in the page
  current_read_page_offset++;

  // read next record from the fetched page
  while (readPage  ->  ReadNext(&fetchme, current_read_page_offset) == 0 &&
         current_read_page_index <= current_write_page_index) {
    current_read_page_index++;
    if (current_read_page_offset > 0) {
      current_read_page_offset = 0;
    }
    persistent_file  ->  GetPage(readPage, current_read_page_index);
  }

  // If no more records in any pages are remaining
  if (current_read_page_index > current_write_page_index) {
    return 0;
  } else {
    return 1;
  }
  */

  return 1;
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {}

char *DBFile::GetMetaDataFileName(const char *file_path) {
  string f_path_str(file_path);
  string metadata_file_extension(".header");
  size_t dot_pos = f_path_str.rfind('.');
  string metadata_file_name =
      f_path_str.substr(0, dot_pos) + metadata_file_extension;
  return (char *)metadata_file_name.c_str();
}
