#include "DBFile.h"
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

DBFile::DBFile() { Instantiate(); }

DBFile::~DBFile() { delete persistent_file; }

int DBFile::Create(const char *f_path, fType f_type, void *startup)
{
  try
  {
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
    if (metadata_file_descriptor < 0)
    {
      string err("Error creating metadata file for ");
      err += file_path;
      err += " does not exist\n";
      throw runtime_error(err);
    }

    // Write metadata variables
    // Write current_page_index to meta data file
    lseek(metadata_file_descriptor, 0, SEEK_SET);
    write(metadata_file_descriptor, &type, sizeof(fType));
    write(metadata_file_descriptor, &current_write_page_index, sizeof(off_t));

    // No exception occured
    is_open = true;
    mode = idle;
    return 1;
  }
  catch (runtime_error &e)
  {
    cerr << e.what() << endl;
    return 0;
  }
  catch (...)
  {
    return 0;
  }
}

int DBFile::Open(const char *f_path)
{
  try
  {
    // Set the file_path for persistent file
    file_path = f_path;
    CheckIfFileNameIsValid(f_path);

    // Open persistent file
    persistent_file->Open(1, (char *)file_path);

    // Open metadata file
    int file_mode = O_RDWR;
    metadata_file_descriptor =
        open(GetMetaDataFileName(file_path), file_mode, S_IRUSR | S_IWUSR);

    // Check if the metadata file exists
    if (metadata_file_descriptor < 0)
    {
      string err("Metadata file for ");
      err += file_path;
      err += " does not exist\n";
      throw runtime_error(err);
    }

    // Read the current_page_index from metadata file
    lseek(metadata_file_descriptor, 0, SEEK_SET);

    // Initialize variables from meta data file
    // Current Meta Data variables  ->
    read(metadata_file_descriptor, &type, sizeof(fType));
    CheckIfCorrectFileType(type);
    is_open = true;
    // It is a heap file
    switch (type)
    {
    case heap:
      // Read the current_write_page_index from metadata file and set the
      // instance variable
      read(metadata_file_descriptor, &current_write_page_index,
           sizeof(off_t));
      MoveFirst();
      break;

    case sorted:
      break;

    case tree:
      break;
    }
    // No exception occured
    return 1;
  }
  catch (runtime_error &e)
  {
    cerr << e.what() << endl;
    return 0;
  }
  catch (...)
  {
    return 0;
  }
}

void DBFile::MoveFirst()
{
  CheckIfFilePresent();
  if (current_write_page_index >= 0)
  {
    // Set current_read_page_index
    current_read_page_index = -1;
    int num_records = 0;
    while (num_records <= 0 &&
           current_read_page_index <= current_write_page_index)
    {
      current_read_page_index++;
      Page *check_page = new Page();
      persistent_file->GetPage(check_page, current_read_page_index);
      num_records = check_page->GetNumRecords();
    }

    // Set other instance variables
    if (num_records <= 0 ||
        current_read_page_index > current_write_page_index)
    {
      // None of the pages have records hence resetting the
      // current_write_page_index as well
      current_write_page_index = -1;
      current_read_page_index = -1;
      current_read_page_offset = -1;
      lseek(metadata_file_descriptor, sizeof(fType), SEEK_SET);
      write(metadata_file_descriptor, &current_write_page_index, sizeof(off_t));
    }
    else
    {
      current_read_page_offset = -1;
    }
  }
}

int DBFile::Close()
{
  try
  {
    CheckIfFilePresent();
    if (dirty)
    {
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

    return 1;
  }
  catch (runtime_error r)
  {
    cerr << "Trying to close a file which is not opened" << endl;
    return 0;
  }
  catch (...)
  {
    return 0;
  }
}

void DBFile::Load(Schema &f_schema, const char *loadpath)
{
  CheckIfFilePresent();
  Record *temp = new Record();
  FILE *table_file = fopen(loadpath, "r");

  if (table_file == NULL)
  {
    string err = "Error opening table file for load";
    err += loadpath;
    throw runtime_error(err);
  }

  count = 0;
  while (temp->SuckNextRecord(&f_schema, table_file) == 1)
  {
    if (temp != NULL)
    {
      count++;
      Add(*temp);
    }
  }
  FlushBuffer();
  cout << "Bulk Loaded " << count << " records" << endl;
}

void DBFile::Add(Record &rec)
{
  CheckIfFilePresent();
  // If it is not dirty empty out all the records read
  if (!dirty)
  {
    buffer->EmptyItOut();
    mode = writing;
    dirty = true;
  }

  // If The buffer is full: Flush the buffer to persistent storage
  // Else: just append to the buffer and set dirty variable
  // cout << buffer->GetNumRecords() << endl;
  if (buffer->Append(&rec) == 0)
  {
    FlushBuffer();
    buffer->Append(&rec); /* Need to append again as the previous append was
                             unsuccessful */
  }
}

void DBFile::FlushBuffer()
{
  int prev_write_page_index = current_write_page_index;

  Page *flush_to_page = new Page();
  // Check if any pages have been alloted before
  if (current_write_page_index < 0)
  {
    // No pages have been alloted before
    // This is the first page
    current_write_page_index++;

    // Set read page and read index
    current_read_page_index = current_write_page_index;
    current_read_page_offset = -1;
  }
  else
  {
    // Pages have been alloted; Check if last page has space
    persistent_file->GetPage(flush_to_page, current_write_page_index);
  }

  // Flush buffer to pages till the buffer is empty
  // The file page size may be smaller than the buffer page size hence
  // The entire buffer may not fit on the page
  bool empty_flush_to_page_flag = false;
  while (CopyBufferToPage(buffer, flush_to_page, empty_flush_to_page_flag) ==
         0)
  {
    persistent_file->AddPage(flush_to_page, current_write_page_index);
    current_write_page_index++;
    empty_flush_to_page_flag = true;
  }

  persistent_file->AddPage(flush_to_page, current_write_page_index);
  // If new page(s) was required
  if (prev_write_page_index != current_write_page_index)
  {
    // Update the metadata for the file
    lseek(metadata_file_descriptor, sizeof(fType), SEEK_SET);
    write(metadata_file_descriptor, &current_write_page_index, sizeof(off_t));
  }
}

int DBFile::CopyBufferToPage(Page *buffer, Page *flush_to_page,
                             bool empty_flush_to_page_flag)
{
  // Is it a new page and hence previous flush has already been written to file
  if (empty_flush_to_page_flag)
  {
    flush_to_page->EmptyItOut();
  }

  Record to_be_copied;
  while (buffer->GetFirst(&to_be_copied) != 0)
  {
    if (flush_to_page->Append(&to_be_copied) == 0)
    {
      buffer->Append(
          &to_be_copied); /* Need to append again to the buffer as the
                             GetFirst() will remove the record from buffer */
      return 0;
    }
  }
  return 1;
}

int DBFile::GetNext(Record &fetchme)
{
  CheckIfFilePresent();
  // If the buffer is dirty the records are to be written out to file before
  // being read
  if (dirty)
  {
    FlushBuffer();
    dirty = false;
  }

  // If records in all pages have been read
  if (current_read_page_index < 0 ||
      current_read_page_index > current_write_page_index)
  {
    return 0;
  }

  // Get the Page current_read_page_index from the file
  if (mode == writing || mode == idle)
  {
    persistent_file->GetPage(buffer, current_read_page_index);
    mode = reading;
  }

  // Increment to get the next record on the page no `current_read_page_index`
  current_read_page_offset++;
  // If there are no more records on the page `current_read_page_index` get next
  // page
  while (current_read_page_offset >= buffer->GetNumRecords() &&
         current_read_page_index <= current_write_page_index)
  {
    current_read_page_index++;
    // If there are no more pages remaining in the file
    // All records have been read
    if (current_read_page_index > current_write_page_index)
    {
      return 0;
    }
    current_read_page_offset = 0;
    persistent_file->GetPage(buffer, current_read_page_index);
  }

  // Read the next record now from appropriate page
  buffer->ReadNext(fetchme, current_read_page_offset);
  return 1;
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
  while (GetNext(fetchme) != 0)
  {
    ComparisonEngine comp;
    if (comp.Compare(&fetchme, &literal, &cnf))
    {
      return 1;
    }
    else
    {
      continue;
    }
  }
  return 0;
}

char *DBFile::GetMetaDataFileName(const char *file_path)
{
  string f_path_str(file_path);
  string metadata_file_extension(".header");
  size_t dot_pos = f_path_str.rfind('.');
  string metadata_file_name =
      f_path_str.substr(0, dot_pos) + metadata_file_extension;
  return (char *)metadata_file_name.c_str();
}

void DBFile::CheckIfFilePresent()
{
  if (!is_open)
  {
    throw runtime_error(
        "File destination is not open or created, Please create a file or open "
        "an existing one");
  }
}

bool DBFile::CheckIfCorrectFileType(fType type)
{
  switch (type)
  {
  case heap:
    return true;
    break;
  case sorted:
    cout << "Sorted Files: Not implemented yet! Coming soon " << endl;
    exit(0);
    break;
  case tree:
    cout << "Tree Files: Not implemented yet! Coming soon " << endl;
    exit(0);
    break;
  default:
    throw runtime_error("File type is incorrect or not supported");
  }
}

bool DBFile::CheckIfFileNameIsValid(const char *file_name)
{
  // TODO: check for file extension?
  if (file_name == NULL || *file_name == '\0')
  {
    throw runtime_error("File name is invalid");
  }
  else
  {
    return true;
  }
}

void DBFile::Instantiate()
{
  persistent_file = new File();
  buffer = new Page();
  dirty = false;
  is_open = false;
  mode = idle;
}

int DBFile::GetNumRecsInBuffer() { return buffer->GetNumRecords(); }

bool DBFile::WillBufferBeFull(Record &to_be_added)
{
  return buffer->IsPageFull(&to_be_added);
}

off_t DBFile::GetNumPagesInFile() { return persistent_file->GetLength(); }

off_t DBFile::GetCurrentReadPageIndex() { return current_read_page_index; }

off_t DBFile::GetCurrentWritePageIndex() { return current_write_page_index; }