#include "DBFile.h"
#include <unistd.h>
#include "HeapDBFile.h"
#include "SortedDBFile.h"

DBFile::DBFile() {}
DBFile::~DBFile() {}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
  try {
    CheckIfCorrectFileType(f_type);
    dbFile = GetDBFileInstance(f_type);
    return dbFile->Create(f_path, f_type, startup);
  } catch (runtime_error &e) {
    cerr << e.what() << endl;
    return 0;
  } catch (...) {
    return 0;
  }
}

int DBFile::Open(const char *fpath) {
  try {
    fType type = GetFileType(fpath);
    dbFile = GetDBFileInstance(type);
    return dbFile->Open(fpath);
  } catch (runtime_error &e) {
    cerr << e.what() << endl;
    return 0;
  } catch (...) {
    return 0;
  }
}

int DBFile::Close() { return dbFile->Close(); }

void DBFile::Load(Schema &myschema, const char *loadpath) {
  dbFile->Load(myschema, loadpath);
}

void DBFile::MoveFirst() { dbFile->MoveFirst(); }

void DBFile::Add(Record &addme) { dbFile->Add(addme); }

int DBFile::GetNext(Record &fetchme) { return dbFile->GetNext(fetchme); }

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
  return dbFile->GetNext(fetchme, cnf, literal);
}

bool DBFile::CheckIfCorrectFileType(fType type) {
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

char *DBFile::GetMetaDataFileName(const char *file_path) {
  string f_path_str(file_path);
  string metadata_file_extension(".header");
  size_t dot_pos = f_path_str.rfind('.');
  string metadata_file_name =
      f_path_str.substr(0, dot_pos) + metadata_file_extension;
  return (char *)metadata_file_name.c_str();
}

GenericDBFile *DBFile::GetDBFileInstance(fType type) {
  switch (type) {
    case heap:
      return new HeapDBFile();
    case sorted:
    case tree:
      return new SortedDBFile();
  }
}

fType DBFile::GetFileType(const char *fpath) {
  int file_mode = O_RDWR;
  int metadata_file_descriptor =
      open(GetMetaDataFileName(fpath), file_mode, S_IRUSR | S_IWUSR);

  // Read the current_page_index from metadata file
  lseek(metadata_file_descriptor, 0, SEEK_SET);

  // Initialize variables from meta data file
  // Current Meta Data variables  ->
  fType type;
  read(metadata_file_descriptor, &type, sizeof(fType));
  CheckIfCorrectFileType(type);

  switch (type) {
    case heap:
      return heap;

    case sorted:
      return sorted;

    case tree:
      return tree;
  }
}
