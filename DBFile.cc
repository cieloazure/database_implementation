#include "DBFile.h"
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fstream>
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
    CheckIfCorrectFileType(type);
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

string DBFile::GetMetaDataFileName(const char *file_path) {
  string f_path_str(file_path);
  string metadata_file_extension(".header");
  size_t dot_pos = f_path_str.rfind('.');
  string metadata_file_name =
      f_path_str.substr(0, dot_pos) + metadata_file_extension;
  return metadata_file_name;
}

GenericDBFile *DBFile::GetDBFileInstance(fType type) {
  switch (type) {
    case heap:
      return new HeapDBFile();
    case sorted:
      return new SortedDBFile();
    case tree:
      throw runtime_error("Tree file not yet implemented");
  }
}

fType DBFile::GetFileType(const char *fpath) {
  // TODO: Quick fix, find reason for open system call failing
  string s = GetMetaDataFileName(fpath);
  ifstream myFile(s, ios::in | ios::binary);
  fType type;
  myFile.read(reinterpret_cast<char *>(&type), sizeof(fType));
  if (!myFile) {
    cout << "Error occured in reading metadata file " << s << endl;
    throw runtime_error("Error occured in reading metadata file in DBFile");
    // An error occurred!
    // myFile.gcount() returns the number of bytes read.
    // calling myFile.clear() will reset the stream state
    // so it is usable again.
  }
  return type;
}
