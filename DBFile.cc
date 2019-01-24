#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
    // dbFileInstance = new File();
    persistent_file = new File();
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    try {
      // Set the type of file
      type = f_type;

      // Set file_path for persistent file
      file_path = f_path;

      // Open persistent file
      persistent_file->Open(0, (char *)file_path);

      // Set current page index to -1 as no pages exist yet
      current_page_index = -1;

      // Open metadata file
      metadata_file.open(GetMetaDataFileName(file_path), ios::trunc | ios::out | ios::in);

      // Check if the metadata file has opened
      if (!metadata_file.is_open()) {
        string err("Error creating metadata file for ");
        err += file_path;
        err += " does not exist\n";
        throw runtime_error(err);
      }

      // Write current_page_index to meta data file
      metadata_file << current_page_index << endl;

      // Write fType to meta data file
      metadata_file << type << endl;

      // No exception occured
      return 1;
    } catch(runtime_error &e){
        cerr << e.what() << endl;
        return 0;
    } catch (...) {
      return 0;
    }
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
}

int DBFile::Open (const char *f_path) {
    try{
        // Set the file_path for persistent file
        file_path = f_path;

        // Open persistent file
        persistent_file -> Open(1, (char *)file_path);

        // Open metadata file
        metadata_file.open(GetMetaDataFileName(file_path), ios::app | ios::out | ios::in);

        // Check if the metadata file exists
        if(!metadata_file.eof()){
            string err("Metadata file for ");
            err += file_path;
            err += " does not exist\n";
            throw invalid_argument(err);
        }

        // Read the current_page_index from metadata file
        metadata_file.seekg(0, ios::beg);

        // Initialize variables from meta data file
        // Current Meta Data variables -> 
        // * current_page_index(int)
        // * file_type(enum fType)
        int count = 1;
        string metadata;
        while(metadata_file){
            getline(metadata_file, metadata);
            switch(count){
                case 1: 
                    current_page_index = stoi(metadata);
                    break;
                    // add other cases here
                case 2:
                    type = (fType)stoi(metadata);
                    break;
            }
            count++;
        }

        // No exception occured
        return 1;
    }catch(invalid_argument &e){
        cerr << e.what() << endl;
        return 0;
    }catch(...){
        return 0;
    }
}

void DBFile::MoveFirst () {
}

int DBFile::Close () {
    try{
        metadata_file.close();
        persistent_file -> Close();
        current_page_index = -1;
        file_path = NULL;
        return 1;
    }catch(...){
        return 0;
    }
}

void DBFile::Add (Record &rec) {
}

int DBFile::GetNext (Record &fetchme) {
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}

const string DBFile::GetMetaDataFileName(const char *file_path){
  string f_path_str(file_path);
  string metadata_file_extension(".header");
  size_t dot_pos = f_path_str.rfind('.');
  string metadata_file_name = f_path_str.substr(0, dot_pos) + metadata_file_extension;
  return metadata_file_name;
}
