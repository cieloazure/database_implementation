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
    current_record = NULL;
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

      // Write metadata variables 
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
        int lineno = 1;
        string metadata;
        while(metadata_file){
            getline(metadata_file, metadata);
            switch(lineno){
                case 1: 
                    current_page_index = stoi(metadata);
                    break;
                case 2:
                    type = (fType) stoi(metadata);
                    break;
                    // add other cases here
            }
            lineno++;
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
        // Close the persistent file
        persistent_file -> Close();
        
        // Close the metadata file
        metadata_file.close();

        // Set current_page_index to an invalid value
        current_page_index = -1;

        // Set file path to NULL to indicate the file operations has fininshed
        file_path = NULL;
        return 1;
    }catch(...){
        return 0;
    }
}

void DBFile::Add (Record &rec) {
    // This page will be added with the new record
    Page* add_me;

    // Check if any pages have been alloted before
    if(current_page_index < 0){
        // No pages have been alloted before
        current_page_index++;
        Page* new_page = new Page();
        if(new_page -> Append(&rec) == 0){
            cerr << "Page Full! Likely an error as we just created a page";
        }
        add_me = new_page;
    }else{
        // Pages have been alloted; Check if last page has space 
        Page* fetched_page = new Page();
        persistent_file -> GetPage(fetched_page, current_page_index);

        // The fetched page is full 
        // Allocate a new page
        if(fetched_page -> Append(&rec) == 0){
            persistent_file -> AddPage(fetched_page, current_page_index);
            current_page_index++;
            Page* new_page = new Page();
            if(new_page -> Append(&rec) == 0){
                cerr << "Page full! Likely an error as we just created a page";
            }
            add_me = new_page;
        }else{
            add_me = fetched_page;
        }
    }

    // Add the new or last page in the persistent file
    persistent_file -> AddPage(add_me, current_page_index);

    // Update the metadata for the file
    metadata_file.seekg(0, ios::beg);
    metadata_file << current_page_index << endl;

    // Update the current pointer
    if(current_record == NULL){
        current_record = &rec;
    }
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
