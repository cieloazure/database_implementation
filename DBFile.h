#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <string.h>
#include <iostream>
#include <fstream>


typedef enum {heap, sorted, tree} fType;

// stub DBFile header..replace it with your own DBFile.h 

class DBFile {
private:

	const char* file_path;
	fType type;
	File *persistent_file;
	int metadata_file_descriptor;

	int current_write_page_index;
	int current_read_page_index;
	int current_read_page_offset;


public:
	DBFile (); 

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

private:
	//const string GetMetaDataFileName(const char *file_path);
	char* GetMetaDataFileName(const char *file_path);
};
#endif
