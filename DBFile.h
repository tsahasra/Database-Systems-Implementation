#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"

typedef enum {heap, sorted, tree} fType;

// stub DBFile header..replace it with your own DBFile.h 

class GenericDBFile {
	
	public :
	File *f ;
	off_t numPages;
	Record * currRec;
	Page currPage;
	off_t pgindex;
	off_t recindex;

	GenericDBFile(){};
	virtual ~GenericDBFile(){
		delete f;
		delete currRec;
	};

	virtual void Load (Schema &myschema, const char *loadpath){};
	virtual int Close () {};
	virtual void MoveFirst (){};
	virtual void Add (Record &addme){};
	virtual int GetNext (Record &fetchme){};
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal){};

};

class HeapFile :  virtual public GenericDBFile{
	
	

	public :
	HeapFile(File *df);

	~HeapFile();

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};

class SortedFile : virtual public GenericDBFile {

	struct SortInfo { OrderMaker *myOrder; int runLength;
	}sortinf;

	bool writemode;
	BigQ *bigqobj;
	Pipe *inputpipe;
	Pipe *outpipe;
	Page *searchPage;
	off_t spgindex;
	OrderMaker *som;
	OrderMaker *lom;
	const char *tf_path;	

	public :
	

	SortedFile(void * sfin,File *df,const char *fpath,bool wmode);

	~SortedFile();

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	void MergeSortedData ();
};

class DBFile {

private:
	File datafile;
	off_t numPages;
	Record * currRec;
	Page currPage;
	off_t pgindex;
	off_t recindex;
	const char *fpath;
	GenericDBFile *gdbfile;
	

public:
	DBFile (); 
	~DBFile();

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
