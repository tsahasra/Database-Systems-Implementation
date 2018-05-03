#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class Param{

	public:
	Pipe *inpipel;
	Pipe *inpiper;
	Pipe *inpipe;
	Pipe *outpipe;
	DBFile *infile;
	Function computeme;
	Schema *myschema;
	FILE *outfile;
	int *keepme;
	int numattsinput;
	int numattsoutput;	
	OrderMaker groupatts;
	CNF cnfop;
	Record lit;

};

class RelationalOp {
	public:

	pthread_t rothread;

	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
	// pthread_t thread;
	// Record *buffer;
	
	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void *SelectFileRun(void *arg);
};

class SelectPipe : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void *SelectPipeRun(void *arg);
};

class Project : public RelationalOp { 
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void *ProjectRun(void *arg);
};

class Join : public RelationalOp { 
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void *JoinRun(void *arg);
	static void sortMergeJoin(Pipe *inpipel , Pipe *inpiper , Pipe *outpipe , CNF cnfop , Record lit , OrderMaker *oml , OrderMaker *omr);
	static void blockNestedLoopJoin(Pipe *inpipel , Pipe *inpiper , Pipe *outpipe , CNF cnfop , Record lit);
};

class DuplicateRemoval : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void *DuplicateRemovalRun(void *arg);
};

class Sum : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void *SumRun(void *arg);
};

class GroupBy : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void *GroupByRun(void *arg);
};

class WriteOut : public RelationalOp {
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void *WriteOutRun(void *arg);
};
#endif
