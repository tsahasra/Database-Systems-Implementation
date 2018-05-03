#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "ComparisonEngine.h"

using namespace std;

class bigqutil{
	
	public:

	Pipe *inputpipe;
	Pipe *outpipe;
	OrderMaker om;
	int runlength;
	
};

class priorityDataStructure{

private:

public:
	
	Record *rptr;
	Page *pgptr;
	off_t rpgno;
	off_t fpgno;
	off_t runsize; 
	
	priorityDataStructure(Record *r, Page *pptr , off_t runpageno , off_t filepageno , off_t rs);

	priorityDataStructure(priorityDataStructure &copyMe){
		this->rptr = copyMe.rptr;
		this->pgptr = copyMe.pgptr;
		this->rpgno = copyMe.rpgno;
		this->fpgno = copyMe.fpgno;
		this->runsize = copyMe.runsize;
	};

	~priorityDataStructure() {
	};
};

class Comp {

private:
	OrderMaker *sort_order;

public:
	Comp(OrderMaker *om) {
		sort_order = om;
	}
	bool operator()(const priorityDataStructure *p1, const priorityDataStructure *p2);

};

class BigQ {
	
public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
	static void *createRunsAndSort(void *arg);
	static void writeRecord(Record &rec, File *f);
	static bool fexists(const char *filename);
};

#endif
