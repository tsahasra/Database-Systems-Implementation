#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "TwoWayList.h"
#include "TwoWayList.cc"
#include <iostream>
#include <cstdlib>

// stub file .. replace it with your own DBFile.cc


DBFile::DBFile () {
    numPages = 0;
    pgindex = 0;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    
    datafile.Open(0,(char *)f_path);
    
    return 1;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {

    FILE *sf = fopen(loadpath,"r");

    if(!sf) {
        cerr << "ERROR! File to be loaded could not be opened" << endl;
        exit(1);
    }
    else{
        Record temp;
        int counter;
        counter = 0;
        int res = temp.SuckNextRecord(&f_schema,sf);
        while(res > 0){
            Add(*&temp);  
            counter++;
            res = temp.SuckNextRecord(&f_schema,sf);             
        }
    }

    MoveFirst();
    
    fclose(sf);
}

int DBFile::Open (const char *f_path) {

    datafile.Open(1,(char *)f_path);
    return 1;
}

void DBFile::MoveFirst () {
    currPage.EmptyItOut();
    datafile.GetPage(&currPage,0);
    currRec = currPage.GetFirstRecord(); 
    pgindex = 0;   
}

int DBFile::Close () {
    datafile.Close();

    return 1;
}

void DBFile::Add (Record &rec) {
    AddToHeap(rec);
}

void DBFile :: AddToHeap(Record &rec) {
    off_t fileSize = datafile.GetLength();

    if(fileSize == 0){
        Page p;
        currRec = &rec;
        datafile.AddPage(&p,numPages);
        datafile.GetPage(&currPage,numPages);
    }
    else{
        numPages = fileSize - 2;
        currPage.EmptyItOut();
        datafile.GetPage(&currPage,numPages);
    }
    
    int result = currPage.Append(&rec); 

    if(result == 0){
        datafile.AddPage(&currPage,numPages);
        currPage.EmptyItOut();
        numPages++;
        currPage.Append(&rec);    
    }

    datafile.AddPage(&currPage,numPages);
         
}

int DBFile::GetNext (Record &fetchme) {
    int mvres = currPage.CheckNextMove();
    if(mvres == 0){
        pgindex++;
        off_t NumOfPages = datafile.GetLength() - 2;
        if(pgindex > NumOfPages)
            return 0;
        currPage.EmptyItOut();
        datafile.GetPage(&currPage,pgindex);
        currRec = currPage.GetFirstRecord();
    }
    
    fetchme.Copy(currRec);
    currRec = currPage.MoveToNextRecord();    

    return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine ce;

    while(GetNext(fetchme) != 0){
        if(ce.Compare(&fetchme,&literal,&cnf) != 0)
            return 1;
    }
    
    return 0;
}


