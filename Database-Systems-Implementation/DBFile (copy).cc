
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
#include "BigQ.h"
//#include "BigQ.cc"
#include "Pipe.h"
//#include "Pipe.cc"
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <string>
#include <fstream>
#include <sstream>
#include <queue>
#include <map>
#include <list>

// stub file .. replace it with your own DBFile.cc

struct SortInfo{
    OrderMaker *somkr;
    int srunlength;
};


DBFile::DBFile () {
    
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {

    size_t sizeoffile = strlen(f_path);
    sizeoffile += 6;
    char mf_path[sizeoffile];
    sprintf(mf_path,"%s.meta\0",f_path);

    

    ofstream fs;
    fs.open (mf_path);
    
    if(f_type == heap) {
        gdbfile = new HeapFile(&datafile);
        fs << "HEAP\n";
    }
    else{
        gdbfile  = new SortedFile(startup,&datafile,f_path,false);
        fs << "SORTED\n";
        SortInfo * sftemp = (SortInfo *) startup;
        fs << sftemp->srunlength << endl;
        int numOfAttsInOm = sftemp->somkr->GetNumOfAtts();
        int wa[numOfAttsInOm];
	    Type wt[numOfAttsInOm];
        sftemp->somkr->GetAtts(wa);
        sftemp->somkr->GetTypes(wt);
        for(int i=0;i<numOfAttsInOm;++i){
            if(i < (numOfAttsInOm - 1))
                fs << wa[i] << ","<<wt[i]<<endl;
            else
                fs << wa[i] << ","<<wt[i];
        }
    }

    datafile.Open(0,(char *)f_path);
    
    fs.close();

    return 1;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {

    gdbfile->Load(f_schema,loadpath);
}

int DBFile::Open (const char *f_path) {
    size_t sizeoffile = strlen(f_path);
    sizeoffile += 5;
    char mf_path[sizeoffile];
    sprintf(mf_path,"%s.meta",f_path);

    ifstream fis;
    fis.open (mf_path);

    string str;

    if(!fis.eof()) {
        getline(fis,str); 
        if(str.compare("HEAP") == 0){
            gdbfile = new HeapFile(&datafile);
        }
        else if(str.compare("SORTED") == 0){
            getline(fis,str);
            int runl;
            sscanf(str.c_str(), "%d", &runl);
            int wa[MAX_ANDS];
            Type wt[MAX_ANDS];
            int index=0;
            int natts = 0;
            while (!fis.eof()){
                getline(fis,str,',');
                sscanf(str.c_str(), "%d", &wa[index]);
                getline(fis,str);
                sscanf(str.c_str(), "%d", &wt[index]);
                natts++;
                index++;
            }
            SortInfo *sinf = new SortInfo();
            sinf->somkr = new OrderMaker();
            sinf->somkr->SetNumOfAtts(natts);
            sinf->somkr->SetAtts(wa);
            sinf->somkr->SetTypes(wt);
            sinf->srunlength = runl;
            gdbfile = new SortedFile((void *)sinf,&datafile,f_path,false);
        }
    }

    
    fis.close();
    datafile.Open(1,(char *)f_path);

    return 1;
}

void DBFile::MoveFirst () {
    gdbfile->MoveFirst();   
}

int DBFile::Close () {

    return gdbfile->Close();

}

void DBFile::Add (Record &rec) {
    gdbfile->Add(rec);
}


int DBFile::GetNext (Record &fetchme) {
    return gdbfile->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    
    return gdbfile->GetNext(fetchme,cnf,literal);
}



HeapFile :: HeapFile(File *df) {
    	numPages = 0;
    	pgindex = 0;
    	f = df;
	}

void HeapFile::Load (Schema &f_schema, const char *loadpath) {

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

    if(currPage.GetNumOfRecords() > 0)
	    f->AddPage(&currPage,numPages);
    
    fclose(sf);
}

int HeapFile::Open (const char *f_path) {

    f->Open(1,(char *)f_path);
    return 1;
}

void HeapFile::MoveFirst () {
    currPage.EmptyItOut();
    f->GetPage(&currPage,0);
    currRec = currPage.GetFirstRecord(); 
    pgindex = 0;   
}

int HeapFile::Close () {
    f->Close();

    return 1;
}

void HeapFile::Add (Record &rec) {

    int result = currPage.Append(&rec); 

    if(result == 0){
        f->AddPage(&currPage,numPages);
        currPage.EmptyItOut();
        numPages++;
        currPage.Append(&rec);    
    }
         
}

int HeapFile::GetNext (Record &fetchme) {
    int mvres = currPage.CheckNextMove();
    if(mvres == 0){
        pgindex++;
        off_t NumOfPages = f->GetLength() - 2;
        if(pgindex > NumOfPages)
            return 0;
        currPage.EmptyItOut();
        f->GetPage(&currPage,pgindex);
        currRec = currPage.GetFirstRecord();
    }
    
    fetchme.Copy(currRec);
    currRec = currPage.MoveToNextRecord();    

    return 1;
}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine ce;

    while(GetNext(fetchme) != 0){
        if(ce.Compare(&fetchme,&literal,&cnf) != 0)
            return 1;
    }
    
    return 0;
}


SortedFile :: SortedFile(void * sfin,File *df,const char *fpath,bool wmode){
        numPages = 0;
        pgindex = 0;
        tf_path = fpath;
    	sortinf = *((SortInfo *)sfin);
        f = df;
        writemode = wmode;
	}

void SortedFile::Load (Schema &f_schema, const char *loadpath) {

    FILE *sf = fopen(loadpath,"r");

    if(!sf) {
        cerr << "ERROR! File to be loaded could not be opened" << endl;
        exit(1);
    }
    else{
        if(!writemode){
            Record temp;
            bigqobj = new BigQ(*inputpipe,*outpipe,*(sortinf.myOrder),sortinf.runLength); 
            int res = temp.SuckNextRecord(&f_schema,sf);
            while(res > 0){
                inputpipe->Insert(&temp); 
                res = temp.SuckNextRecord(&f_schema,sf);             
            }
            writemode = true;
        }
    }

    //MoveFirst();
    
    fclose(sf);
}

void SortedFile::Add (Record &rec) {
    
    if (!writemode) {
        inputpipe = new Pipe(100);
        outpipe = new Pipe(100);
        bigqobj = new BigQ(*inputpipe,*outpipe,*(sortinf.myOrder),sortinf.runLength);
    } 
    
    inputpipe->Insert(&rec);
    writemode = true;                 
}

void SortedFile :: MergeSortedData () {
   inputpipe->ShutDown();
    
    Record *rec1 , *rec2;
    rec1 = new Record();
    rec2 = new Record();

    Page tempPage;
	tempPage.EmptyItOut();
	
    Page sortedPage;
	sortedPage.EmptyItOut();

    off_t currPgNum = 0 , sortedPgNum = 0;

    bool pipeflag = true;
    bool fileflag = true;

    if(f->GetLength() == 0){
        fileflag = false;   
    }
    else{
        f->GetPage(&tempPage,currPgNum);
        tempPage.GetFirst(rec2);        
        currPgNum++;
    }

    if(outpipe->Remove(rec1) == 0){
        pipeflag = false;
    }
   
    ComparisonEngine ce;

    int tobeadded = 0 , counter = 0;

    size_t sizeoffile = strlen(tf_path);
    sizeoffile += 5;
    char mf_path[sizeoffile];
    sprintf(mf_path,".tmp\0",tf_path);
    
    File tempfile;

    tempfile.Open(0,mf_path);
    
    Record *rectemp;

    while(pipeflag && fileflag){
        rectemp = new Record();
        
        if(ce.Compare(rec1,rec2,sortinf.myOrder) < 0){
            rectemp->Copy(rec1);
            tobeadded = 0;
        }
        else{
            rectemp->Copy(rec2);
            tobeadded = 1;
        }
        
        int result = sortedPage.Append(rectemp);
        
        if(result == 0){
            tempfile.AddPage(&sortedPage,sortedPgNum);
            sortedPage.EmptyItOut();
            sortedPgNum++;
            sortedPage.Append(rectemp);
        }
        
        delete rectemp;
        rectemp = NULL;

        if(tobeadded == 0){
            if(rec1 != NULL)
                delete rec1;
            rec1 = new Record();
            Record temp;
            if(outpipe->Remove(&temp) == 0){
                pipeflag = false;
                continue;
            }
            rec1->Copy(&temp);
        }
        else{
            if(rec2 != NULL)
                delete rec2;
            rec2 = new Record();
            int fres = tempPage.GetFirst(rec2);
            
            if(fres == 0){
                if(currPgNum <= (f->GetLength() - 2)){
                    tempPage.EmptyItOut();
                    f->GetPage(&tempPage,currPgNum);
                    currPgNum++;
                    tempPage.GetFirst(rec2);
                }
                else{
                    fileflag = false;
                    continue;
                }
                
            }
           
        }
        
    }

    int res = 0;

    while(fileflag){
        
            res = sortedPage.Append(rec2);
            
            if(res == 0){
                tempfile.AddPage(&sortedPage,sortedPgNum);
                sortedPage.EmptyItOut();
                sortedPgNum++;
                sortedPage.Append(rec2);
            }

            if(rec2 != NULL)
                delete rec2;
            
            rec2 = new Record();
            int fres = tempPage.GetFirst(rec2);
            if(fres == 0){
                if(currPgNum <= (f->GetLength() - 2)){
                    tempPage.EmptyItOut();
                    f->GetPage(&tempPage,currPgNum);
                    currPgNum++;
                    tempPage.GetFirst(rec2);
                }
                else{
                    fileflag = false;
                    continue;
                }
                
            }

            //cout<<"here4"<<endl;
    }
    
    while(pipeflag){
             //cout<<"here5"<<endl;
            res = sortedPage.Append(rec1);
            
             if(res == 0){
                //cout<<"here51"<<endl;
                tempfile.AddPage(&sortedPage,sortedPgNum);
                sortedPage.EmptyItOut();
                sortedPgNum++;
                sortedPage.Append(rec1);
            }

            if(rec1 != NULL)
                delete rec1;
            
            rec1 = new Record();
            Record temp;
            if(outpipe->Remove(&temp) == 0){
                pipeflag = false;
                continue;
            }
            rec1->Copy(&temp);
            
    }
    
    if(sortedPage.GetNumOfRecords() > 0)
        tempfile.AddPage(&sortedPage,sortedPgNum);

    outpipe->ShutDown();

    f->Close();
    tempfile.Close();

    remove(tf_path);    
    rename(mf_path,tf_path);
    f->Open(1,(char *)tf_path);

    if(rec1 != NULL)
        delete rec1;

    if(rec2 != NULL)
        delete rec2;

    delete bigqobj;
    bigqobj = NULL;
    delete inputpipe;
	inputpipe = NULL;
	delete outpipe;
	outpipe = NULL;
}

void SortedFile::MoveFirst () {
    if(writemode){
        MergeSortedData();
    }
    currPage.EmptyItOut();
    f->GetPage(&currPage,0);
    currRec = currPage.GetFirstRecord();
    pgindex = 0;   
    writemode = false;
}


int SortedFile::GetNext (Record &fetchme) {
    if(writemode){
        MergeSortedData();
    }
    
    int mvres = currPage.CheckNextMove();
    if(mvres == 0){
        pgindex++;
        off_t NumOfPages = f->GetLength() - 2;
        if(pgindex > NumOfPages)
            return 0;
        currPage.EmptyItOut();
        f->GetPage(&currPage,pgindex);
        currRec = currPage.GetFirstRecord();
    }
    
    fetchme.Copy(currRec);
    currRec = currPage.MoveToNextRecord();    

    return 1;
}

int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    
    if (writemode) {
		MergeSortedData();
	}
	
	ComparisonEngine comparer;
	
	if (searchPage == NULL) {
		OrderMaker *msom = sortinf.myOrder;
		
		if (som != NULL) {
			delete som;
		}

		som = new OrderMaker();

		if (lom!= NULL) {
			delete lom; 
		}

		lom = new OrderMaker();
		
		for (int i = 0; i < msom->GetNumOfAtts(); i++) {
			int att = msom->GetAttribute(i);
			int literalIndex;
			if ((literalIndex = cnf.HasEqualityCheck(att)) != -1) {
				som->AddAttribute(att, msom->GetType(i));
				lom->AddAttribute(literalIndex, msom->GetType(i));
			} else {
				break;
			}
		}
		
		off_t high = f->GetLength() - 2;
		off_t low = 0;
		off_t mid = (high + low) / 2;
		searchPage = new Page();
		int result;
		int pageWithEqualRecordFirst = -1;
		while (low <= high) {
			mid = (high + low) / 2;
			f->GetPage(searchPage, mid);
			spgindex = mid;
			searchPage->GetFirst(&fetchme);
			result = comparer.Compare(&fetchme, som, &literal, lom);
			if (result < 0) {
				int nextResult;
				do {
					nextResult = searchPage->GetFirst(&fetchme);
				} while (nextResult && (result = comparer.Compare(&fetchme, som, &literal, lom)) < 0);
				if (result > 0 && nextResult) {
					return 0;
				}
				if (result == 0) {
					break;
				}
				low = mid + 1;
			} else {
				if (result == 0) {
					pageWithEqualRecordFirst = mid;
				}
				result = 1;
				high = mid - 1;
			}
		}
		
		if (result == 0) {
		} else if (pageWithEqualRecordFirst > -1) {
			f->GetPage(searchPage, mid);
			spgindex = mid;
			searchPage->GetFirst(&fetchme);
		} else {
			delete searchPage;
			searchPage = NULL;
			return 0;
		}
	} else {
		int result = searchPage->GetFirst(&fetchme);
		if (result == 0){
			spgindex++;
			if (spgindex < f->GetLength() - 1) {
				f->GetPage(searchPage, spgindex);
				searchPage->GetFirst(&fetchme);
			} else {
				return 0;
			}
		}
	}	

	while (!comparer.Compare(&fetchme, &literal, &cnf)) {
		if (comparer.Compare(&fetchme, som, &literal, lom) != 0) {
			return 0;
		}
		int result = searchPage->GetFirst(&fetchme);
		if (result == 0){
			spgindex++;
			if (spgindex < f->GetLength() - 1) {
				f->GetPage(searchPage, spgindex);
				searchPage->GetFirst(&fetchme);
			} else {
				return 0;
			}
		}
	}
	return 1;

}

int SortedFile::Close () {
    if(writemode){
        MergeSortedData();
    }

    f->Close();

    return 1;
}
