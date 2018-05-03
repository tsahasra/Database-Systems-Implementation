#include "BigQ.h"
#include "Schema.h"
#include "File.h"
#include <vector>
#include <algorithm>
#include <fcntl.h>
#include <queue>
#include <map>
#include <fstream>




priorityDataStructure :: priorityDataStructure(Record *r, Page *pptr , off_t runpageno , off_t filepageno , off_t rs){
	this->rptr = r;
	this->pgptr = pptr;
	this->rpgno = runpageno;
	this->fpgno = filepageno;
	this->runsize = rs;
}

bool Comp :: operator() (const priorityDataStructure *p1, const priorityDataStructure *p2){
	
	ComparisonEngine CE;
	
	Record *r1 = p1->rptr;
	Record *r2 = p2->rptr;

		int result = CE.Compare(r1,r2,sort_order);

		if(result < 0){
			return false;
		}
		else{
			return true;
		}
	
}

class Sort_Run 
{
	OrderMaker *sort_order;
public:
	Sort_Run(OrderMaker *order)
	{		
		sort_order = order;		
	}
	
	ComparisonEngine comp;

	bool operator()(Record * r1, Record * r2)
	{
		
		if (comp.Compare(r1, r2, sort_order) < 0)
		{			
			return true;
		}
		else{
			return false;
		} 
			
	}
};

bool BigQ::fexists(const char *filename) {
  std::ifstream ifile(filename);
  return (bool)ifile;
}

void * BigQ:: createRunsAndSort(void *arg) {
	
	bigqutil *bqu = (bigqutil *)arg;

	Pipe *in = bqu->inputpipe;
	Pipe *out = bqu->outpipe;
	OrderMaker omkr = bqu->om;
	int runl = bqu->runlength;
	
	Page currPage;
	off_t numPages = 0;
	off_t NumOfPagesInRun = 0;
	map <int , int>  m;

	/*char tmpfilename[L_tmpnam];
	char * strfilename = tmpnam (tmpfilename);
	tempfile.Open(0,strfilename);*/

	int num=rand()%10000;

	char fileName[10];
	sprintf(fileName,"%d.%s\0",num,"tmp");
	while(fexists(fileName)){
		num=rand()%10000;
		sprintf(fileName,"%d.%s\0",num,"tmp");
	}
	//cout<<fileName<<endl;

	File tempfile;

	tempfile.Open(0,fileName);

	int rl = 0;
	Record tmpr;
	//Schema *s = new Schema("catalog","partsupp");
	vector <Record *> runvector;
	Page p;
	int rcount = 0;
	vector<Record *> :: iterator it;
	int cnt = 0;

	while(in->Remove(&tmpr)){
		//cout<<"phase1 "<<fileName<<endl;
		Record robjt;
		robjt.Copy(&tmpr);
		
		if(p.Append(&tmpr) == 0){
			p.EmptyItOut();
			p.Append(&tmpr);
			rl++;
		}

		if(rl == runl){
			//runvector.erase(runvector.end() - 1);
			rcount++;
			stable_sort(runvector.begin(),runvector.end(),Sort_Run(&omkr));
			NumOfPagesInRun = 0;
			for(it=runvector.begin();it!=runvector.end();it++){
				Record *rt = *it;

				int result = currPage.Append(*it); 

    			if(result == 0){
        			tempfile.AddPage(&currPage,numPages);
					NumOfPagesInRun++;
        			currPage.EmptyItOut();
        			numPages++;
        			currPage.Append(*it);    
    			}

				delete rt;
			}

			if(currPage.GetNumOfRecords() > 0){
				tempfile.AddPage(&currPage,numPages);
				NumOfPagesInRun++;
				m[rcount] = NumOfPagesInRun;
				numPages++;
				currPage.EmptyItOut();
			}
			cnt += runvector.size();
			//cout << "No of records in run = " << cnt<<endl;
			runvector.clear();
			rl = 0;
		}

		runvector.push_back(new Record());
		(runvector.at(runvector.size()-1))->Copy(&robjt);

	}

	

	if(p.GetNumOfRecords() > 0){
		rcount++;
		stable_sort(runvector.begin(),runvector.end(),Sort_Run(&omkr));
		NumOfPagesInRun = 0;

		for(it=runvector.begin();it!=runvector.end();it++){
			Record *rt = *it;
			int result = currPage.Append(*it); 

    		if(result == 0){
        		tempfile.AddPage(&currPage,numPages);
				NumOfPagesInRun++;
        		currPage.EmptyItOut();
        		numPages++;
        		currPage.Append(*it);    
    		}

			delete rt;
		}

		if(currPage.GetNumOfRecords() > 0){
			tempfile.AddPage(&currPage,numPages);
			NumOfPagesInRun++;
			m[rcount] = NumOfPagesInRun;
			numPages++;			
			currPage.EmptyItOut();
		}
		cnt += runvector.size();
		//cout << "No of records in run = " << cnt <<endl;

		runvector.clear();

		//cout<<"No Of Runs "<<rcount<<" ANd" << fileName<<endl;
	}

	
	priority_queue <priorityDataStructure *, std::vector<priorityDataStructure *>, Comp> minQueue(&(omkr));
		


	//cout<<"Initializing Second Phase ..."<<fileName<<endl;	
	int temp=0;
	int runCounter=1;
	int index = 0;
	//Page pglist [rcount];
	while(temp <= (tempfile.GetLength()-2)){		

		Record robj;
		Page pg;

		//tempfile.AddPage(&pglist[index],temp);
		priorityDataStructure pds(new Record(),new Page(),1,temp,m[runCounter]);	
		tempfile.GetPage(pds.pgptr,temp);
		pds.pgptr->GetFirst(&robj);
		(pds.rptr)->Copy(&robj);

		temp = temp + pds.runsize;
		minQueue.push(new priorityDataStructure(pds));		
		
		runCounter++;
		index++;
		//cout<<" index "<<index<<" "<<fileName<<endl;
	}

	
	int qcount = 0;
	//cout<<"Executing Second Phase ..."<<fileName<<endl;
	
	while(!minQueue.empty()){
		//cout<<"phase2 "<<fileName<<endl;
		priorityDataStructure *minRecord = minQueue.top();
		minQueue.pop();				
		out->Insert(minRecord->rptr);
		++qcount;
		
		Record r;
		
		if((minRecord->pgptr)->GetNumOfRecords() != 0){
			minRecord->pgptr->GetFirst(&r);
			minRecord->rptr->Copy(&r);
			minQueue.push(new priorityDataStructure(*minRecord));	
			minRecord->rptr = NULL;
			minRecord->pgptr = NULL;	
			//tempfile.AddPage(&po,currentPage);
		}
		else {
			if(minRecord->fpgno == (tempfile.GetLength() -2))
				continue;
			if(minRecord->rpgno < minRecord->runsize){
				++(minRecord->rpgno);
				++(minRecord->fpgno);
				//currentPage = minRecord->fpgno;
				tempfile.GetPage(minRecord->pgptr, minRecord->fpgno);
				minRecord->pgptr->GetFirst(&r);
				minRecord->rptr->Copy(&r);
				minQueue.push(new priorityDataStructure(*minRecord));	
				minRecord->rptr = NULL;
				minRecord->pgptr = NULL;
				//tempfile.AddPage(&po,currentPage);
			}
						
		}
			
		delete minRecord;
		
	}
	
	//cout<<"reachedhere "<<fileName<<endl;
	tempfile.Close();
	remove(fileName);
	out->ShutDown();
}


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	
	// read data from in pipe sort them into runlen pages
	pthread_t wthread;
	
	bigqutil *bq = new bigqutil();

	bq->inputpipe = &in;
	bq->outpipe = &out;
	bq->om = sortorder;
	bq->runlength = runlen;


	pthread_create(&wthread,NULL,createRunsAndSort,(void *) bq);

    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	//out.ShutDown ();
	//pthread_join (wthread, NULL);
}

BigQ::~BigQ () {
}

