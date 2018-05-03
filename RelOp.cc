#include "RelOp.h"
#include <string.h>
#include <sstream>
#include <list>

// **************** Select File **************** //

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	
	Param *par = new Param();

	par->infile = &inFile;
	par->outpipe = &outPipe;
	par->cnfop = selOp;
	par->lit = literal;

	pthread_create(&rothread,NULL,SelectFileRun,(void *)par);
}

void SelectFile::WaitUntilDone () {
	pthread_join (rothread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}

void * SelectFile::SelectFileRun(void *arg) {
	
	Param *par = (Param *)arg;
	//cout<<"sf-1"<<endl;
	(par->infile)->MoveFirst();
	//cout<<"sf-2"<<endl;

	Record fetchme;

	int count = 0;

	while(par->infile->GetNext(fetchme,(par->cnfop),(par->lit))){

		par->outpipe->Insert(&fetchme);

	}

	par->outpipe->ShutDown();

}



// **************** Select Pipe **************** //

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {

	Param *par = new Param();

	par->inpipe = &inPipe;
	par->outpipe = &outPipe;
	par->cnfop = selOp;
	par->lit = literal;

	pthread_create(&rothread,NULL,SelectPipeRun,(void *)par);
}

void SelectPipe::WaitUntilDone () {
	pthread_join (rothread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {

}

void * SelectPipe::SelectPipeRun(void *arg) {

	Param *par = (Param *)arg;

	Record fetchme;

	ComparisonEngine ce;

	while(par->inpipe->Remove(&fetchme)){
		if(ce.Compare(&fetchme,&(par->lit),&(par->cnfop)) != 0)
			par->outpipe->Insert(&fetchme);
	}

	par->outpipe->ShutDown();
}



// **************** Project **************** //

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) { 
	
	Param *par = new Param();

	par->inpipe = &inPipe;
	par->outpipe = &outPipe;
	par->keepme = keepMe;
	par->numattsinput = numAttsInput;
	par->numattsoutput = numAttsOutput;

	pthread_create(&rothread,NULL,ProjectRun,(void *)par);
}
	
void Project::WaitUntilDone () { 
	pthread_join (rothread, NULL);
}
	
void Project::Use_n_Pages (int n) { 

}

void * Project::ProjectRun (void *arg) {

	Param *par = (Param *)arg;

	Record fetchme;

	int count = 0;

	while(par->inpipe->Remove(&fetchme)){
		//cout << "Project 1"<<endl;
		fetchme.Project(par->keepme,par->numattsoutput,par->numattsinput);
		//cout << "Project 2"<<endl;
		par->outpipe->Insert(&fetchme);
		//count++;
		//cout<<"Project - "<<count<<endl;
		//cout << "Project 3"<<endl;
	}

	par->outpipe->ShutDown();

}



// **************** Join **************** //

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 
	
	Param *par = new Param();

	par->inpipel = &inPipeL;
	par->inpiper = &inPipeR;
	par->outpipe = &outPipe;
	par->cnfop = selOp;
	par->lit = literal;

	pthread_create(&rothread,NULL,JoinRun,(void *)par);
}
	
void Join::WaitUntilDone () { 
	pthread_join (rothread, NULL);
}
	
void Join::Use_n_Pages (int n) { 

}

void * Join::JoinRun (void *arg) {
	
	Param *par = (Param *)arg;

	OrderMaker *oml = new OrderMaker() , *omr = new OrderMaker();

	int domergejoin = par->cnfop.GetSortOrders(*oml,*omr);

	if(domergejoin) {
		sortMergeJoin(par->inpipel,par->inpiper,par->outpipe,par->cnfop,par->lit , oml , omr);
	}
	else{
		blockNestedLoopJoin(par->inpipel,par->inpiper,par->outpipe,par->cnfop,par->lit);
	}

	par->outpipe->ShutDown();

}

void Join :: sortMergeJoin(Pipe *inpipel , Pipe *inpiper , Pipe *outpipe , CNF cnfop , Record lit , OrderMaker *oml , OrderMaker *omr) {
	Record lrec;
	Record rrec;

	Pipe *outpipel = new Pipe(100);
	Pipe *outpiper = new Pipe(100);

	BigQ bq1(*inpipel,*outpipel,*oml,1);
	BigQ bq2(*inpiper,*outpiper,*omr,1);

	int lempty = outpipel->Remove(&lrec);	
	int rempty = outpiper->Remove(&rrec);

	int jc = 0;
	ComparisonEngine ce;
	int compres = 0 , lattnum = 0 , rattnum = 0;

	vector<Record *> lvec;
	//list<Record *> rvec;

	while(lempty !=0 && rempty != 0) {
		//cout<<"join1"<<endl;
		compres = ce.Compare(&lrec,oml,&rrec,omr);

		if(compres < 0) {
			//cout<<"join2"<<endl;
			lempty = outpipel->Remove(&lrec);
			//cout<<"join3"<<endl;
			continue;
		}
		else if(compres > 0) {
			//cout<<"join4"<<endl;
			rempty = outpiper->Remove(&rrec);
			//cout<<"join5"<<endl;
			continue;
		}
		else{
			//cout<<"join6"<<endl;			

			lvec.push_back(new Record());
			(lvec.back())->Copy(&lrec);

			lempty = outpipel->Remove(&lrec);

			Record * temp = lvec.back(); 

			//cout<<"join7"<<endl;
			while(lempty != 0) {
				if(ce.Compare(&lrec,temp,oml) == 0) {
					lvec.push_back(new Record());
					(lvec.back())->Copy(&lrec);
				}
				else
					break;
				lempty = outpipel->Remove(&lrec);
			}


			vector <Record *> :: iterator it;
			
			Record finrec;
			
			lattnum = ((int*) lrec.bits)[1]/sizeof(int)-1;
			rattnum = ((int*) rrec.bits)[1]/sizeof(int)-1;

			
			int attrsToKeep[lattnum + rattnum];

			int numAttToKeep = lattnum + rattnum;

			int k = 0;
             
	        for (int i = 0; i < lattnum; i++) {
                attrsToKeep[k] = i;
				k++;
			}

            for (int i = 0; i < rattnum; i++) {
                attrsToKeep[k] = i;
				k++;
            }

			
			for(it=lvec.begin();it!=lvec.end();++it){
				Record *lr = *it;
				
				while(rempty && ce.Compare(lr,oml,&rrec,omr) == 0){
					Record rr;
					rr.Copy(&rrec);
					finrec.MergeRecords(*it,&rr,lattnum,rattnum,attrsToKeep,numAttToKeep,lattnum);
					outpipe->Insert(&finrec);

					rempty = outpiper->Remove(&rrec);
				}

				delete lr;
				
			}

			lvec.clear();
			
		}
		
	}

}

void Join :: blockNestedLoopJoin(Pipe *inpipel , Pipe *inpiper , Pipe *outpipe , CNF cnfop , Record lit) {
	
	list<Record *> lvec;
	list<Record *> rvec;

	Record lrec , rrec;

	int lempty = inpipel->Remove(&lrec);
	int rempty = inpiper->Remove(&rrec);

	lvec.push_back(new Record());
	(lvec.back())->Copy(&lrec);

	rvec.push_back(new Record());
	(rvec.back())->Copy(&rrec);

	int lattnum = ((int*) lrec.bits)[1]/sizeof(int)-1;
	int rattnum = ((int*) rrec.bits)[1]/sizeof(int)-1;

	int attrsToKeep[lattnum + rattnum];

	int numAttToKeep = lattnum + rattnum;

	int k = 0;
             
	for (int i = 0; i < lattnum; i++) {
        attrsToKeep[k] = i;
		k++;
	}

    for (int i = 0; i < rattnum; i++) {
        attrsToKeep[k] = i;
		k++;
    }

	while(inpipel->Remove(&lrec)) {
		lvec.push_back(new Record());
		(lvec.back())->Copy(&lrec);
	}

	while(inpiper->Remove(&rrec)) {
		rvec.push_back(new Record());
		(rvec.back())->Copy(&rrec);
	}



	list<Record *> :: iterator it;
	list<Record *> :: iterator jt;

	ComparisonEngine ce;
	Record finrec;

	for(it=lvec.begin();it!=lvec.end();++it){
		Record lr = *(*it);
		for(jt=rvec.begin();jt!=rvec.end();++jt){
			Record rr ;
			rr.Copy(*jt);
			if(ce.Compare(&lr,&rr,&lit,&cnfop) != 0) {
				finrec.MergeRecords(&lr,&rr,lattnum,rattnum,attrsToKeep,numAttToKeep,lattnum);
				outpipe->Insert(&finrec);
			}
		}
	}

	outpipe->ShutDown();
}



// **************** DuplicateRemoval **************** //

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { 

	Param *par = new Param();
	
	par->inpipe = &inPipe;
	par->outpipe = &outPipe;
	par->myschema = &mySchema;

	pthread_create(&rothread,NULL,DuplicateRemovalRun,(void *)par);
}
	
void DuplicateRemoval::WaitUntilDone () { 
	pthread_join (rothread, NULL);
}
	
void DuplicateRemoval::Use_n_Pages (int n) { 

}

void * DuplicateRemoval::DuplicateRemovalRun (void *arg) {

	Param *par = (Param *)arg;

	Record fetchme;
	Record *prev = new Record();

	Pipe *tempoutpipe = new Pipe(100);

	OrderMaker *om = new OrderMaker();

	par->myschema->GetSortOrder(*om);
	
	BigQ *bigq = new BigQ(*par->inpipe,*tempoutpipe,*om,1);
	
	ComparisonEngine ce;

	
	if(tempoutpipe->Remove(&fetchme)){
		prev->Copy(&fetchme);
		par->outpipe->Insert(&fetchme);
	}

	

	while(tempoutpipe->Remove(&fetchme)) {
		if(ce.Compare(&fetchme,prev,om) != 0){
			prev->Copy(&fetchme);
			par->outpipe->Insert(&fetchme);
		}
	}

	par->outpipe->ShutDown();

}



// **************** Sum **************** //

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { 

	Param *par = new Param();

	par->inpipe = &inPipe;
	par->outpipe = &outPipe;
	par->computeme = computeMe;
	
	pthread_create(&rothread,NULL,SumRun,(void *)par);
}
	
void Sum::WaitUntilDone () { 
	pthread_join (rothread, NULL);
}
	
void Sum::Use_n_Pages (int n) { 

}

void * Sum::SumRun (void *arg) {

	Param *par = (Param *)arg;

	Record fetchme;

	int intres = 0;
	double dblres = 0;
	double sum = 0;
	Type t;

	while(par->inpipe->Remove(&fetchme)){
		t = par->computeme.Apply(fetchme,intres,dblres);
		sum += (intres + dblres);
	}

	Record res;
	
	Attribute att = {(char*)"sum", Double};
	Schema *sum_sch = new Schema("sum_sch", 1, &att);

	ostringstream ss;
	ss << sum;
	string str(ss.str());
	char dest[str.length()+1];
	sprintf(dest,"%s%s\0",str.c_str(),"|");
	
	res.SuckNextRecord(sum_sch,dest,str.length()+1);

	par->outpipe->Insert(&res);

	par->outpipe->ShutDown();
	
}



// **************** GroupBy **************** //

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { 
	Param *par = new Param();

	par->inpipe = &inPipe;
	par->outpipe = &outPipe;
	par->groupatts = groupAtts;
	par->computeme = computeMe;

	pthread_create(&rothread,NULL,GroupByRun,(void *)par);

}
	
void GroupBy::WaitUntilDone () { 
	pthread_join (rothread, NULL);
}
	
void GroupBy::Use_n_Pages (int n) { 

}

void * GroupBy::GroupByRun (void *arg) {

	Param *par = (Param *)arg;

	Pipe *tempoutpipe = new Pipe(100);

	BigQ bq1(*par->inpipe,*tempoutpipe,par->groupatts,1);

	Record fetchme , prev;

	int empty = tempoutpipe->Remove(&prev);

	ComparisonEngine ce;

	int intres = 0;
	double dblres = 0;
	double sum = 0;
	Type t;

	t = par->computeme.Apply(prev,intres,dblres);
	sum += (intres + dblres);

	empty = tempoutpipe->Remove(&fetchme);
	//fetchme.Print(&join_sch);
	Record res , finalRec;
	Attribute *att;
	Schema *sum_sch;

	att = new Attribute{(char*)"sum", Double};
	sum_sch = new Schema("sum_sch", 1, att);

	int nlatts = 1;
	int nratts = par->groupatts.GetNumOfAtts();
	int tatts = nlatts + nratts;

	int * tkm;			

	while(empty != 0) {

		if(ce.Compare(&prev,&fetchme,&par->groupatts) != 0) { 
			
			ostringstream ss;
			ss << sum;
			string str(ss.str());
			
			char dest[str.length()+1];
			sprintf(dest,"%s%s\0",str.c_str(),"|");
	
			res.SuckNextRecord(sum_sch,dest,str.length()+1);

			tkm = new int[tatts];

			int *wa = new int[par->groupatts.GetNumOfAtts()];

			par->groupatts.GetAtts(wa);

			tkm[0] = 0;

			for(int i=1;i<tatts;i++)
				tkm[i] = wa[i-1];

			finalRec.MergeRecords(&res,&prev,nlatts,nratts,tkm,tatts,nlatts);				

			par->outpipe->Insert(&finalRec);

			sum = 0;
			prev.Copy(&fetchme);			
			t = par->computeme.Apply(prev,intres,dblres);
			sum += (intres + dblres);
		}
		else{
			t = par->computeme.Apply(fetchme,intres,dblres);
			sum += (intres + dblres);
		}

		empty = tempoutpipe->Remove(&fetchme);
		//fetchme.Print(&join_sch);
	}

	ostringstream ss;
	ss << sum;
	string str(ss.str());
	
	char dest[str.length()+1];
	sprintf(dest,"%s%s\0",str.c_str(),"|");
	
	res.SuckNextRecord(sum_sch,dest,str.length()+1);

	tkm = new int[tatts];

	int *wa = new int[par->groupatts.GetNumOfAtts()];

	par->groupatts.GetAtts(wa);

	tkm[0] = 0;

	for(int i=1;i<tatts;i++)
		tkm[i] = wa[i-1];

	finalRec.MergeRecords(&res,&prev,nlatts,nratts,tkm,tatts,nlatts);				

	par->outpipe->Insert(&finalRec);

	par->outpipe->ShutDown();

}



// **************** WriteOut **************** //

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { 

	Param * par = new Param();

	par->inpipe = &inPipe;
	par->outfile = outFile;
	par->myschema = &mySchema;

	int n = par->myschema->GetNumAtts();
	Attribute *atts = new Attribute[n];
	atts = par->myschema->GetAtts();

	Record fetchme;

	while(par->inpipe->Remove(&fetchme)) {

	for (int i = 0; i < n; i++) {

		fputs(atts[i].name,par->outfile);
		fputs(": ", par->outfile);

		int pointer = ((int *) fetchme.bits)[i + 1];

		fputs("[",par->outfile);

		if (atts[i].myType == Int) {
			int *myInt = (int *) &(fetchme.bits[pointer]);
			fprintf(par->outfile,"%d",*myInt);	

		} else if (atts[i].myType == Double) {
			double *myDouble = (double *) &(fetchme.bits[pointer]);
			fprintf(par->outfile,"%lf",*myDouble);	

		} else if (atts[i].myType == String) {
			char *myString = (char *) &(fetchme.bits[pointer]);
			fputs(myString,par->outfile);	
		} 

		fputs("]",par->outfile);

		if (i != n - 1) {
			fputs(", ",par->outfile);
		}
	}

	fputs("\n",par->outfile);

	}

	//pthread_create(&rothread,NULL,WriteOutRun,(void *)par);
}
	
void WriteOut::WaitUntilDone () { 
	pthread_join (rothread, NULL);
}
	
void WriteOut::Use_n_Pages (int n) { 

}

void * WriteOut::WriteOutRun (void *arg) {

	Param *par = (Param *)arg;

	int n = par->myschema->GetNumAtts();
	Attribute *atts = new Attribute[n];
	atts = par->myschema->GetAtts();

	Record fetchme;

	while(par->inpipe->Remove(&fetchme)) {

	for (int i = 0; i < n; i++) {

		fputs(atts[i].name,par->outfile);
		fputs(": ", par->outfile);

		int pointer = ((int *) fetchme.bits)[i + 1];

		fputs("[",par->outfile);

		if (atts[i].myType == Int) {
			int *myInt = (int *) &(fetchme.bits[pointer]);
			fprintf(par->outfile,"%d",*myInt);	

		} else if (atts[i].myType == Double) {
			double *myDouble = (double *) &(fetchme.bits[pointer]);
			fprintf(par->outfile,"%lf",*myDouble);	

		} else if (atts[i].myType == String) {
			char *myString = (char *) &(fetchme.bits[pointer]);
			fputs(myString,par->outfile);	
		} 

		fputs("]",par->outfile);

		if (i != n - 1) {
			fputs(", ",par->outfile);
		}
	}

	fputs("\n",par->outfile);

	}

}