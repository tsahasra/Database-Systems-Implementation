
#include <iostream>
#include <climits>
#include <cstring>
#include "ParseTree.h"
#include "Statistics.h"
#include "QueryNode.h"
#include <ctime>

//#include "Setup.h"

using namespace std;



extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query
extern struct InsertOperation *insertOp; // insert operation parameters
extern struct CreateAttList *createAtts; // attributes for the new table being created
extern struct SortAttList *sortAtts; // attributes to be sorted on
extern char *ctablename; // create table parameter
extern char *dtablename; // drop table parameter
extern char *outfilename; // file to which query output will be written to
extern int cfiletype; // create file type
extern int outtype; // set output type

map<string,RelOpNode*> currNodeMap;
map<string,string> relPartMap;
map<string,string> tableAliasMap;
vector<RelOpNode *> QueryNodeList;

FILE tblcatalog;

int otype;
char *ofname;

time_t starttime;
time_t endtime;


void getRelNames(struct AndList *cal, char *relNames[], int& numToJoin,Statistics &stat) {

	struct OrList *tempOr = cal->left;

	if(tempOr != NULL){

		char lcarr[strlen(tempOr->left->left->value)+1];
		strcpy(lcarr,tempOr->left->left->value);

		const char *sop1 = (strtok(lcarr,"."));

		relNames[0] = new char[strlen(sop1)+1];		

		strcpy(relNames[0],sop1);

		if(stat.relmap.count(string(relNames[0])) == 0){

			char * temp = relNames[0];
			string tstr(temp);
			strcpy(relNames[0] ,relPartMap[tstr].c_str());
		}		

		numToJoin++;

		if(tempOr->left->right->code == NAME) {

			char rcarr[strlen(tempOr->left->right->value)+1];
			strcpy(rcarr,tempOr->left->right->value);			

			const char *sop2 = (strtok(rcarr,"."));

			relNames[1] = new char[strlen(sop2)+1];		

			strcpy(relNames[1],sop2);

			//cout<<relNames[1]<<endl;
			if(stat.relmap.count(string(relNames[1])) == 0){

				char * temp = relNames[1];
				string tstr(temp);
				strcpy(relNames[1] ,relPartMap[tstr].c_str());
			}		


			numToJoin++;
		}

		
	}

}


bool checkIfSingleRelation(struct AndList *cal,char **relNames, int numToJoin, Statistics &stat) {

	OrList *tor = cal->left;
	char key[] = {"."};
	string rel = "";

	while(tor != NULL){
		char *top = strpbrk (tor->left->left->value,key)+1;

		if(stat.attAliasMap.count((string)top))
			top = (char *)(stat.attAliasMap[top]).c_str();

		for(int i=0;i<numToJoin;i++){

			if(stat.attmap[string(relNames[i])].count(string(top)) > 0) {
				if(rel == "") {
					rel = (string) relNames[i];
					break;
				}else
					if((string) relNames[i] != rel)
						return false;
			}
			else {
				return false;
			}   
		}

		tor = tor->rightOr;

	}

	return true;
}

void processAndListRecursively(struct AndList *boolean, struct AndList& leastCostlyAl, Statistics& stat,double& leastEstimate){

	if(boolean == NULL)
		return;

	struct AndList cal;

	if(boolean != NULL){
		cal = *boolean;
		boolean = boolean->rightAnd;
		cal.rightAnd = NULL;		
	}
	else {
		return;
	}

	char *relNames[2];

	int numToJoin = 0; 

	getRelNames(&cal,relNames,numToJoin,stat);

	double currEstimate = __DBL_MAX__;
	
	if(checkIfSingleRelation(&cal,relNames,numToJoin,stat))
	 	currEstimate = stat.Estimate(&cal,relNames,numToJoin);

	if(currEstimate < leastEstimate) {
		leastCostlyAl  = cal;
		leastEstimate = currEstimate;
	}
	
	processAndListRecursively(boolean,leastCostlyAl,stat,leastEstimate);

}


void createQueryNode(struct AndList *cal, char **relNames,int numToJoin,int& pipeIDCounter) {

	RelOpNode *rop;

	if (numToJoin == 1){

		if(currNodeMap.count(string(relNames[0])) == 0){
			rop = new SelectFileOpNode(cal,tableAliasMap[string(relNames[0])],pipeIDCounter);			
		} else {
			rop = new SelectPipeOpNode(cal,(currNodeMap[string(relNames[0])]),tableAliasMap[string(relNames[0])],pipeIDCounter);
		}
	
		currNodeMap[string(relNames[0])] = rop;
		QueryNodeList.push_back(rop);
	}
	else {

		RelOpNode *lrop, *rrop;

		if(currNodeMap.count(string(relNames[0])) == 0) {
			AndList tempal1;
			tempal1.left = NULL;
			tempal1.rightAnd = NULL;
			lrop = new SelectFileOpNode(&tempal1,tableAliasMap[string(relNames[0])],pipeIDCounter);
			currNodeMap[string(relNames[0])] = lrop;
			QueryNodeList.push_back(lrop);
		}
		else{
			lrop = currNodeMap[string(relNames[0])];
		}

		if(currNodeMap.count(string(relNames[1])) == 0) {
			AndList tempal2;
			tempal2.left = NULL;
			tempal2.rightAnd = NULL;
			rrop = new SelectFileOpNode(&tempal2,tableAliasMap[string(relNames[1])],pipeIDCounter);
			QueryNodeList.push_back(rop);
			//currNodeMap[string(relNames[1])] = *rrop;
		}
		else{
			rrop = currNodeMap[string(relNames[1])];
		}

		rop = new JoinOpNode(cal,lrop,rrop,pipeIDCounter);
		QueryNodeList.push_back(rop);
		
		currNodeMap[string(relNames[0])] = rop;
		currNodeMap.erase(string(relNames[1]));

	}

}

void removeDotsFromAndList(struct AndList *tempal){

	char key[] = ".";

	while(tempal != NULL){

		struct OrList *tempor = tempal->left;

		while(tempor != NULL){			

			if(tempor->left->left->code == NAME)
				tempor->left->left->value = strpbrk (tempor->left->left->value, key)+1;

			if(tempor->left->right->code == NAME)
				tempor->left->right->value = strpbrk (tempor->left->right->value, key)+1;

			tempor = tempor->rightOr;
		}

		tempal = tempal->rightAnd;
	}
}

void removeDotsFromFuncOperator(struct FuncOperator *fop){
	if(fop == 0)
		return;

	removeDotsFromFuncOperator(fop->leftOperator);

	char key[] = ".";
	if(fop->leftOperand != 0 && fop->leftOperand->code == NAME)
		fop->leftOperand->value = strpbrk (fop->leftOperand->value, key)+1;

	removeDotsFromFuncOperator(fop->right);
}


void generateQueryPlan(struct AndList *boolean, Statistics& stat) {

	/*Isolate andlist from the original list */
	int pipeIDCounter = 0;

	RelOpNode *rop;

	if(boolean == NULL) {
		
		char *rNames[1] = {tables->aliasAs};

		int ntj = 1;

		createQueryNode(boolean,rNames,ntj,pipeIDCounter);
	}

	while(boolean != NULL) {

		struct AndList leastCostlyAl ;
		leastCostlyAl.left = NULL;
		leastCostlyAl.rightAnd = NULL;

		double leastEstimate = __DBL_MAX__;

		struct AndList * currAndptr = boolean;

		processAndListRecursively(currAndptr, leastCostlyAl, stat, leastEstimate);

		char *relNames[2];

		int numToJoin = 0; 

		getRelNames(&leastCostlyAl,relNames,numToJoin,stat);		
		
		stat.Apply(&leastCostlyAl,relNames,numToJoin);

		if(numToJoin == 2) 
			relPartMap[relNames[1]] = relNames[0];			

		removeDotsFromAndList(&leastCostlyAl);

		createQueryNode(&leastCostlyAl,relNames,numToJoin,pipeIDCounter);

		//createQueryNode(leastCostlyAl);

		struct AndList *calptr = boolean;

		if(calptr->left == leastCostlyAl.left){
			boolean = calptr->rightAnd;
		}
		else {
			while(calptr != NULL) {
				if(calptr->rightAnd->left == leastCostlyAl.left){
					calptr->rightAnd = calptr->rightAnd->rightAnd;
					break;
				}
					
				calptr = calptr->rightAnd;
					
			}
		}
		
	}


	/* Add Function and GroupBy clauses to query tree */

	RelOpNode *prevQueryHead = currNodeMap.begin()->second;

	if(groupingAtts != NULL){
		removeDotsFromFuncOperator(finalFunction);
		QueryHead = new GroupByOpNode(groupingAtts,attsToSelect,finalFunction,prevQueryHead,pipeIDCounter);	
	}
	else if(finalFunction != NULL){
		removeDotsFromFuncOperator(finalFunction);
		QueryHead = new SumOpNode(finalFunction,prevQueryHead,pipeIDCounter);
	}
	else {
		QueryHead = new ProjectOpNode(attsToSelect,prevQueryHead,pipeIDCounter);
	}

	RelOpNode *cqn = QueryHead;

	QueryNodeList.push_back(cqn);
	
	RelOpNode *tempNode = new DuplicateRemovalOpNode(distinctAtts,distinctFunc,QueryHead,pipeIDCounter);

	if(distinctAtts||distinctFunc) {
		QueryNodeList.push_back(tempNode);
		QueryHead = tempNode;
	}
	

	if(QueryHead == NULL) {
		QueryHead = prevQueryHead;
		QueryNodeList.push_back(QueryHead);
	}
	

}


void initializeQueryPlan(Statistics& s) {

	char *fromWhere = new char[15];
	
	fromWhere = "Statistics.txt";

	s.Read(fromWhere);

	struct TableList *temptl = tables;

	int partitionNo = 1;

	while(temptl != NULL){
		s.CopyRel(temptl->tableName,temptl->aliasAs);
		tableAliasMap[temptl->aliasAs] = temptl->tableName;
		temptl = temptl->next;
	}


}


void QueryTreeInorderPrint(RelOpNode *curr) {
	if(!curr)
		return;

	QueryTreeInorderPrint(curr->left);

	curr->Print();

	QueryTreeInorderPrint(curr->right);
}

void PrintQueryTree(RelOpNode *qhead) {

	cout<< " @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" << endl;
	cout<< "           OPTIMAL QUERY TREE		 "<<endl;
	cout<< " @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" << endl;
	cout<<endl;
	

	QueryTreeInorderPrint(qhead);	
}


void QueryTreePostOrderExecution(RelOpNode *curr) {
	if(curr == NULL)
		return;

	QueryTreePostOrderExecution(curr->left);

	QueryTreePostOrderExecution(curr->right);

	curr->Run();
}


void ExecuteQueryTree(RelOpNode *curr) {
	
	cout<<endl<<endl;
	cout<< " Executing Query ..." << endl;

	//starttime = time(NULL);

	QueryTreePostOrderExecution(curr);
}



void PrintQueryResult(RelOpNode *qhead) {

	Pipe *rpipe = qhead->outPipe;
	Schema *s = qhead->sch;
	Record rec;
	int rcount = 0;

	cout<<endl<<endl;
	cout<< " @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" << endl;
	cout<< "             QUERY RESULT		     "<<endl;
	cout<< " @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" << endl;
	cout<<endl;

	while(rpipe->Remove(&rec) != 0) {
		cout<<" ";
		rec.Print(s);
		rcount++;
	}

	endtime = time(NULL);

	cout<<endl;
	cout<<" The Query returned "<<rcount<<" record(s)"<<endl;
	cout<<endl;

	//cout<< " Execution Time: " << difftime(endtime,starttime) << " seconds" << endl << endl;
	//cout<< " End time : " << ctime(&endtime) << endl;
}

bool checkIfTablesExist(struct TableList *tables) {
	
	while(tables != NULL){
		if(!checkIfSchemaPresent(tables->tableName))
			return false;
		tables = tables->next;
	}

	return true;
}


void PrintOrExecuteQueryTree(Statistics &stat , int& otype , char *& ofname){

	if(!checkIfTablesExist(tables)) {
		cout<< endl;
		cout<<" One or more table(s) do not exist" << endl;
		cout << endl;
		return;
	}

	initializeQueryPlan(stat);

	generateQueryPlan(boolean,stat);

	if(otype == 3) {
		PrintQueryTree(QueryHead);	
	} 
	else
	{
		if(otype == 1) {
			ExecuteQueryTree(QueryHead);
			PrintQueryResult(QueryHead);
			QueryHead->WaitUntilDone();
		}
		else {
			RelOpNode *crn = new WriteOutOpNode(QueryHead,ofname);
			QueryHead = crn;
			ExecuteQueryTree(QueryHead);
			QueryHead->WaitUntilDone();
		}
	}
}


void createTable(char *ctname, struct CreateAttList *catts, struct SortAttList *satts, int ftype) {

	if(checkIfSchemaPresent(ctname)) {
		cout << endl;
		cout << " Table "<<ctname<<" already exists! ... " << endl;
		cout << endl;
		return;
	}

	addRelation(ctname,createAtts);

	DBFile dbfile;

	relation *relnew = new relation (ctname, new Schema (catalog_path, ctname), dbfile_dir);

	fType f;

	void * stup = NULL;

	switch(cfiletype){
		case 1: 
			f = heap;
			dbfile.Create (relnew->path(), f, NULL);
		break;

		case 2:
			f = sorted;
			
			struct SortAttList *tsatts = satts;

			int sortattsize = 0;

			while(tsatts != NULL){
				sortattsize++;
				tsatts = tsatts->next;
			}

			int *wa = new int[sortattsize];
			Type *wt = new Type[sortattsize];

			tsatts = satts;
			int i =0;

			while(tsatts != NULL){
				wa[i] = relnew->schema()->Find(tsatts->name);
				wt[i] = relnew->schema()->FindType(tsatts->name);
				tsatts = tsatts->next;
				i++;
			}

			struct SortInfo	{
				OrderMaker *som;
				int srunlength;
			}si;

			si.som = new OrderMaker();
			si.som->SetNumOfAtts(sortattsize);
			si.som->SetAtts(wa);
			si.som->SetTypes(wt);

			si.srunlength = 1;

			dbfile.Create (relnew->path(), f, (void *)&si);

		break;
	}

	
	dbfile.Close ();

	cout << endl;
	cout<< " Create table successful ... " << endl;
	cout<< endl;
}


void dropTable(char *dtname) {

	if(!checkIfSchemaPresent(dtname)) {
		cout << endl;
		cout << " Table "<<dtname << " does not exist ! ... " << endl;
		cout << endl;
		return;
	}

	relation *relexist = new relation (dtname, new Schema (catalog_path, dtname), dbfile_dir);

	char *dbfilepath = relexist->path();

	size_t sizeoffile = strlen(dbfilepath);
    sizeoffile += 5;
    char mf_path[sizeoffile];
    sprintf(mf_path,"%s.meta",dbfilepath);

	remove(mf_path);
	remove(dbfilepath);

	removeRelation(dtname);

	cout << endl;
	cout<< " Drop table successful ... " << endl;
	cout<< endl;

}


void insertRecordsIntoTable(struct InsertOperation *cio) {

	if(!checkIfSchemaPresent(cio->tname)) {
		cout << endl;
		cout << " Table " << cio->tname <<" does not exist ! ... " << endl;
		cout << endl;
		return;
	}
	
	DBFile dbfile;

	relation *rel = new relation (cio->tname, new Schema (catalog_path, cio->tname), dbfile_dir);

	dbfile.Open(rel->path());

	char *txt_path = new char[strlen(tpch_dir) + strlen(cio->fname) + 4];

	strcpy(txt_path,tpch_dir);
	strcat(txt_path,cio->fname);
	strcat(txt_path,".tbl");

	dbfile.Load(*(rel->schema()),txt_path);

	dbfile.Close();

}


void initializeOutputtype(int &otype , char *& ofname) {

	ifstream ifs;

	ifs.open("OpSettings");

	string line;

	getline(ifs,line);

	stringstream sstr(line);
 
    int x = 0;
    sstr >> x;

	
	switch(x){
		case 1:
			otype = 1;
			ofname = NULL;
		break;

		case 2:
			otype = 2;
			getline(ifs,line);
			ofname = new char[strlen(line.c_str())];
			strcpy(ofname,line.c_str());
		break;

		case 3:
			otype = 3;
			ofname = NULL;
		break;
			
	}

}


void setOutputType(int& otype, char *& ofname) {
	switch(outtype) {
		case 1:
		case 3:
			otype = outtype;
			ofname = NULL;
		break;

		case 2:
			otype = outtype;
			ofname = outfilename;
			break;
	}
}




void clearGlobals() {

	currNodeMap.clear();
	relPartMap.clear();
	tableAliasMap.clear();

}


void Shutdown(){

	//cleanup();

	ofstream ofs;

	ofs.open("OpSettings");

	ofs << otype << endl;

	if(otype == 2)
		ofs << ofname << endl;

	ofs.close();

	cout<<endl;
	cout << " State saved .. System shutdown successful" << endl;
	cout << endl;

	exit(0);
}


int main () {

	setup();

	initializeOutputtype (otype,ofname);

	//cout << ofname << endl;

	while(1) {

		Statistics s;				

		int tindx = 0;

		while (tindx < 1 || tindx > 6) {
			cout << " Select Database Operation: \n";
			cout << " \t 1. Create table \n";
			cout << " \t 2. Drop table \n";
			cout << " \t 3. Insert records into table \n";
			cout << " \t 4. Set query output type \n";
			cout << " \t 5. Execute SQL query \n ";
			cout << " \t 6. Shutdown database \n"; 
			cin >> tindx;
		}

		if(tindx != 6)
			yyparse();

		switch(tindx) {
			case 1:
				createTable(ctablename,createAtts,sortAtts,cfiletype);
			break;

			case 2:
				dropTable(dtablename);
			break;

			case 3:
				insertRecordsIntoTable(insertOp);
			break;

			case 4:
				setOutputType(otype,ofname);
			break;

			case 5:
				PrintOrExecuteQueryTree(s,otype,ofname);
			break;

			case 6:
				Shutdown();
			break;
		}

		clearGlobals();
	} 

	//cout << "done" << endl;
}




