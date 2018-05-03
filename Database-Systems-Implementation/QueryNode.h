#include "ParseTree.h"
#include "Schema.h"
#include "Setup.h"
#include "RelOp.h"
//#include "Function.h"
#include <cstring>



class RelOpNode {

    public:
    Schema *sch;
    //Pipe *inPipeL;
    //Pipe *inPipeR;
    Pipe *outPipe;
    int pipeID;

    RelOpNode *left;
    RelOpNode *right;

    virtual void Print(){};
    virtual void Run(){};
    virtual void WaitUntilDone() {};

    RelOpNode(){
        outPipe = new Pipe(100);
        left = NULL;
        right = NULL;
    };

    ~RelOpNode(){};

}*QueryHead;

void PrintNameList(NameList *nl){
    while(nl != NULL){
        cout << "    " << nl->name << endl;
        nl = nl->next;
    }
}

void PrintOutputSchema(Schema* s){

    cout<<endl;
    cout << "-------- Output Schema -------" << endl;

    for(int i = 0;i<s->GetNumAtts();i++){
        Attribute *att = s->GetAtts();
        cout << "    " << att[i].name << ":     ";
    
        if (att[i].myType == 0)
            cout << "{Int}" << endl;
        else if(att[i].myType == 1)
            cout << "{Double}" << endl;
        else if(att[i].myType == 2)
            cout << "{String}" << endl;
        
    }

    cout << "----------------------------" << endl;
    cout<<endl;
}


class SelectFileOpNode : public RelOpNode {
    
    private:
    relation *rel;
    DBFile dbf;
    CNF selOp;
    Record literal;
    SelectFile sf;

    public:
    
    SelectFileOpNode(struct AndList *cal,string relName, int& pipeIDCount){

        RelOpNode();

        rel = new relation ((char*)relName.c_str(), new Schema (catalog_path, (char*)relName.c_str()), dbfile_dir);

        sch = rel->schema();

        pipeIDCount++;

        pipeID = pipeIDCount;

        selOp.GrowFromParseTree (cal, sch, literal);
        
    };

    void Run (){
        dbf.Open(rel->path());
        sf.Run(dbf,*outPipe,selOp,literal);
    };

    void WaitUntilDone(){
        sf.WaitUntilDone();
    }

    void Print(){

        cout << endl;
        cout << endl;
        cout << "===========================" << endl;
        cout << "        SELECT FILE         " << endl;
        cout << "===========================" << endl;

        cout << endl;
        cout << "Pipe ID of Output: " << pipeID << endl;

        PrintOutputSchema(sch);

        cout << "Select Condition: " << endl << "    ";

        selOp.Print();
        cout << endl;

        cout << "===========================" << endl;
        cout<<endl;
    };

    ~SelectFileOpNode(){
        dbf.Close();
    };

};


class SelectPipeOpNode : public RelOpNode {

    private:

    CNF selOp;
    Record literal;
    SelectPipe sp;
    
    public:

    SelectPipeOpNode(struct AndList *cal, RelOpNode *lrop, string relName, int& pipeIDCount){

        RelOpNode();

        left = lrop;
        
        sch = left->sch;

        pipeIDCount++;

        pipeID = pipeIDCount;

        selOp.GrowFromParseTree (cal, sch, literal);

    };

    void Run () {
        sp.Run(*(left->outPipe) , *outPipe , selOp , literal);
    };

    void WaitUntilDone(){
        sp.WaitUntilDone();
    }

    void Print(){

        cout << endl;
        cout << endl;
        cout << "===========================" << endl;
        cout << "        SELECT PIPE    " << endl;
        cout << "===========================" << endl;

        cout << endl;
        cout << "Pipe ID of Input : " << left->pipeID << endl;
        cout << "Pipe ID of Output: " << pipeID << endl;

        PrintOutputSchema(left->sch);

        cout << "Select Condition: " << endl << "    ";

        selOp.Print();
        cout << endl;

        cout << "===========================" << endl;
        cout<<endl;
    };

    ~SelectPipeOpNode();

};


class ProjectOpNode : public RelOpNode {

    private:
    
    int *keepMe;
    int numAttsInput;
    int numAttsOutput;  
    struct NameList *pnl; 
    Project p;
    
    public:

    ProjectOpNode(struct NameList *cas, RelOpNode *prev, int& pipeIDCount){

        RelOpNode();

        left = prev;        
        
        Schema *insch = left->sch;

        pipeIDCount++;

        pipeID = pipeIDCount;

        numAttsInput = insch->GetNumAtts();

        numAttsOutput = 0;

        char key[] = ".";

        int i = 0;

        pnl = cas;

        struct NameList *tempas = cas;

        while(tempas != NULL){

            tempas->name = strpbrk (tempas->name, key)+1;

            numAttsOutput++;

            i++;

            tempas = tempas->next;
        }

        Attribute* tattarr = new Attribute[numAttsOutput]();

        keepMe = new int[numAttsOutput];

        tempas = cas;

        for(int j=0;j<numAttsOutput;j++) {
            keepMe[j] = insch->Find(tempas->name);
            tattarr[j].name = tempas->name;
            tattarr[j].myType = insch->FindType(tempas->name);
            tempas = tempas->next;
        }

        sch = new Schema("projectoutsch",numAttsOutput,tattarr);

    };

    void Run() {
        p.Run(*(left->outPipe),*outPipe,keepMe,numAttsInput,numAttsOutput);
    };

    void WaitUntilDone(){
        p.WaitUntilDone();
    }

    void Print(){

        cout << endl;
        cout << endl;
        cout << "===========================" << endl;
        cout << "        PROJECTION         " << endl;
        cout << "===========================" << endl;

        cout << endl;
        cout << "Pipe ID of Input : " << left->pipeID << endl;
        cout << "Pipe ID of Output: " << pipeID << endl;

        PrintOutputSchema(sch);

        cout << "Project Attributes:" << endl;

        PrintNameList(pnl);
        cout << endl;

        cout << "===========================" << endl;
        cout<<endl;
    };

    ~ProjectOpNode();

};


class SumOpNode : public RelOpNode {
    
    public:
    struct FuncOperator *sfo;
    Function computeMe;
    Sum s;

    SumOpNode(struct FuncOperator *cfo, RelOpNode *prev, int& pipeIDCount){

        RelOpNode();

        left = prev;
        
        sfo = cfo; 

        computeMe.GrowFromParseTree(cfo,*(left->sch));

        Attribute DA = {"Sum", Double};

        sch = new Schema("sumoutsch",1,&DA);

        pipeIDCount++;

        pipeID = pipeIDCount;

    };

    void Run(){
        s.Run(*(left->outPipe),*outPipe,computeMe);
    };

    void WaitUntilDone(){
        s.WaitUntilDone();
    }

    void Print(){

        cout << endl;
        cout << endl;
        cout << "===========================" << endl;
        cout << "            SUM            " << endl;
        cout << "===========================" << endl;

        cout << endl;
        cout << "Pipe ID of Input : " << left->pipeID << endl;
        cout << "Pipe ID of Output: " << pipeID << endl;

        PrintOutputSchema(sch);

        cout << "Aggregate Function: " << endl;

        this->computeMe.Print(sfo);
        cout << endl;

        cout << "===========================" << endl;
        cout<<endl;
    };

    ~SumOpNode();


};


class JoinOpNode : public RelOpNode {

    private:
    
    CNF selOp;
    Record literal; 
    Join j;
    
    public:
    
    JoinOpNode(struct AndList *cal, RelOpNode *lrop, RelOpNode *rrop, int& pipeIDCount){

        RelOpNode();

        left = lrop;
        right = rrop;

        sch = left->sch;
        sch = sch->mergeSchema(right->sch);
        
        pipeIDCount++;

        pipeID=pipeIDCount;        

        selOp.GrowFromParseTree(cal,left->sch,right->sch,literal);
    };

    void Run() {
        j.Run(*(left->outPipe) , *(right->outPipe) , *outPipe , selOp , literal);
    }

    void WaitUntilDone(){
        j.WaitUntilDone();
    }

    void Print(){

        cout << endl;
        cout << endl;
        cout << "===========================" << endl;
        cout << "            JOIN           " << endl;
        cout << "===========================" << endl;

        cout << endl;
        cout << "Pipe ID of Input 1: " << left->pipeID << endl;
        cout << "Pipe ID of Input 2: " << right->pipeID << endl;
        cout << "Pipe ID of Output: " << pipeID << endl;

        PrintOutputSchema(sch);

        cout << "Join Condition: " << endl << "    ";

        selOp.Print();
        cout << endl;

        cout << "===========================" << endl;
        cout<<endl;
    };

    ~JoinOpNode();

};


class GroupByOpNode : public RelOpNode {
    
    private:

    OrderMaker groupAtts;
    Function computeMe;
    struct NameList *grpnl;
    struct FuncOperator *grpfop;
    GroupBy gb;

    public:

    GroupByOpNode(struct NameList *cnl, struct NameList *selectnl, struct FuncOperator *cfo, RelOpNode *prev, int& pipeIDCount) {
        
        RelOpNode();

        left = prev;

        pipeIDCount++;

        pipeID = pipeIDCount;

        grpfop = cfo;

        computeMe.GrowFromParseTree(cfo,*(left->sch));

        grpnl = cnl;

        struct NameList *tempnl = cnl , *snl = selectnl;

        Schema *insch = left->sch;

        char key[] = ".";

        /* Handling select attributes in query other than aggregate function */

        int ngrpatts = 0;

        while(tempnl != NULL){
            ngrpatts++;
            tempnl = tempnl->next;
        }

        groupAtts.SetNumOfAtts(ngrpatts);

        Attribute* satts = new Attribute[ngrpatts+1];        

        satts[0] = {(char*)"Sum",Double};
		
        int wa[ngrpatts];
        Type wt[ngrpatts];

        int i=0; 

        while(cnl != NULL) {

            cnl->name = strpbrk (cnl->name, key)+1;

            wa[i] =  insch->Find(cnl->name);

            wt[i] = insch->FindType(cnl->name);

            satts[i+1] = {cnl->name,wt[i]};

            i++;

            cnl = cnl->next;
        }

        sch = new Schema("grpbysch",ngrpatts+1,satts);

        groupAtts.SetAtts(wa);
        groupAtts.SetTypes(wt);        
        
    };

    void Run(){
        gb.Run(*(left->outPipe),*outPipe,groupAtts,computeMe);
    };

    void WaitUntilDone(){
        gb.WaitUntilDone();
    }

    void Print(){

        cout << endl;
        cout << endl;
        cout << "===========================" << endl;
        cout << "          GROUP BY         " << endl;
        cout << "===========================" << endl;

        cout << endl;
        cout << "Pipe ID of Input : " << left->pipeID << endl;
        cout << "Pipe ID of Output: " << pipeID << endl;

        PrintOutputSchema(sch);

        cout << "Grouping Attributes:" << endl;

        PrintNameList(grpnl);
        cout << endl;

        cout << "Aggregate Function:" << endl;

        computeMe.Print(grpfop);
        cout << endl;

        cout << "===========================" << endl;
        cout << endl;
    };

    ~GroupByOpNode();

};


class DuplicateRemovalOpNode : public RelOpNode {
    
    public:
    
    DuplicateRemoval d;

    DuplicateRemovalOpNode(int distinctAtts, int distinctFunc, RelOpNode *prev, int& pipeIDCount){
        
        if(!(distinctAtts || distinctFunc)) {
            return;
        }

        RelOpNode();

        left = prev;

        pipeIDCount++;

        pipeID = pipeIDCount;

        sch = left->sch;
    };

    void Run(){
        d.Run(*(left->outPipe),*outPipe,*sch);
    };

    void WaitUntilDone(){
        d.WaitUntilDone();
    }

    void Print(){

        cout << endl;
        cout << endl;
        cout << "===========================" << endl;
        cout << "      DUPLICATE REMOVAL     " << endl;
        cout << "===========================" << endl;

        cout << endl;
        cout << "Pipe ID of Input : " << left->pipeID << endl;
        cout << "Pipe ID of Output: " << pipeID << endl;

        PrintOutputSchema(sch);

        cout << "===========================" << endl;

        cout<<endl;
    };

    ~DuplicateRemovalOpNode();

};


class WriteOutOpNode : public RelOpNode {

    public:

    FILE *outFile;
    WriteOut wo;

    WriteOutOpNode (RelOpNode *crn, char *& ofname) {
        //inPipeL = &iPipe;
        left = crn;
        outFile = fopen(ofname,"w");
        sch = left->sch;
    };

    void Run() {
        wo.Run(*(left->outPipe) , outFile , *sch);
    };

    void WaitUntilDone(){
        wo.WaitUntilDone();
    }

    ~WriteOutOpNode();

};