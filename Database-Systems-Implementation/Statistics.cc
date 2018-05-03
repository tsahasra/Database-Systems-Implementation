#include "Statistics.h"
#include <fstream>
#include <iostream>
#include <cstring>
#include <sstream>
#include <cmath>
#include <algorithm>
#include <set>

using namespace std;

Statistics::Statistics()
{
    relindex = 0;
}

Statistics::Statistics(Statistics &copyMe)
{
    this->relmap = copyMe.relmap;
    this->attmap = copyMe.attmap;
    this->attAliasMap = copyMe.attAliasMap;
    this->AndMap = copyMe.AndMap;
    this->OrMap = copyMe.OrMap;
    this->relindex = copyMe.relindex;
}

Statistics::~Statistics()
{
    applyCalled = 0;
}

void Statistics::AddRel(char *relName, int numTuples)
{
    relmap[relName] = numTuples;
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    
    (attmap[relName])[attName] = numDistincts;
    
}

void Statistics::CopyRel(char *oldName, char *newName)
{
    int temp = relmap[oldName];

    relmap[newName] = temp;

    map<string,int> tempmap = attmap[oldName];

    attmap[newName] = tempmap;
    
    //map<string,int> :: iterator it;

    /*for(it = attmap[oldName].begin(); it != attmap[oldName].end();++it){
        string temp = newName;
        temp += "." + it->first;
        attmap[newName][temp] = it->second;
    }*/
    
}
	
void Statistics::Read(char *fromWhere)
{
    ifstream ifile (fromWhere);

    bool fexists = (bool)ifile;

    char * token;

    if(fexists) {
        int numRel = 0;

        string line;
        getline(ifile,line);
        stringstream s1(line);
        s1 >> numRel;

        char * relstr;
        char * attstr;
        int nt = 0 , nd = 0;

        for(int i=0;i<numRel;i++) {
            getline(ifile,line);
            int n = line.length();
            char ca[n+1] ;
            strcpy(ca,line.c_str());
            relstr = strtok(ca," ");
            stringstream ss1(strtok(NULL," "));
            ss1 >> nt;
            AddRel(relstr,nt);
        }

        while(getline(ifile,line)) {
            int n = line.length();
            char ca[n+1] ;
            strcpy(ca,line.c_str());
            relstr = strtok(ca," ");
            attstr = strtok(NULL," ");
            stringstream ss2(strtok(NULL," "));
            ss2 >> nd;
            AddAtt(relstr,attstr,nd);
        }
    }
    
    ifile.close();
}

void Statistics::Write(char *fromWhere)
{
    ofstream ofile (fromWhere);

    int numRel = relmap.size();

    map<string,int> :: iterator itr;
    map<string,map<string,int> > :: iterator ita;
    
    ofile << numRel << endl;

    for(itr = relmap.begin() ; itr != relmap.end() ; ++itr) {
        ofile << itr->first << " " << itr->second << endl;
    }

    //ofile << numAttTot << endl;

    for(ita = attmap.begin() ; ita != attmap.end() ; ++ita) {
        map <string,int> m = ita->second;

        for(itr =m.begin() ; itr != m.end() ; ++itr) {
            ofile << ita->first << " " << itr->first << " " << itr->second << endl;
        }

    }

    ofile.close();
}

/*void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    applyCalled = 1;
    Estimate(parseTree,relNames,numToJoin);
    applyCalled = 0;
}*/

/*double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{    
    double result = 0.0;
    
    struct AndList *currAnd;
    struct OrList *currOr;

    currAnd = parseTree;

    string lrel;
    string rrel;

    string latt;
    string ratt;

    string jlrel, jrrel;

    bool joinFlag = false;
    bool joinPerformedFlag = false;

    bool dependsFlag = false;
    bool done = false;
    string prev;

    double andFactor = 1.0;
    double orFactor = 1.0;

    map<string, int> cnfmap;


    while (currAnd != NULL) {

            currOr = currAnd->left;
            orFactor = 1.0;

           while (currOr != NULL) {

                joinFlag = false;
                ComparisonOp *currentCompOp = currOr->left;

                if (currentCompOp->left->code != NAME) {
                    return 0;
                } else {

		        latt = currentCompOp->left->value;

			if(strcmp(latt.c_str(),prev.c_str())==0)
			{                   
				dependsFlag=true;
			}

			prev= latt;
				

                        for (map<string, map<string, int> >::iterator mapita = attmap.begin(); mapita != attmap.end(); mapita++) {
                                if (attmap[mapita->first].count(latt) > 0) {
                                        lrel = mapita->first;
                                        break;
                                }

                            }

                    }

                    if (currentCompOp->right->code == NAME) {
                            joinFlag = true;
                            joinPerformedFlag = true;
                            ratt = currentCompOp->right->value;

			
                            for (map<string, map<string, int> >::iterator mapita = attmap.begin(); mapita != attmap.end(); ++mapita) {
                                if (attmap[mapita->first].count(ratt) > 0) {
                                    rrel = mapita->first;
                                    break;
                                }
                            }
                    }

                    if (joinFlag == true) {

			
                            double leftDistinctCount = attmap[lrel][currentCompOp->left->value];
                            double rightDistinctCount = attmap[rrel][currentCompOp->right->value];

                            if (currentCompOp->code == EQUALS) {
                                orFactor *=(1.0 - (1.0 / max(leftDistinctCount, rightDistinctCount)));
                            }

                            jlrel = lrel;
                            jrrel = rrel;

                    } else {
		
			if(dependsFlag){

			    if(!done){
			        orFactor = 1.0 - orFactor;
				done = true;
                            }

			    if (currentCompOp->code == GREATER_THAN || currentCompOp->code == LESS_THAN) {
                                orFactor += (1.0 / 3.0);
                                cnfmap[currentCompOp->left->value] = currentCompOp->code;
                            }

                            if (currentCompOp->code == EQUALS) {
                                orFactor +=(1.0 / (attmap[lrel][currentCompOp->left->value]));
                                cnfmap[currentCompOp->left->value] = currentCompOp->code;
                            }

			}
                else{

                            if (currentCompOp->code == GREATER_THAN || currentCompOp->code == LESS_THAN) {
                                orFactor *= (2.0 / 3.0);
                                cnfmap[currentCompOp->left->value] = currentCompOp->code;

                            }
                            if (currentCompOp->code == EQUALS) {
                                orFactor *=(1.0- (1.0 / attmap[lrel][currentCompOp->left->value]));
                                cnfmap[currentCompOp->left->value] = currentCompOp->code;
                            }

			}

                    }
                    currOr = currOr->rightOr;
            }
	
        if(!dependsFlag)
	   orFactor =1.0 -orFactor;	
		      dependsFlag=false;
	    done =false;

            andFactor *= orFactor;
            currAnd = currAnd->rightAnd;
    }


    double rightTupleCount = relmap[jrrel];

    if (joinPerformedFlag == true) {
            double leftTupleCount = relmap[jlrel];
            result = leftTupleCount * rightTupleCount * andFactor;
    } else {
            double leftTupleCount = relmap[lrel];
            result = leftTupleCount * andFactor;
    }

    if (applyCalled) {

            map<string, int>::iterator cnfitr, itr;
            set<string> joinAttrSet;
            if (joinPerformedFlag) {
                    for (cnfitr = cnfmap.begin(); cnfitr != cnfmap.end(); cnfitr++) {

                        for (int i = 0; i < relmap.size(); i++) {
                                if (relNames[i] == NULL)
                                        continue;
                                int cnt = (attmap[relNames[i]]).count(cnfitr->first);
                                if (cnt == 0)
                                        continue;
                                else if (cnt == 1) {

                                for (itr = attmap[relNames[i]].begin(); itr != attmap[relNames[i]].end(); itr++) {
                                     if ((cnfitr->second == LESS_THAN) || (cnfitr->second == GREATER_THAN)) {
                                        attmap[jlrel + "_" + jrrel][itr->first] = (int)round((double)(itr->second) / 3.0);
                                     } else if (cnfitr->second == EQUALS) {
                                          if (cnfitr->first == itr->first) { 
                                               attmap[jlrel + "_" + jrrel][itr->first] = 1;
                                          } else
                                               attmap[jlrel + "_" + jrrel][itr->first] = min((int)round(result), itr->second);
                                          }
                                     }
                                        break;
                                    } else if (cnt > 1) {
                                        for (itr = attmap[relNames[i]].begin(); itr != attmap[relNames[i]].end(); itr++) {
                                            if (cnfitr->second == EQUALS) {
                                                if (cnfitr->first == itr->first) {
                                                    attmap[jlrel + "_" + jrrel][itr->first] = cnt;
                                                } else
                                                    attmap[jlrel + "_" + jrrel][itr->first] = min((int) round(result), itr->second);
                                                }
                                            }
                                            break;
                                    }
                                    joinAttrSet.insert(relNames[i]);
                            }
                    }

                    if (joinAttrSet.count(jlrel) == 0) {
                        for (map<string, int>::iterator ita = attmap[jlrel].begin(); ita != attmap[jlrel].end(); ita++) {
                            attmap[jlrel + "_" + jrrel][ita->first] = ita->second;
                        }
                    }
                    if (joinAttrSet.count(jrrel) == 0) {
                        for (map<string, int>::iterator ita = attmap[jrrel].begin(); ita != attmap[jrrel].end(); ita++) {
                            attmap[jlrel + "_" + jrrel][ita->first] = ita->second;
                        }
                    }
                    
                    relmap[jlrel + "_" + jrrel] =round(result);
                    relmap.erase(jlrel);
                    relmap.erase(jrrel);

                    attmap.erase(jlrel);
                    attmap.erase(jrrel);

            }
    }
    return result;
}*/


void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    double estimate = 0.0;
    double flag = true;
    relindex = 0;
    processAndList(parseTree,relNames,numToJoin,estimate,flag);
    
}


double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{ 
    Statistics sc(*this);
    double estimate = 0.0;
    double flag = true;
    relindex = 0;
    sc.processAndList(parseTree,relNames,numToJoin,estimate,flag);
    return estimate;
}


int Statistics::processAndList(struct AndList *currAnd, char **relNames, int numToJoin, double& estimate, bool flag)
{

    if (currAnd != NULL) {

        struct OrList *pOr = currAnd->left;

        bool singleRelOrList=true; 
        bool duplicateAtt=false;
        bool diffAndAttVal=false;
        double AndFactor = 0.0;

        int ind = processOrList(pOr, relNames, numToJoin,AndFactor,singleRelOrList,duplicateAtt,diffAndAttVal);

        relmap[relNames[ind]] = (int) AndFactor;
        
        if(diffAndAttVal){ 
            estimate=0;
            return ind;
        }
        else {
            estimate = AndFactor;
        }

        if (currAnd->rightAnd) {
            ind = processAndList(currAnd->rightAnd, relNames, numToJoin,estimate,flag);
        }

        return ind;
    }
    else {
        return -1;
    }
    
}


int Statistics::processOrList(struct OrList *currOr, char **relNames, int numToJoin, double& AndFactor, bool singleRelOrList, bool duplicateAtt, bool diffAndAttVal)
{
	if (currOr != NULL) {
        struct ComparisonOp *currCompOp = currOr->left;
        double OrFactor = 0.0;

        int *temp = new int[numToJoin];

        for(int i = 0;i<numToJoin;i++)
            temp[i] = relmap[relNames[i]];

        bool diffOrAttVal = false;
    
        int rin = processCompOp(currCompOp, relNames, numToJoin, OrFactor,(currOr->rightOr==NULL&&singleRelOrList),duplicateAtt,diffAndAttVal,diffOrAttVal);
    
        int den = temp[rin];

        delete []temp;
    
        double nom = OrFactor;

        if(diffOrAttVal && !duplicateAtt) {
            AndFactor = AndFactor + nom;  
        }
        else if(!diffOrAttVal && !duplicateAtt) {                             
            AndFactor = AndFactor + nom - AndFactor/den * nom;
        }

        if (currOr->rightOr) {

            singleRelOrList=false;

            bool duplicateAtt=false;

            rin = processOrList(currOr->rightOr, relNames, numToJoin,AndFactor,singleRelOrList,duplicateAtt,diffAndAttVal);

        }
        return rin;
    }
    else {
        return -1;
    }

}


int Statistics::processCompOp(struct ComparisonOp *currCompOp, char **relNames, int numToJoin, double& OrFactor , bool firstPredicate, bool DuplicateAtt, bool diffAndAttVal , bool diffOrAttVal)
{
    if(currCompOp != NULL) {

        if(currCompOp->left->code != NAME && currCompOp->right->code != NAME)
            return -1;

        if(currCompOp->left->code == NAME && currCompOp->right->code == NAME){

            int lin = 100 , rin = 100;
            bool lres = false , rres = false;

            string lop(currCompOp->left->value);
            int dotindex = (lop.find("."));
            char *lval = currCompOp->left->value+(dotindex+1);            
            string leftAttName(lval);

            string rop(currCompOp->right->value);
            dotindex = (rop.find("."));
            char *rval = currCompOp->right->value+(dotindex+1);
            string rightAttName(rval);

            if(attAliasMap.find(leftAttName)!=attAliasMap.end()){
                leftAttName = attAliasMap[leftAttName];
            }

            if(attAliasMap.find(rightAttName)!=attAliasMap.end()){
                rightAttName = attAliasMap[rightAttName];
            }

            for (int i = 0; i < numToJoin; i++) {
                
                if (attmap[string(relNames[i])].count(leftAttName)) {
                    lres = true;
                    lin = i;
                }

                if (attmap[string(relNames[i])].count(rightAttName)) {
                    rres = true;
                    rin = i;
                }

            }

            if (rightAttName.compare(leftAttName)==0) {
                int count=0;
                for (int i = 0; i < numToJoin; i++) {
                    if (attmap[string(relNames[i])].count(leftAttName)) {
                        if(count==0){
                            lres = true;
                            lin = i;
                            count=1;
                        }
                        else {
                            rres = true;
                            rin = i;
                            break;
                        }
                    }
                }
            }


            if (lres && rres && (lin != rin)) {

                int ltup = relmap[string(relNames[lin])];
                int rtup = relmap[string(relNames[rin])];

                int distlnum = attmap[string(relNames[lin])][leftAttName];
                int distrnum = attmap[string(relNames[rin])][rightAttName];
          
                OrFactor = (double) ltup * rtup / ((distlnum > distrnum) ? distlnum : distrnum);
          
                if(firstPredicate){
                    relmap[string(relNames[lin])] = (int) OrFactor;
                    
                    for (map<string, int>::iterator it = attmap[string(relNames[rin])].begin(); it != attmap[string(relNames[rin])].end(); ++it) {
              
                        if (strcmp(it->first.c_str(), rval) != 0) {
                            attmap[string(relNames[lin])][it->first] = it->second;
                        }
                    }

                }
                
                attAliasMap[rval] = lval;
          
                if(firstPredicate){
                    relmap.erase(string(relNames[rin]));
                    attmap.erase(string(relNames[rin]));
                }
          
                relindex = lin;
                
                return lin;
            }

            else{ 
                cerr << "ERROR: Join attributes not found in existing relations!" << endl;

                //cout<<"debug information: lIndex, rin,"<<lIndex<<" "<<rin<<lresult << rresult<<" num to Join "<<numToJoin<<leftValue<<endl;

                return -1;
            }

        }
        else {

            string attname = string(currCompOp->left->value);

            string attval = string(currCompOp->right->value);

            attname = attname.substr(attname.find(".")+1);
            
            if(attAliasMap.count(attname)!=0){
                attname = attAliasMap[attname];
            }

            if(currCompOp->code == EQUALS) {
                OrFactor = relmap[relNames[relindex]]/(double)attmap[relNames[relindex]][attname];

                relmap[relNames[relindex]] = (int)OrFactor;
                attmap[relNames[relindex]][attname] = 1;

                if(firstPredicate) {
                    
                    if(AndMap.count(attname)){
                        if(AndMap[attname] == attval) {
                            DuplicateAtt = true;
                        }else {
                            diffAndAttVal = true;
                            relmap.erase(string(relNames[relindex]));
                        }
                    }
                    else {
                        AndMap[attname] = attval;
                    }
                    
                } 

                else {
                    
                    if(OrMap.count(attname)) {
                        std::pair<std::map<string,string>::iterator,std::map<string,string>::iterator> range;
                        std::map<string,string>::iterator it;

                        range = OrMap.equal_range(attname);
                        it= find_if (range.first,range.second,stringCMP(attname));

                        if(it!=range.second){
                            DuplicateAtt = true;
                        }  
                        else {
                            diffOrAttVal = true;
                        } 
                    }
                    else {
                        OrMap.insert({attname,attval});
                    }

                }

                return relindex;

            }
            else if(currCompOp->code == LESS_THAN || currCompOp->code == GREATER_THAN) {
                
                OrFactor = relmap[relNames[relindex]]/3.0;

                if(firstPredicate) {
                    relmap[relNames[relindex]] = (int)OrFactor;
                    attmap[relNames[relindex]][attname] = (int)(attmap[relNames[relindex]][attname])/3.0;
                }

            }

            return relindex;
        }

    }
        return -1;
}


bool Statistics :: checkParseTreeViolation(struct AndList *parseTree, char **relNames) {
    
}


bool Statistics :: checkRelNamesViolation(struct AndList *parseTree, char **relNames) {

}