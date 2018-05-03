#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <string>
#include <map>
#include <vector>

using namespace std;

/*struct attInfo
{
	char * att;
	int numDistincts;
};

struct cmp_str
{
  bool operator()(char const *a, char const *b)
   {
      return strcmp(a, b) < 0;
   }
};*/


class Statistics
{

public:

	map <string,int> relmap;
	map <string,map<string,int> > attmap;
	map <string,string> attAliasMap;
	map <string,string> AndMap;
	multimap <string,string> OrMap;
	struct stringCMP {
    		stringCMP(string x) : x(x) {}
    		bool operator()(pair<string,string> y) { return x.compare(y.second)==0; }
    	private:
      		string x;
  	};

	int relindex;
	int applyCalled;

	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

	int processAndList(struct AndList *currAnd, char **relNames, int numToJoin, double& estimate, bool flag);
	int processOrList(struct OrList *currOr, char **relNames, int numToJoin, double& AndFactor, bool singleRelOrList, bool duplicateAtt, bool diffAndAttVal);
	int processCompOp(struct ComparisonOp *currCompOp, char **relNames, int numToJoin, double& OrFactor,bool firstPredicate, bool duplicateAtt, bool diffAndAttVal, bool diffOrAttVal);

	bool checkParseTreeViolation(struct AndList *parseTree, char **relNames);
	bool checkRelNamesViolation(struct AndList *parseTree, char **relNames);
};

#endif
