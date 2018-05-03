
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <math.h>
#include <fstream>
#include <sstream>

//#include "Function.h"
//#include "Pipe.h"
#include "DBFile.h"
//#include "Record.h"

using namespace std;

// test settings file should have the 
// catalog_path, dbfile_dir and tpch_dir information in separate lines
const char *settings = "test.cat";

// donot change this information here
char *catalog_path, *dbfile_dir, *tpch_dir = NULL;

class relation {

private:
	char *rname;
	char *prefix;
	char rpath[100]; 
	Schema *rschema;
public:
	relation (char *_name, Schema *_schema, char *_prefix) :
		rname (_name), rschema (_schema), prefix (_prefix) {
		sprintf (rpath, "%s%s.bin", prefix, rname);
	}
	char* name () { return rname; }
	char* path () { return rpath; }
	Schema* schema () { return rschema;}
	void info () {
		cout << " relation info\n";
		cout << "\t name: " << name () << endl;
		cout << "\t path: " << path () << endl;
	}

};

relation *rel;

char *supplier = "supplier"; 
char *partsupp = "partsupp"; 
char *part = "part"; 
char *nation = "nation"; 
char *customer = "customer"; 
char *orders = "orders"; 
char *region = "region"; 
char *lineitem = "lineitem"; 

//relation *s, *p, *ps, *n, *li, *r, *o, *c;

void setup () {
	FILE *fp = fopen (settings, "r");
	if (fp) {
		char *mem = (char *) malloc (80 * 3);
		catalog_path = &mem[0];
		dbfile_dir = &mem[80];
		tpch_dir = &mem[160];
		char line[80];
		fgets (line, 80, fp);
		sscanf (line, "%s\n", catalog_path);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", dbfile_dir);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", tpch_dir);
		fclose (fp);
		if (! (catalog_path && dbfile_dir && tpch_dir)) {
			cerr << " Test settings file 'test.cat' not in correct format.\n";
			free (mem);
			exit (1);
		}
	}
	else {
		cerr << " Test settings files 'test.cat' missing \n";
		exit (1);
	}

	/*ifstream ifs;

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
			ofname = (char *)line.c_str();
		break;

		case 3:
			otype = 3;
			ofname = NULL;
		break;
			
	}*/
	

	/*cout << " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
	cout << " catalog location: \t" << catalog_path << endl;
	cout << " tpch files dir: \t" << tpch_dir << endl;
	cout << " heap files dir: \t" << dbfile_dir << endl;
	cout << " \n\n";

	s = new relation (supplier, new Schema (catalog_path, supplier), dbfile_dir);
	p = new relation (part, new Schema (catalog_path, part), dbfile_dir);
	ps = new relation (partsupp, new Schema (catalog_path, partsupp), dbfile_dir);
	n = new relation (nation, new Schema (catalog_path, nation), dbfile_dir);
	li = new relation (lineitem, new Schema (catalog_path, lineitem), dbfile_dir);
	r = new relation (region, new Schema (catalog_path, region), dbfile_dir);
	o = new relation (orders, new Schema (catalog_path, orders), dbfile_dir);
	c = new relation (customer, new Schema (catalog_path, customer), dbfile_dir);*/
}



bool checkIfSchemaPresent(char *relName) {
	FILE *foo = fopen (catalog_path, "r");
	
	// this is enough space to hold any tokens
	char space[200];

	fscanf (foo, "%s", space);
	int totscans = 1;

	// see if the file starts with the correct keyword
	if (strcmp (space, "BEGIN")) {
		cout << "Unfortunately, this does not seem to be a schema file.\n";
		exit (1);
	}	
		
	while (1) {

		// check to see if this is the one we want
		fscanf (foo, "%s", space);
		totscans++;
		if (strcmp (space, relName)) {

			// it is not, so suck up everything to past the BEGIN
			while (1) {

				// suck up another token
				if (fscanf (foo, "%s", space) == EOF) {
					fclose(foo);
					return false;
				}

				totscans++;
				if (!strcmp (space, "BEGIN")) {
					break;
				}
			}

		// otherwise, got the correct file!!
		} else {
			fclose(foo);
			return true;
		}
	}
	
}


void removeRelation(char * relName) {

	if(!checkIfSchemaPresent(relName)) {
		cerr << "Could not find the schema for the specified relation.\n";
		exit (1);
	}

	char *tcat_path = new char[strlen(catalog_path)+5];

	strcpy(tcat_path,catalog_path);
	strcat(tcat_path,".tmp");

	string line , templine;

	ifstream ifs;

	ifs.open(catalog_path);

	ofstream ofs;

	ofs.open(tcat_path);

	while(getline(ifs,line)){
		if(line == "BEGIN"){
			if(getline(ifs,templine)){
				if (templine == string(relName)) {
					while(getline(ifs,line)) {
						if(line == "END") {
							break;
						}else {
							continue;
						}
					}
				} else {
					ofs << line << endl;
					ofs << templine << endl;
				}
			}
			else {
				cerr<<"error... catalog file corrupt!"<<endl;
				exit(1);
			}
			
		}
		else {
			ofs << line << endl;
		}
	}

	ifs.close();
	ofs.close();

	remove(catalog_path);
	rename(tcat_path,catalog_path);
}


void addRelation(char *relName , struct CreateAttList *catts){

	ofstream ofs;

	ofs.open(catalog_path,std::ios::app);

	ofs << endl;
	ofs << "BEGIN" << endl;
	ofs << relName << endl;
	ofs << relName << ".tbl" << endl;

	while(catts != NULL) {
		ofs << catts->name << " " ;
		
		switch(catts->code){
			case 1:
				ofs << "Double" << endl;
			break;
			
			case 2:
				ofs << "Int" << endl;
			break;

			case 4:
				ofs << "String" << endl;
			break;
		}

		catts = catts->next;

	}

	ofs << "END" << endl;

	ofs.close();

}


void cleanup () {
	//delete s, p, ps, n, li, r, o, c

	free (catalog_path);
	free (dbfile_dir);
	free (tpch_dir);
}


