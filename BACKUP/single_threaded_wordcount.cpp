#include "kenny_include.h"
#include <iostream>
#include <fstream>
#include <time.h>
#include <stdlib.h>
#include <string>
#include <sys/time.h>
#include <unistd.h>
#include <pthread.h>
#include <utility>
#include <algorithm>
#include <vector>
#include <unordered_map>
#define INFILE "WORDCOUNT_INPUT_FILE"
#define OUTFILE "S_WORDCOUNT_OUTPUT_FILE"

using namespace std;
 
int main(){
    long double start_time = get_time();

    ifstream in_file;
    in_file.open(INFILE);
    string cur_line;

    vector<string> input_strings;
    unordered_map<string, int> string_count_pairs;

    // READING INPUTS:
    if (in_file.is_open()){
        while (getline(in_file, cur_line)){
            char cstr_cur_line[cur_line.size() + 1];
            char* cstr_tok;

            //Copy the current line into a Cstring so that we can call strtok()
            strcpy(cstr_cur_line, &cur_line[0]);
            cstr_tok = strtok(cstr_cur_line, " \n");
            while (cstr_tok != NULL){
                string tok = cstr_tok;
                input_strings.push_back(tok);
                string_count_pairs[tok] = 0;
                cstr_tok = strtok(NULL, " \n");
            }
        }
    } else { 
        cout << "\nINPUT_READER: The file didn't open correctly oh god oh man" << endl;
    }
    // DONE READING INPUTS

    for (int i = 0; i < input_strings.size(); ++i){
        ++string_count_pairs[input_strings[i]];
    }


    ofstream outfile;
    outfile.open(OUTFILE); 
    for (unordered_map<string, int>::iterator i = string_count_pairs.begin(); i != string_count_pairs.end(); ++i){
        string word = i->first;
        int count = i->second;
        outfile << word << ": " << count;
    }
    long double finish_time = get_time();

    cout << finish_time - start_time << endl;
        
}


