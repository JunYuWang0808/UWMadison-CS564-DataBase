//
// File: wl.h, wl.cpp
// 
//  Description: Design the data sttucture. Set several operations here.
// Deal with "new", "load", "locate" and ""
//  Student Name: Add stuff here .. 
//  UW Campus ID: Add stuff here .. 
//  enamil: Add stuff here ..
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <map>
#include <vector>
#include <sstream>

using namespace std;


class Data_Store{
public:
    Data_Store(){
        count = 1; // index begins from 1
    }
    void New(){
        mymap.clear();
        count = 1;
        //cout<<mymap.size()<<endl;
        return;
    }
    /**
     * Load file
     *
     * Read the file and get the stream of it. Then process it line by line
     * @param filename the path of the file
     */
    void load(string filename){

        count = 1; // use this to store the position of the word
        ifstream in(filename);
        if(!in){
            // read error
            cout<< "ERROR: Invalid command"<<endl;
            return;
        }
        New(); // "new", only if you load a new file successfully
        string temp; // to store temporary strings
        for(string string1; getline(in, string1);){
            string1 = remove_punc(string1); // remove the punctuations except ' \' '
            stringstream ss(string1);
            while(ss >> temp){
                store(temp);
                count ++;
            }
        }
        // cout<<"done"<<mymap.size()<<endl;
        map<string, vector<int> >::iterator it;
    }
    /**
     * Locate the position of the word
     *
     * Given the word and index, you can find the position vector of
     * a certain word, then get the value in the index of it
     *
     * @param word to be located
     * @param index the index of its occurence
     */
    void locate(string word, int index){
        map<string, vector<int> >::iterator it;
        it = mymap.find(word);
        if(it == mymap.end()){
            cout<<"No matching entry"<<endl;
        } else{
            if(index > it->second.size()){
                cout<<"No matching entry"<<endl;
            } else{
                cout<<it->second[index-1]<<endl;
            }
        }
        return;
    }

private:

    /**  Remove the punctuation
     *
     * Given the word, use a for loop to replace the useless character with space
     * in order to seperate the words.
     *
     * @param words, the string to be precessed
     * @return the word without punctuation
     */
    string remove_punc(string words){
        // for loop every char in the string
        for(int i = 0; i < words.size(); i ++){
            // determine if the current char is needed to be removed
            if((words[i] >= 'a' && words[i] <= 'z') || (words[i] >= 'A' && words[i] <= 'Z')
                ||(words[i] >= '0' && words[i] <= '9')|| (words[i] == '\'') || (words[i]==' '))
            {
                continue;
            }
            else
                words.replace(i, 1, " ");// replace this character with white space
        }
        return words;
    }
    /**
     * Store the data in the structure
     *
     * Here you put in a word you want to store, you will record the word as a
     * key and its position as the value in a vector
     *
     * @param word to be stored
     */
    void store(string word){
        for(int i = 0; i < word.size(); i ++){
            if(word[i] >= 'A' && word[i] <= 'Z')
                word[i] += 32;
        }// make the word be Case-Insensitive
        map<string, vector<int> >::iterator it;
        it = mymap.find(word); // find if the word has been put
        if (it == mymap.end()){
        //    cout<<word<<" put "<<endl;
            // this is the first time it is put
            vector<int> tempvector;
            tempvector.push_back(count);
            mymap.insert(pair<string, vector<int> >(word, tempvector));
        }
        else{

            // directly put the current word in it
          //  cout<<word<<" +1 "<<endl;
            it->second.push_back(count);
        }
    }

    int count; // record the index of the word
    map<string, vector<int> > mymap; // this is the data structure we use here

};