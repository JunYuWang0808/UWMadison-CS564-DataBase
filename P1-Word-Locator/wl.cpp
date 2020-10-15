//
// File: wl.h, wl.cpp
//
//  Description:  This is the main function. Read and analyse the command here and pass
//  them into the data structure designed, where you can find in wl.h
//
//  Student Name: Junyu Wang
//  UW Campus ID: 9081092562
//  enamil: jwang2289@wisc.edu
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include "wl.h"

using namespace std;
/**
 * Trim the word
 *
 *  Remove the white space before and after the string
 *
 * @param str to be processed
 * @return the processed string without space
 */
string &Trim_string(string &str) {
    if (str.empty()) {
        return str;
    }

    str.erase(0, str.find_first_not_of(" "));
    str.erase(str.find_last_not_of(" ") + 1);
    return str;
}
/**
 * Legal check of word
 *
 * Check if the word to locate command is legal, which means only
 * the word only contains characters, numbers and /'
 *
 * @param string1 to be checked
 * @return true/false
 */
bool locate_word_legal(string string1) {
        for (int i = 0; i < string1.size(); i++) {
        if ((string1[i] >= 'a' && string1[i] <= 'z') || (string1[i] >= 'A' && string1[i] <= 'Z')
            || (string1[i] >= 'a' && string1[i] <= 'z') || (string1[i] >= '0' && string1[i] <= '9')
            || string1[i] == '\'') {
            continue;
        } else {
            // if the string contains illegal chars, then return false
            return 0;
        }
    }

    return 1;
}

/**
 * Legal check of word
 *
 * check if the position of the locate command is pure number
 *
 * @param string1 to be checked
 * @return true/false
 */
bool locate_pos_legal(string string1) {
    for (int i = 0; i < string1.size(); i++) {
        if (string1[i] >= '0' && string1[i] <= '9') {
            continue;
        } else {
            // if this is not a pure number, then return false
            return 0;
        }
    }
    return 1;
}

int main() {
    Data_Store *data_store = new Data_Store();
    string s;
    vector<string> commands; // build a vector to restore the commands
    while (1) {
        cout<<">";
        getline(cin, s);
        commands.clear();
        s = Trim_string(s); // trim the input string
        if (s.empty()) { // if this is empty, let the user input new command
            continue;
        }
        string buf; // restore the temporary string
        stringstream ss(s); // using the stringstream to parse the command
        while (ss >> buf) {
            for(int i = 0; i < buf.size(); i ++){
                if(buf[i] >= 'A' && buf[i] <= 'Z')
                    buf[i] += 32;
            }
            commands.push_back(buf); // store them in the vector
        }
        if (commands[0] == "end") {
            if (commands.size() > 1) {
                cout << "ERROR: Invalid command" << endl;
            } else {
                break;
            }

        } else if (commands[0] == "load") { // exetuate the "new" command before the "load"

            if (commands.size() == 1) { // 1. if there is not load filename
                cout << "ERROR: Invalid command" << endl;
            } else if (commands.size() > 2) {
                cout << "ERROR: Invalid command" << endl;
            } else {
              data_store->load(commands[1]);
              //  data_store->load(path);
            }
            // 2. if there is a reading error
        } else if (commands[0] == "locate") {
            int index;
           // cout << commands.size() << endl;
            if (commands.size() < 3) {
                cout << "ERROR: Invalid command" << endl;
            } else if (commands.size() > 3) {
                cout << "ERROR: Invalid command" << endl;
            } else {
                if (locate_word_legal(commands[1]) && locate_pos_legal(commands[2])) {
                    // if the command is lagal
                //    cout << "legal" << endl;
                    istringstream iss(commands[2]);
                    iss >> index;
                    data_store->locate(commands[1], index);
                } else {
                    cout << "ERROR: Invalid command" << endl;
                }
            }
            // if this is the "new command"
        } else if (commands[0] == "new") {
            if (commands.size() > 1) {
                cout << "ERROR: Invalid command" << endl;
            } else {
                data_store->New();
            }
        } else {
            cout << "ERROR: Invalid command" << endl;
        }
    }
    // release the memory
    delete (data_store);
    return 0;
}
