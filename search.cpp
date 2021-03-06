
// ------------------------------- Includes ----------------------------------

#include <cstdio>
#include <iostream>
#include <stdlib.h>
#include <cstring>
#include <dirent.h>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"

using namespace std;

// -------------------------------- Constants --------------------------------

/**
 * VALID_ARG_NUM 2
 * @brief  A const value representing function success
 */
#define VALID_ARG_NUM 2

/**
 * MULTI_THREADS_LEVEL 10
 * @brief  A const value representing function success
 */
#define MULTI_THREADS_LEVEL 10

/**
 * FUNC_SUCCESS 0
 * @brief  A const value representing function success
 */
#define FUNC_SUCCESS 0

string thread_init_fail = "Usage: <substring to search> "
        "<folders, separated by space>";



// ----------------------------- Global variables ---------------------------


IN_ITEMS_VEC mapInput;

OUT_ITEMS_VEC search_output_vector;

bool frameworkDeleteResources = true;



// ----------------------------CLASSES DEFINITIONS ---------------------------


class FileName : public k1Base, public k2Base, public k3Base
{
public:
    /**
     * A default FileName constructor.
     */
    FileName(){};

    /**
     * 
     * @param file_name 
     * @return 
     */
    FileName(string file_name)  {_fileName = file_name;};

    /**
     * A FileName destructor.
     */
    ~FileName(){};

    string get_file_name() const { return _fileName;};

    bool operator<(const k1Base &other) const;

    bool operator<(const k2Base &other) const;

    bool operator<(const k3Base &other) const;

private:
    string _fileName;
};


class WordToSearch: public v1Base, public v2Base, public v3Base
{
public:
    /**
     * A default FileName constructor.
     */
    WordToSearch(){};

    WordToSearch(string  word_search)  {_word_to_search = word_search;};

    /**
     * A FileName destructor.
     */
    ~WordToSearch(){};

    string get_word() const { return _word_to_search;};

private:
    string  _word_to_search;

};


class MapReduceSearch: public MapReduceBase
{
public:
    void Map(const k1Base *const key, const v1Base *const val) const;
    void Reduce(const k2Base *const key, const V2_VEC &vals) const;
};

// ------------------- class functions implementation-------------------------

/**
 * The iperator < implementation for the k1Base
 * @param other
 * @return
 */
bool FileName::operator<(const k1Base &other) const
{
    const FileName& other_file = (const FileName&)(other);
    int res = strcmp(this->get_file_name().c_str(), other_file.get_file_name()
            .c_str());
    return res < 0;
}

bool FileName::operator<(const k2Base &other) const
{
    const FileName& other_file = (const FileName&)(other);
    int res = strcmp(this->get_file_name().c_str(), other_file.get_file_name().c_str());
    return res < 0;
}


bool FileName::operator<(const k3Base &other) const
{
    const FileName& other_file = (const FileName&)(other);
    int res = strcmp(this->get_file_name().c_str(), other_file.get_file_name()
            .c_str());
    return res < 0;
}


/**
 *
 * @param key
 * @param val
 */
void MapReduceSearch::Map(const k1Base *const key, const v1Base *const val) const
{
    FileName* file_key = (FileName*)(key);
    WordToSearch* word_val = (WordToSearch*)(val);
    string s2 = word_val->get_word();
    DIR *dir;
    struct dirent *ent;
    if((dir = opendir(file_key->get_file_name().c_str())))
    {
        while((ent = readdir(dir)) != NULL)
        {
            string s1 = (string)ent->d_name;
            if (s1.find(s2) != std::string::npos)
            {
                FileName* k2 = new FileName(s1);
                WordToSearch* v2 = new WordToSearch(s2);
                Emit2(k2, v2);
            }
        }
        closedir(dir);
    }

}

/**
 *
 * @param key
 * @param vals
 */
void MapReduceSearch::Reduce(const k2Base *const key, const V2_VEC &vals) const
{

    auto file_name = (FileName*)key;
    FileName* k3 = new FileName(file_name->get_file_name());
    unsigned long list_size = vals.size();
    auto v3 = new WordToSearch(to_string(list_size));
    Emit3(k3, v3);
}



///////////////////////////// THE SEARCH PROGRAM ///////////////////////////////


// -------------------------------- Functions ----------------------------------

/**
 * This function prepares the input for the map framework
 * @param programArguments An array of char(represents program arguments)
 * @param numOfArg An integer(represents the number of program arguments)
 * @return An IN_ITEMS_VEC variable(containing a data prepare to map)
 */
IN_ITEMS_VEC prepareToMap(char* programArguments[], int numOfArg)
{
    IN_ITEMS_VEC input_items_vec ((unsigned long)numOfArg-2);
    WordToSearch* v1 = new WordToSearch(programArguments[1]);
    for (int i = 2; i < numOfArg ; ++i)
    {
        FileName* k1 = new FileName(programArguments[i]);
        IN_ITEM input_item (k1, v1);
        input_items_vec[i-2] = input_item;
    }
    return input_items_vec;
}

/**
 * This function release resources
 */
void release_resources()
{
    delete mapInput[0].second;
    mapInput[0].second = nullptr;
    for (unsigned int j = 0; j < mapInput.size(); ++j)
    {
        delete mapInput[j].first;
        mapInput[j].first = nullptr;
    }
    for (unsigned int j = 0; j < search_output_vector.size(); ++j)
    {
        delete search_output_vector[j].first;
        search_output_vector[j].first = nullptr;
        delete search_output_vector[j].second;
        search_output_vector[j].second = nullptr;
    }
}

// -------------------------------- Main Function -----------------------------

/**
 * Runs the Search program:
 * Gets arguments - word to search and directories to search into and prints
 * all word found according to the format:"word1 word2 wor3...word".
 * this function returns 0 on success
 * @param argc  An integer(represents the number of program arguments)
 * @param argv An array of char(represents program arguments)
 * @return 0 on success
// */
int main(int argc, char * argv[])
{
    if (argc < VALID_ARG_NUM)
    {
        fprintf(stderr, "%s\n", thread_init_fail.c_str());
        exit(1);
    }
    if(argc == VALID_ARG_NUM)
    {
        return FUNC_SUCCESS;
    }
    //Initialize MapReduceFramework parameters
    mapInput = prepareToMap(argv, argc);
    MapReduceSearch m;
    //Runs the MapReduceFramework
    search_output_vector = RunMapReduceFramework(m, mapInput,
                                                 MULTI_THREADS_LEVEL,
                                                 frameworkDeleteResources);
    //print search output
    for (unsigned int i = 0; i < search_output_vector.size(); ++i) {
        const FileName *name = (const FileName *)
                (search_output_vector[i].first);
        const WordToSearch *num = (const WordToSearch *)
                (search_output_vector[i].second);
        int number_of_appearance = stoi(num->get_word());
        for (int j = 0; j < number_of_appearance; ++j)
        {
            cout << name->get_file_name().c_str();
            if((j < number_of_appearance-1 ) || (i < search_output_vector
                                                                 .size()-1))
            {
                cout << " ";
            }
        }
    }
    //release resources and return success
    release_resources();
    mapInput.clear();
    search_output_vector.clear();
    return FUNC_SUCCESS;
}