#include <cstdio>
#include <iostream>
#include <stdlib.h>
#include <cstring>
#include <dirent.h>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"

using namespace std;




/////////////////////////////CLASSES DEFINITIONS///////////////////////////////



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

///////////////////////// class functions implementation //////////////////////

/**
 * The iperator < implementation for the k1Base
 * @param other
 * @return
 */
bool FileName::operator<(const k1Base &other) const
{
    const FileName& other_file = dynamic_cast<const FileName&>(other);
    int res = strcmp(this->get_file_name().c_str(), other_file.get_file_name()
            .c_str());
    return res < 0;
}

bool FileName::operator<(const k2Base &other) const
{
    const FileName& other_file = dynamic_cast<const FileName&>(other);
    int res = strcmp(this->get_file_name().c_str(), other_file.get_file_name()
            .c_str());
    return res < 0;
}


bool FileName::operator<(const k3Base &other) const
{
    const FileName& other_file = dynamic_cast<const FileName&>(other);
    int res = strcmp(this->get_file_name().c_str(), other_file.get_file_name()
            .c_str());
    return res < 0;
}

void MapReduceSearch::Map(const k1Base *const key, const v1Base *const val) const
{
    const FileName* file_key = dynamic_cast<const FileName*>(key);
//    printf("Address of key is %p\n", (void *)key);
//    printf("Address of file_key is %p\n", (void *)file_key);
    const WordToSearch* word_val = dynamic_cast<const WordToSearch*>(val);
    string s2 = word_val->get_word();
    const char* directory_name = file_key->get_file_name().c_str();
    DIR *dir = opendir(directory_name);
    if(dir)
    {
        struct dirent *ent;
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
    }
}

void MapReduceSearch::Reduce(const k2Base *const key, const V2_VEC &vals) const
{
    auto k3 = (FileName*)key;
    unsigned long list_size = vals.size();
    auto v3 = new WordToSearch(to_string(list_size));
    Emit3(k3, v3);
}


//
///////////////////////////// THE SEARCH PROGRAM ///////////////////////////////




////////////////////////////////// Constants ///////////////////////////////////

#define VALID_ARG_NUM 2
string thread_init_fail = "Usage: <substring to search> "
        "<folders, separated by space>";

IN_ITEMS_VEC mapInput;
OUT_ITEMS_VEC search_output_vector;
bool frameworkDeleteResources = true;
int multiThreadLevel = 4;


////////////////////////////////// Functions ///////////////////////////////////

/**
 *
 * @param programArguments
 * @param numOfArg
 * @return
 */
IN_ITEMS_VEC prepareToMap(char* programArguments[], int numOfArg)
{
    IN_ITEMS_VEC input_items_vec (numOfArg-2); // TODO check intialize values
    //TODO make pairs of (K1,V1), where V1 is the first arg - the word to search
    // and the K1's are the filenames arguments.
    WordToSearch* v1 = new WordToSearch(programArguments[1]);
    for (int i = 2; i < numOfArg ; ++i)
    {
        FileName* k1 = new FileName(programArguments[i]);
        IN_ITEM input_item (k1, v1);
        input_items_vec[i-2] = input_item;
    }
    return input_items_vec;
}


void release_resources()
{
    for (int j = 0; j < mapInput.size(); ++j)
    {
        delete mapInput[j].first;
//        delete mapInput[j].second;
    }
    for (int j = 0; j < search_output_vector.size(); ++j)
    {
        delete search_output_vector[j].first;
        delete search_output_vector[j].second;
    }
}


/////////////////////////////// Main Function  /////////////////////////////////



/**
 * 
 * @param argc 
 * @param argv 
 * @return 
 */
int main(int argc, char * argv[])
{
    if (argc < VALID_ARG_NUM)
    {
        fprintf(stderr, "%s\n", thread_init_fail.c_str());
        exit(1);

    }
//    IN_ITEMS_VEC mapInput;
//    for (int k = 0; k < 20; ++k)
//    {

        mapInput = prepareToMap(argv, argc);
        MapReduceSearch m;
//        OUT_ITEMS_VEC search_output_vector;
        search_output_vector = RunMapReduceFramework(m, mapInput,
                                                     multiThreadLevel,
                                                     frameworkDeleteResources);
        // values
        for (int i = 0; i < search_output_vector.size(); ++i) {
            const FileName *name = dynamic_cast<const FileName *>(search_output_vector[i].first);
            const WordToSearch *num = dynamic_cast<const WordToSearch *>(search_output_vector[i].second);
            int number_of_appearance = stoi(num->get_word());
//        printf("num of app %d \n", number_of_appearance);
            for (int j = 0; j < number_of_appearance; ++j) {
                printf("%s ", name->get_file_name().c_str());
                fflush(stdout);
            }
        }
        release_resources();
    //TODO release k2,v2 resources
//        mapInput.clear();
//        search_output_vector.clear();
//        printf("\n");
//        fflush(stdout);
//    }
}




//..................................TESTS .................................  //

//
//    DIR *dir = opendir("/cs/usr/hananbnz/safe/OS/ex_3/os2015/exercise");
//    if(dir)
//    {
//        struct dirent *ent;
//        while((ent = readdir(dir)) != NULL)
//        {
//            string s1 = (string)ent->d_name;
//            if (s1.find("os") != std::string::npos)
//            {
//                std::cout << s1 <<" found!" << '\n';
//            }
//        }
//    }
//    printf("search running....\n");
//    printf("search for 'os' in os2015/exercise blabla myFolder\n");
//    printf("found 'osTargil' 'sos' 'sos'\n");
//    printf("search finished...\n");


