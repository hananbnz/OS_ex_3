#include <cstdio>
#include <iostream>
#include <stdlib.h>
//#include "string"
#include <stdbool.h>
#include <cstring>
#include <dirent.h>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"

using namespace std;

class FileNameKey : public k1Base, public k2Base, public k3Base
{
public:
    /**
     * A default FileNameKey constructor.
     */
    FileNameKey(){};

    FileNameKey(string file_name)  {_fileName = file_name;};

    /**
     * A FileNameKey destructor.
     */
    ~FileNameKey(){};

    string get_file_name() const { return _fileName;};

    bool operator<(const k1Base &other) const;

    bool operator<(const k2Base &other) const;

    bool operator<(const k3Base &other) const;

private:
    string _fileName;
};


class WordSearch: public v1Base, public v2Base, public v3Base
{
public:
    /**
     * A default FileNameKey constructor.
     */
    WordSearch(){};

    WordSearch(string  word_search)  {_word_search = word_search;};

    /**
     * A FileNameKey destructor.
     */
    ~WordSearch(){};

    string get_word() const { return _word_search;};

private:
    string  _word_search;

};


class MapReduceSearch: public MapReduceBase
{
public:

    void Map(const k1Base *const key, const v1Base *const val) const;
//    {
//        const FileNameKey* file_key = dynamic_cast<const FileNameKey*>(key);
//        const WordSearch* word_val = dynamic_cast<const WordSearch*>(val);
//        string s2 = word_val->get_word();
//        char* directory_name = ((char*)file_key->get_file_name().c_str());
//        DIR *dir = opendir(directory_name);
//        if(dir)
//        {
//            struct dirent *ent;
//            while((ent = readdir(dir)) != NULL)
//            {
//                string s1 = (string)ent->d_name;
//                if (s1.find(s2) != std::string::npos)
//                {
//                    FileNameKey* k2 = new FileNameKey(s1);
//                    WordSearch* v2 = new WordSearch(s2);
//                    Emit2(k2, v2);
//                }
//            }
//        }
//    }

    void Reduce(const k2Base *const key, const V2_VEC &vals) const;
//    {
//        {
//            auto k3 = (FileNameKey*)key;
//            unsigned long list_size = vals.size();
//            auto v3 = new WordSearch(to_string(list_size));
//            Emit3(k3, v3);
//        }
//    }

};

///////////////////////// class functions implementation //////////////////////

/**
 * The iperator < implementation for the k1Base
 * @param other
 * @return
 */
bool FileNameKey::operator<(const k1Base &other) const
{
    const FileNameKey& other_file = dynamic_cast<const FileNameKey&>(other);
//    const FileNameKey& this_file = dynamic_cast<const FileNameKey&>(this);
    int res = strcmp(this->get_file_name().c_str(), other_file.get_file_name
            ().c_str());
    //TODO changed to char*
    //int res = strcmp(this->get_file_name().c_str(), other_file.get_file_name().c_str());

    if(res < 0)
    {
        return true;
    }
    return false;
}

bool FileNameKey::operator<(const k2Base &other) const
{
    const FileNameKey& other_file = dynamic_cast<const FileNameKey&>(other);
//    const FileNameKey& this_file = dynamic_cast<const FileNameKey&>(this);
    int res = strcmp(this->get_file_name().c_str(), other_file.get_file_name().c_str());

    if(res < 0)
    {
        return true;
    }
    return false;
}

bool FileNameKey::operator<(const k3Base &other) const
{
    const FileNameKey& other_file = dynamic_cast<const FileNameKey&>(other);
//    const FileNameKey& this_file = dynamic_cast<const FileNameKey&>(this);
    int res = strcmp(this->get_file_name().c_str(), other_file.get_file_name().c_str());

    if(res < 0)
    {
        return true;
    }
    return false;
}

void MapReduceSearch::Map(const k1Base *const key, const v1Base *const val) const
{
    const FileNameKey* file_key = dynamic_cast<const FileNameKey*>(key);
    const WordSearch* word_val = dynamic_cast<const WordSearch*>(val);
    string s2 = word_val->get_word();
    char* directory_name = ((char*)file_key->get_file_name().c_str());
    DIR *dir = opendir(directory_name);
    if(dir)
    {
        struct dirent *ent;
        while((ent = readdir(dir)) != NULL)
        {
            string s1 = (string)ent->d_name;
            if (s1.find(s2) != std::string::npos)
            {
                FileNameKey* k2 = new FileNameKey(s1);
                WordSearch* v2 = new WordSearch(s2);
                Emit2(k2, v2);
            }
        }
    }
}

void MapReduceSearch::Reduce(const k2Base *const key, const V2_VEC &vals) const
{
    auto k3 = (FileNameKey*)key;
    unsigned long list_size = vals.size();
    auto v3 = new WordSearch(to_string(list_size));
    Emit3(k3, v3);
}

/////////////////////////////// The Search program /////////////////////////////


#define VALID_ARG_NUM 2



string thread_init_fail = "Usage: <substring to search> <folders, separated by space>";

void thread_library_function_fail(string text)
{
    fprintf(stderr, "MapReduceFramework Failure: %s failed\n", text.c_str());
}

IN_ITEMS_VEC prepareToMap(char* programArguments[], int numOfArg)
{
    IN_ITEMS_VEC input_items_vec (numOfArg-2); // TODO check intialize values
    //TODO make pairs of (K1,V1), where V1 is the first arg - the word to search
    // and the K1's are the filenames arguments.
    WordSearch* v1 = new WordSearch(programArguments[1]);
    for (int i = 2; i < numOfArg ; ++i)
    {
        FileNameKey* k1 = new FileNameKey(programArguments[i]);
        IN_ITEM input_item (k1, v1); // = new IN_ITEM(k1,v1);
        input_items_vec[i-2] = input_item;
    }
    return input_items_vec;

}


///
/// \param argc
/// \param argv
/// \return

int main(int argc, char * argv[])
{
    if (argc < VALID_ARG_NUM)
    {
        fprintf(stderr, "%s\n", thread_init_fail.c_str());
        exit(1);

    }
    IN_ITEMS_VEC mapInput = prepareToMap(argv, argc);
    MapReduceSearch m;
    OUT_ITEMS_VEC output_vector;
    output_vector = RunMapReduceFramework(m, mapInput, 4, true);//TODO check
    // values

    for (int i = 0; i < output_vector.size() ; ++i)
    {
        const FileNameKey* name = dynamic_cast<const FileNameKey*>(output_vector[i].first);
        const WordSearch*  num = dynamic_cast<const WordSearch*>(output_vector[i].second);
        int number_of_appearance = stoi(num->get_word());
        for (int j = 0; j < number_of_appearance ; ++j)
        {
            printf("%s ", name->get_file_name());
        }
    }
    //TODO release memory

//    DIR *dir = opendir("/cs/usr/reuveny/safe/OS/ex_3/os2015/exercise");
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

}


//void prepareToMap(std::string input_txt);