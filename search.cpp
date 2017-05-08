

#include <cstdio>
#include <iostream>
#include <stdlib.h>
//#include "string"
#include <stdbool.h>
#include <cstring>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"

using namespace std;

class FileNameKey : public k1Base
{
public:
    /**
     * A default FileNameKey constructor.
     */
    FileNameKey(){};

    FileNameKey(char * file_name)  {_fileName = file_name;};

    /**
     * A FileNameKey destructor.
     */
    ~FileNameKey(){};

    string get_file_name() const { return _fileName;};

    bool operator<(const k1Base &other) const;
//    bool operator==()(const k1Base &other) const; TODO decide if want to impelemnt

private:
    string _fileName;
};

class WordSearch: public v1Base
{
public:
    /**
     * A default FileNameKey constructor.
     */
    WordSearch(){};

    WordSearch(char * word_search)  {_word_search = word_search;};

    /**
     * A FileNameKey destructor.
     */
    ~WordSearch(){};

    string get_word() const { return _word_search;};

private:
    char *  _word_search;

};

///////////////////////// class functions implementation //////////////////////

/**
 * The iperator < implementation for the k1Base
 * @param other
 * @return
 */
bool FileNameKey::operator<(const k1Base &other) const //TODO Does this fileNameKey change works???
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

int main(int argc, char * argv[])
{
    if (argc < VALID_ARG_NUM)
    {
        fprintf(stderr, "%s\n", thread_init_fail.c_str());
        exit(1);

    }
    IN_ITEMS_VEC mapInput = prepareToMap(argv, argc);
//    printf("search running....\n");
//    printf("search for 'os' in os2015/exercise blabla myFolder\n");
//    printf("found 'osTargil' 'sos' 'sos'\n");
//    printf("search finished...\n");
}


//void prepareToMap(std::string input_txt);