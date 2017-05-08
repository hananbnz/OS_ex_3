

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
    FileNameKey();

    /**
     * A FileNameKey destructor.
     */
    ~FileNameKey();

    string get_file_name() const { return _fileName;};

    bool operator<(const FileNameKey &other) const;
//    bool operator==()(const k1Base &other) const; TODO decide if want to impelemnt

private:
    string _fileName;
};

class SearchWord: public v1Base
{

};

///////////////////////// class functions implementation //////////////////////

/**
 * The iperator < implementation for the k1Base
 * @param other
 * @return
 */
bool operator<(const FileNameKey &other) const //TODO Does this fileNameKey change works???
{
    int res = strcmp(this->get_file_name(), other.get_file_name());

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

void prepareToMap(char* programArguments)
{
    //TODO make pairs of (K1,V1), where V1 is the first arg - the word to search
    // and the K1's are the filenames arguments.

}


int main(int argc, char * argv[])
{
//    for (int i = 0; i <argc ; ++i) {
//        printf("%s\n",argv[i]);
//    }
    if (argc < VALID_ARG_NUM)
    {
        fprintf(stderr, "%s\n", thread_init_fail.c_str());
        exit(1);

    }
//    printf("search running....\n");
//    printf("search for 'os' in os2015/exercise blabla myFolder\n");
//    printf("found 'osTargil' 'sos' 'sos'\n");
//    printf("search finished...\n");
}


//void prepareToMap(std::string input_txt);