
#include "MapReduceFramework.h"
#include <pthread.h>
#include <map>

using namespace std;
#define CHUNK_SIZE 10


typedef std::pair<k2Base*, v2Base*> mapped_item;
typedef std::vector<mapped_item> mapped_vector;



//create next pair variable
int next_pair_to_read = 0;

pthread_mutex_t pthreadToContainer_mutex = PTHREAD_MUTEX_INITIALIZER;

//lock/unlock result varaiable
int res;

// TODO implement RunMapReduceFramework, Emit2, Emit3
//class MapReduceFramework
//{
//public:
//
//
//
//private:
//
//};

/**
 * 1. Create ExecMap threads (pthreads) - each one of them will exec chunk of
 * pairs in the map func
 */





OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2)
{
    //locking mutex
    res  = pthread_mutex_lock(&pthreadToContainer_mutex);


    for (int i = 0; i <multiThreadLevel ; ++i)
    {
        // TODO creation of pthread
        pthread_t* cur_thread = pthread_create();

    }
    // create the map size multiThreadLevel each with key - thread ID, val -
    // the thread ID container
    // check about the data structure that every container is in a different location.
    //create framework internal structure
    map<pthread_t, mapped_vector> pthreadToContainer;
    if(pthreadToContainer.size() >= multiThreadLevel)
    {
        //unlocking mutex
        res  = pthread_mutex_unlock(&pthreadToContainer_mutex);
    }

}

void mapping(const int vec_size )
{
    while (true)
    {
        if(next_pair_to_read < vec_size)
        {
            // thread will take CHUNK (or the last reminder) and read
        }
    }

}

void ExecMap(MapReduceBase& mapReduce)
{
    // TODO the execmap func lock and unlock mutex and than map in mapReduce
    //locking mutex
    res  = pthread_mutex_lock(&pthreadToContainer_mutex);
    //unlocking mutex
    res  = pthread_mutex_unlock(&pthreadToContainer_mutex);
    // TODO in different func - read a chunk of pairs if still need to
    // in the func will send one-by-one pairs to map



}
void Emit2 (k2Base*, v2Base*)
{
    // check what thread is running with self() and use the ID as a key
    // add to the shared container {<key - thread ID, val- thread map output container>}

}

//create shuffle thread with the creation of execMap threads
//merge all containers with the same key
//converts list of <k2,v2> to list <k2,list(v2)>