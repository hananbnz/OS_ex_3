//
// Created by reuveny on 5/7/17.
//

#include "MapReduceFramework.h"
#include <pthread.h>

#define CHUNK_SIZE 10

// TODO implement RunMapReduceFramework, Emit2, Emit3
class MapReduceFramework
{

};

/**
 * 1. Create ExecMap threads (pthreads) - each one of them will exec chunk of
 * pairs in the map func
 */


OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2)
{
    for (int i = 0; i <multiThreadLevel ; ++i)
    {
        // TODO creation of pthread
        pthread_t* cur_thread = pthread_create();

    }
    // create the map size multiThreadLevel each with key - thread ID, val -
    // the thread ID container
}

