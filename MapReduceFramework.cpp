
#include "MapReduceFramework.h"
#include <pthread.h>
#include <map>
#include <list>
#include <semaphore.h>
#include <algorithm>

using namespace std;
#define CHUNK_SIZE 10

//struct sort_pred
//{
//    bool operator()(const std::pair<k3Base*, v3Base*> &left, const std::pair<k3Base*, v3Base*> &right)
//    {
//        return left.first < right.first;
//    }
//};

typedef std::pair<k2Base*, v2Base*> mapped_item;
typedef std::vector<mapped_item> mapped_vector;
typedef vector<v2Base*> shuffled_vec;//TODO combine type from MapReduceFrameworkto
typedef std::pair<k2Base*, shuffled_vec> shuffled_item;


//create next pair variable
int next_pair_to_read = 0;


///////////////////////   lockers /////////////////////////////////////////
pthread_mutex_t pthreadToContainer_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t nextValue_mutex = PTHREAD_MUTEX_INITIALIZER;

sem_t* shuffle_sem;

bool finished_shuffle = false;

//lock/unlock result varaiable
int res;

IN_ITEMS_VEC input_vec;

map<pthread_t, mapped_vector> pthreadToContainer;

map<k2Base*, shuffled_vec> shuffledContainer;

vector<shuffled_item> shuffledVector;

OUT_ITEMS_VEC output_vector;



int finishedMapThreads = -1;


/**
 * 1. Create ExecMap threads (pthreads) - each one of them will exec chunk of
 * pairs in the map func
 */

void shuffle( )
{
    // TODO - need to
    // if semaphore is
    int *sem_val;
    int res = sem_getvalue(shuffle_sem, sem_val);
    if (res != 0)
    {
        // TODO reuven write an erorr
    }
    while(sem_val > 0 ||  finishedMapThreads != 0) // every time will check
        // again
        // for
    {
            // place semaphore ??!
        for (auto it = pthreadToContainer.begin(); it != pthreadToContainer.end();
             ++it)
        {
            // iterate over the different containers in the map pthreadToContainer
            // if the container is empty - pass
            // if not - so take the pair and put it in the shuffle map with
            // <key - the matched word, val - list of search word (length ++1)>
            // implement semaphore down.
            if(!(it->second.empty()))
            {
                k2Base* newKey = it->second.back().first;
                v2Base* newVal = it->second.back().second;
                shuffledContainer[newKey].push_back(newVal);
                it->second.pop_back();
                // TODO semaphore DOWN
                int sem_res = sem_wait(shuffle_sem);
                if(sem_res < 0)
                {
                    // TODO write an error
                }
            }
            continue;
        }
    }
    finished_shuffle = true;
    next_pair_to_read = 0;
//    pthread_exit(0);
}

unsigned long set_chunk_size()
{
    unsigned long current_chunk_size = CHUNK_SIZE;
    if((next_pair_to_read + current_chunk_size) > input_vec.size())
    {
        current_chunk_size = input_vec.size() - next_pair_to_read;
    }
    return current_chunk_size;
}

void *ExecMapFunc(void* mapReduce)
{
    mapped_vector* newMapVec = new mapped_vector;
    pthreadToContainer.insert(pair<pthread_t,
            mapped_vector>(pthread_self(), newMapVec));
    // TODO the execmap func lock and unlock mutex and than map in mapReduce
    MapReduceBase& mapReduce1 = (MapReduceBase&)mapReduce; // TODO check
    //locking mutex
    res  = pthread_mutex_lock(&pthreadToContainer_mutex);
    //unlocking mutex
    res  = pthread_mutex_unlock(&pthreadToContainer_mutex);
    // TODO in different func - read a chunk of pairs if still need to
    // in the func will send one-by-one pairs to map
    while (true)
    {
        if(next_pair_to_read >= input_vec.size())
        {
            break;
        }
        //locking mutex
        res  = pthread_mutex_lock(&nextValue_mutex);
        unsigned long current_chunk_size = set_chunk_size();
//        //locking mutex
//        res  = pthread_mutex_lock(&nextValue_mutex);
        int begin = next_pair_to_read;
        unsigned long end = next_pair_to_read + current_chunk_size;
        next_pair_to_read += current_chunk_size;
        //unlocking mutex
        res  = pthread_mutex_unlock(&nextValue_mutex);
        for (int i = begin; i < end; ++i)
        {
            // Reading the pairs in the input vector one-by-one to map
            mapReduce1.Map(input_vec[i].first, input_vec[i].second);

        }
        // thread will take CHUNK (or the last reminder) and read
        // TODO lock mutex if here or before the loop
    }
    // TODO need variable to
    finishedMapThreads --;
//    pthread_exit(0);
}

void prepare_to_reduce()
{
    for (auto it = shuffledContainer.begin(); it != shuffledContainer.end();
         ++it)
    {
        shuffledVector.push_back(shuffled_item(it->first, it->second));
    }
}

void *ExecReduceFunc(void* mapReduce)
{
    //create a container for each thread
    mapped_vector* newMapVec = new mapped_vector;
    pthreadToContainer.insert(pair<pthread_t,
            mapped_vector>(pthread_self(), newMapVec));
    // TODO the execmap func lock and unlock mutex and than map in mapReduce
    MapReduceBase& mapReduce1 = (MapReduceBase&)mapReduce; // TODO check
    while (true)
    {
        if(next_pair_to_read >= input_vec.size())
        {
            break;
        }
        //locking mutex
        res  = pthread_mutex_lock(&nextValue_mutex);
        unsigned long current_chunk_size = set_chunk_size();
        int begin = next_pair_to_read;
        unsigned long end = next_pair_to_read + current_chunk_size;
        next_pair_to_read += current_chunk_size;
        //unlocking mutex
        res  = pthread_mutex_unlock(&nextValue_mutex);


        for (int i = begin; i < end; ++i)
        {
            // Reading the pairs in the input vector one-by-one to map
            mapReduce1.Reduce(shuffledVector[i].first, shuffledVector[i].second);
        }
    }
    // TODO need variable to
    finishedMapThreads --;
//    pthread_exit(0);
}


OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2) {
    pthread_t *multiThreadLevel_threads[multiThreadLevel];
    // initialize semaphore for shuffle
    int sem_res = sem_init(shuffle_sem, 0, 0);
    if (sem_res < 0) {
        // TODO error initializing semaphore
    }
    finishedMapThreads = multiThreadLevel;
    //locking mutex
    res = pthread_mutex_lock(&pthreadToContainer_mutex);
    input_vec = itemsVec;

    //Map part
    for (int i = 0; i < multiThreadLevel; ++i) {
        // TODO creation of pthread
        pthread_t *newExecMap = NULL;
//        mapped_vector* newMapVec = new mapped_vector;
        int thread_res = pthread_create(newExecMap, NULL, ExecMapFunc,
                                        (void *) mapReduce);
        multiThreadLevel_threads[i] = newExecMap;
        // TODO if have an error in creating a thread
    }
    // create the map size multiThreadLevel each with key - thread ID, val -
    // the thread ID container
    // check about the data structure that every container is in a different location.
    // create framework internal structure

    if (pthreadToContainer.size() >= multiThreadLevel) {
        //unlocking mutex
        res = pthread_mutex_unlock(&pthreadToContainer_mutex);
    }
    for (int j = 0; j < multiThreadLevel; ++j) {

    }
    // to map
    pthread_t *shuffleThread = NULL;
    int thread_res = pthread_create(shuffleThread, NULL, shuffle, NULL);
    for (int k = 0; k < multiThreadLevel; ++k) {
        int res = pthread_join(*multiThreadLevel_threads[k], NULL);
        if (res < 0) {
            // TODO reuven write an error message
        }
    }
    //Reduce part
    while (!finished_shuffle) {}
    prepare_to_reduce();
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_t *ExecReduce = NULL;
        int reduce_res = pthread_create(ExecReduce, NULL, ExecReduceFunc,
                                        (void *) mapReduce);
        multiThreadLevel_threads[i] = ExecReduce;
        if (reduce_res != 0) {
            printf("error");
        }
    }
    for (int k = 0; k < multiThreadLevel; ++k) {
        int res = pthread_join(*multiThreadLevel_threads[k], NULL);
        if (res < 0) {
            // TODO reuven write an error message
        }
    }
    sort(output_vector.begin(), output_vector.end());
    return output_vector;


}
void Emit2 (k2Base* key, v2Base* val)
{
    // check what thread is running with self() and use the ID as a key
    // add to the shared container {<key - thread ID, val- thread map output container>}
    mapped_item new_pair = pair<k2Base*, v2Base*>(key, val);
    pthreadToContainer[pthread_self()].push_back(new_pair);
    // TODO semaphore +1
    int sem_res = sem_post(shuffle_sem);
    if(sem_res < 0)
    {
        // TODO write an error
    }
}

//create shuffle thread with the creation of execMap threads
//merge all containers with the same key
//converts list of <k2,v2> to list <k2,list(v2)>

void Emit3 (k3Base* key, v3Base* val)
{
    // check what thread is running with self() and use the ID as a key
    // add to the shared container {<key - thread ID, val- thread map output container>}
    mapped_item new_pair = pair<k3Base*, v3Base*>(key, val);
    output_vector.push_back(new_pair);
}
