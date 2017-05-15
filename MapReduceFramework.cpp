
#include "MapReduceFramework.h"
#include <pthread.h>
#include <map>
#include <list>
#include <semaphore.h>
#include <algorithm>
#include <fstream>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <sys/time.h>

using namespace std;
#define CHUNK_SIZE 10
/**
 * TIME_MEASURE_FAIL -1
 * @brief  the value the function gettimeofday return when an error occured.
 */
#define TIME_MEASURE_FAIL -1

/**
 * FUNC_FAIL -1
 * @brief  A const value representing function failure
 */
#define FUNC_FAIL -1

/**
 * SEC_TO_NANO_CONST 1000000000
 * @brief A const value to convert seconds to nanoseconds
 */
#define SEC_TO_NANO_CONST 1000000000

/**
 * MICRO_TO_NANO_CONST 1000
 * @brief A const value to convert microseconds to nanoseconds
 */
#define MICRO_TO_NANO_CONST 1000

/**
 * FUNC_SUCCESS 0
 * @brief  A const value representing function success
 */
#define FUNC_SUCCESS 0

typedef std::pair<k2Base*, v2Base*> mapped_item;
typedef std::vector<mapped_item> mapped_vector;
typedef vector<v2Base*> shuffled_vec;//TODO combine type from MapReduceFrameworkto
typedef std::pair<k2Base*, shuffled_vec> shuffled_item;


//create next pair variable
int next_pair_to_read = 0;


///////////////////////   lockers /////////////////////////////////////////
pthread_mutex_t pthreadToContainer_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t nextValue_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t logFile_mutex = PTHREAD_MUTEX_INITIALIZER;

sem_t shuffle_sem;

bool finished_shuffle = false;

//lock/unlock result varaiable
int res;
int sem_val;


/////////////////////////////// DATA STRUCTURES  //////////////////////////////
IN_ITEMS_VEC input_vec;

map<pthread_t, mapped_vector> pthreadToContainer;

map<k2Base*, shuffled_vec> shuffledContainer;

vector<shuffled_item> shuffledVector;

OUT_ITEMS_VEC output_vector;

MapReduceBase* mapReduceBase;


int finishedMapThreads = -1;

//////////////////// LOG FILE MESSAGES ////////////////////////////////////////

string start_MapReduceFramwork1 = "RunMapReduceFramework started with ";
string start_MapReduceFramwork2 = " threads\n";
string finish_MapReduceFramwork = "RunMapReduceFramework finished\n";
string create_threadTypeMap = "Thread ExecMap created ";
string create_threadTypeShuffle = "Thread Shuffle created ";
string create_threadTypeReduce = "Thread ExecReduce created ";
string finish_threadTypeMap = "Thread ExecMap terminated ";
string finish_threadTypeShuffle = "Thread Shuffle terminated ";
string finish_threadTypeReduce = "Thread ExecReduce terminated ";
string time_for_Map_and_shuffle = "Map and Shuffle took ";
string time_for_Reduce = "Reduce took ";
string time_format = " ns\n";

//////////////////////////// LOG FILE FUNCTIONS ///////////////////////////////


// A vriable to the log file
ofstream outputFile;
void create_log_file()
{
//    outputFile.open(getcwd(".MapReduceFramwork.log"));
    outputFile.open("/cs/usr/hananbnz/safe/OS/ex_3/.MapReduceFramwork.log");
    if(!outputFile.is_open())
    {
        fprintf(stderr, "system error: %s\n", "ERROR opening Log File");
    }
}

void log_file_message(string txt)
{
    res  = pthread_mutex_lock(&logFile_mutex);
    outputFile << txt << endl;
    res  = pthread_mutex_unlock(&logFile_mutex);
}

void closing_log_file()
{
    outputFile.close();
}


string get_cur_time()
{
    auto t = time(nullptr);
    auto tm = *std::localtime(&t);

    stringstream ss;
    ss << put_time(&tm, "[%d.%m.%Y %H:%M:%S]");
    return ss.str();
}
/**
 * @brief  timeval structs to measure start and end time of different
 * operations in library
 */
struct timeval start_time, end_time;

/////////////////////////// PROGRAM FUNCTIONS /////////////////////////////////

/**
 * 1. Create ExecMap threads (pthreads) - each one of them will exec chunk of
 * pairs in the map func
 */

void *shuffle(void*)
{
    // TODO - need to
    // if semaphore is
    int res = sem_getvalue(&shuffle_sem, &sem_val);
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
                int sem_res = sem_wait(&shuffle_sem);
                if(sem_res != 0)
                {
                    // TODO write an error
                }
            }
            continue;
        }
    }
    finished_shuffle = true;
    next_pair_to_read = 0;
    res = sem_destroy(&shuffle_sem);
    if(res != 0)
    {
        fprintf(stderr, "system error: %s\n", "ERROR trying to destroy "
                "semaphore");
    }
    log_file_message(finish_threadTypeShuffle + get_cur_time()+"\n");
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
//    mapped_vector newMapVec = new mapped_vector;
    pthreadToContainer[pthread_self()]; // = newMapVec;
//    pthreadToContainer.insert(pair<pthread_t,
//            mapped_vector>(pthread_self(), newMapVec));
    // TODO the execmap func lock and unlock mutex and than map in mapReduce
//    MapReduceBase& mapReduce1 = (MapReduceBase&)mapReduce; // TODO check
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
        int begin = next_pair_to_read;
        unsigned long end = next_pair_to_read + current_chunk_size;
        next_pair_to_read += current_chunk_size;
        //unlocking mutex
        res  = pthread_mutex_unlock(&nextValue_mutex);
        for (int i = begin; i < end; ++i)
        {
            // Reading the pairs in the input vector one-by-one to map
            mapReduceBase->Map(input_vec[i].first, input_vec[i].second);

        }
        // thread will take CHUNK (or the last reminder) and read
        // TODO lock mutex if here or before the loop
    }
    // TODO need variable to
    finishedMapThreads --;
    log_file_message(finish_threadTypeMap + get_cur_time()+"\n");
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
//    pthreadToContainer.insert(pair<pthread_t,
//            mapped_vector>(pthread_self(), newMapVec));
//    mapped_vector* newMapVec = new mapped_vector;
    pthreadToContainer[pthread_self()];
    // TODO the execmap func lock and unlock mutex and than map in mapReduce
//    MapReduceBase& mapReduce1 = (MapReduceBase&)mapReduce; // TODO check
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
            mapReduceBase->Reduce(shuffledVector[i].first, shuffledVector[i]
                    .second);
        }
    }
    // TODO need variable to
    finishedMapThreads --;
    log_file_message(finish_threadTypeReduce + get_cur_time()+"\n");
//    pthread_exit(0);
}


OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce,
                                    IN_ITEMS_VEC&itemsVec,
                                    int multiThreadLevel,
                                    bool autoDeleteV2K2) {
    create_log_file();
    // Map&Shuffle measure time
    double total_time = 0;
    if (gettimeofday(&start_time, NULL) == TIME_MEASURE_FAIL)
    {
        // TODO Error
    };
    string num = to_string(multiThreadLevel);
    log_file_message(start_MapReduceFramwork1 + num + start_MapReduceFramwork2);

    mapReduceBase = &mapReduce;
    pthread_t *multiThreadLevel_threads[multiThreadLevel];
    // initialize semaphore for shuffle
    int sem_res = sem_init(&shuffle_sem, 0, 0);
    if (sem_res != 0) {
        // TODO error initializing semaphore
        fprintf(stderr, "system error: %s\n", "ERROR initializing semaphore");
    }
    finishedMapThreads = multiThreadLevel;
    //locking mutex
    res = pthread_mutex_lock(&pthreadToContainer_mutex);
    input_vec = itemsVec;

    //Map part
    for (int i = 0; i < multiThreadLevel; ++i) {
        // TODO creation of pthread
        pthread_t newExecMap = NULL;
//        mapped_vector* newMapVec = new mapped_vector;
        int thread_res = pthread_create(&newExecMap, NULL, ExecMapFunc, NULL);
        multiThreadLevel_threads[i] = &newExecMap;
        // TODO if have an error in creating a thread
        log_file_message(create_threadTypeMap + get_cur_time()+"\n");
    }
    /**
     * create the map size multiThreadLevel each with key - thread ID, val -
     * the thread ID container
     * check about the data structure that every container is in a different
     * location. create framework internal structure
     */

    if (pthreadToContainer.size() >= multiThreadLevel) {
        //unlocking mutex
        res = pthread_mutex_unlock(&pthreadToContainer_mutex);
        if(res != 0)
        {
            // TODO write an error
        }
    }
    // to map
    pthread_t shuffleThread = NULL;
    int thread_res = pthread_create(&shuffleThread, NULL, shuffle, NULL);
    log_file_message(create_threadTypeShuffle + get_cur_time()+"\n");
    if(thread_res != 0)
    {
        // TODO reuven write an error message
    }
    // join the ExecMap threads
    for (int j = 0; j < multiThreadLevel; ++j) {
        int res = pthread_join(*multiThreadLevel_threads[j], NULL);
        if (res != 0) {
            // TODO reuven write an error message
        }
    }
    // join the Shuffle thread
    int res = pthread_join(shuffleThread, NULL);
    if (res != 0) {
        // TODO reuven write an error message
    }

    if (gettimeofday(&end_time, NULL) == TIME_MEASURE_FAIL)
    {
        // TODO Error
    }
    total_time = ((end_time.tv_sec - start_time.tv_sec) * SEC_TO_NANO_CONST +
                  (end_time.tv_usec - start_time.tv_usec) * MICRO_TO_NANO_CONST);
    log_file_message(time_for_Map_and_shuffle + to_string(total_time)
                     + time_format);
    //Reduce part
    // Map&Shuffle measure time
    total_time = 0;
    if (gettimeofday(&start_time, NULL) == TIME_MEASURE_FAIL)
    {
        // TODO Error
    };
    prepare_to_reduce();
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_t *ExecReduce = NULL;
        int reduce_res = pthread_create(ExecReduce, NULL, ExecReduceFunc,NULL);
        multiThreadLevel_threads[i] = ExecReduce;
        if (reduce_res != 0) {
            printf("error");
        }
        log_file_message(create_threadTypeReduce + get_cur_time()+"\n");
    }
    for (int k = 0; k < multiThreadLevel; ++k) {
        int res = pthread_join(*multiThreadLevel_threads[k], NULL);
        if (res != 0) {
            // TODO reuven write an error message
        }
    }
    sort(output_vector.begin(), output_vector.end());
    if (gettimeofday(&end_time, NULL) == TIME_MEASURE_FAIL)
    {
        // TODO Error
    }
    total_time = ((end_time.tv_sec - start_time.tv_sec) * SEC_TO_NANO_CONST +
                  (end_time.tv_usec - start_time.tv_usec) * MICRO_TO_NANO_CONST);

    log_file_message(time_for_Reduce + to_string(total_time) + time_format);
//    printf(finish_MapReduceFramwork); // TODO print or write to log file
    return output_vector;


}
void Emit2 (k2Base* key, v2Base* val)
{
    // check what thread is running with self() and use the ID as a key
    // add to the shared container {<key - thread ID, val- thread map output container>}
    mapped_item new_pair = pair<k2Base*, v2Base*>(key, val);
    pthreadToContainer[pthread_self()].push_back(new_pair);
    // TODO semaphore +1
    int sem_res = sem_post(&shuffle_sem);
    if(sem_res != 0)
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
//    mapped_item new_pair = pair<k3Base*, v3Base*>(key, val);
    output_vector.push_back(pair<k3Base*, v3Base*>(key, val));
}