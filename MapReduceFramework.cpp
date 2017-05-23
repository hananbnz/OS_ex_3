
// ------------------------------- Includes ----------------------------------

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
#include <unistd.h>
#include <sys/time.h>
#include <iostream>

using namespace std;
typedef unsigned long unsign_l;
typedef unsigned int unsign_i;


// -------------------------------- Constants ---------------------------------

/**
 * CHUNK_SIZE 10
 * @brief  the value of a chunk size for each thread
 */
#define CHUNK_SIZE 10

/**
 * TIME_MEASURE_FAIL -1
 * @brief  the value the function gettimeofday return when an error occured.
 */
#define TIME_MEASURE_FAIL -1

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
 * LOG_BUF_SIZE 1024
 * @brief A const value represents log file buffer size
 */
#define LOG_BUF_SIZE 1024

/**
 * FUNC_SUCCESS 0
 * @brief  A const value representing function success
 */
#define FUNC_SUCCESS 0

typedef unsigned long unsign_l;
typedef unsigned int unsign_i;

// ----------------------------- Global variables ---------------------------


/**
 * A boolean variable to determine if the framework is responsible for
 * releasing resources
 */
bool deleteV2K2 = false;

/**
 * A boolean variable represents if the mapping finished
 */
bool finishedMapThreads = false;

/**
 * Represents an index of the next pair to read
 */
unsign_l next_pair_to_read = 0;

/**
 * An ofstream for log file handling
 */
ofstream outputFile;

/**
 * A char array for log file handling
 */
char buf[LOG_BUF_SIZE];

// -------------------------- Mutexes / Semaphores ----------------------------

pthread_mutex_t pthreadToContainer_Map_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t pthreadToContainer_Reduce_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t nextValue_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t logFile_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t finished_Map_Threads_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t check_time_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t emit3_insert = PTHREAD_MUTEX_INITIALIZER;

sem_t shuffle_sem;

//lock/unlock result varaiables
//int res;



// -------------------------- SHARED DATA STRUCTURES  ------------------------

typedef std::pair<k2Base*, v2Base*> mapped_item;
typedef std::vector<mapped_item> mapped_vector;
typedef std::pair<k2Base*, V2_VEC> shuffled_item;

vector<mapped_item> garbage_collector;

vector<pthread_t> multiThreadLevel_threads_Map;
vector<pthread_t> multiThreadLevel_threads_Reduce;

//vector<pair<k1Base*, v1Base*>> IN_ITEMS_VEC;
IN_ITEMS_VEC input_vec;

map<pthread_t, mapped_vector> pthreadToContainer_Map;

map<pthread_t, mapped_vector> pthreadToContainer_Reduce;

map<pthread_t, pthread_mutex_t> mutex_map;

//vector <pair<k2Base*, vector<v2Base*>>> shuffled_item;
vector<shuffled_item> shuffledVector;

OUT_ITEMS_VEC output_vector;

MapReduceBase* mapReduceBase;

struct sort_pred
{
    bool operator()(std::pair<k3Base*, v3Base*> &left,
                    std::pair<k3Base*, v3Base*> &right)
    {
        return *(left.first) < *(right.first);
    }
};

// ----------------------------- Error Messages  -----------------------------


//Framework Error Messages
string pthread_create_fail = "pthread_create";
string pthread_join_fail = "pthread_join";
string sem_post_fail = "sem_post";
string sem_getvalue_fail = "sem_getvalue";
string sem_wait_fail = "sem_wait";
string sem_destroy_fail = "sem_destroy";
string new_fail = "new";
string read_fail = "read";
string open_fail = "open";
string gettimeofday_fail = "gettimeofday";
string sem_init_fail = "sem_init";
string pthread_mutex_unlock_fail = "pthread_mutex_unlock";
string pthread_mutex_lock_fail = "pthread_mutex_lock";

//LOG FILE MESSAGES
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
string Log_file_name = ".MapReduceFramework.log";


// ---------------------- Framework Error FUNCTIONS  ---------------------------

void framework_function_fail(string text)
{
    fprintf(stderr, "MapReduceFramework Failure: %s failed\n", text.c_str());
    exit(1);
}

// --------------------------  LOG FILE FUNCTIONS  ---------------------------

void create_log_file()
{

    char* r_buf;
    r_buf = getcwd(buf, LOG_BUF_SIZE);
    string file_to_open= (string)r_buf + Log_file_name;
    outputFile.open(file_to_open);
    if(!outputFile.is_open())
    {
        fprintf(stderr, "system error: %s\n", "ERROR opening Log File");
    }
}

void log_file_message(string txt)
{
    int res  = pthread_mutex_lock(&logFile_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_lock_fail);
    }
    outputFile << txt << endl;
    res  = pthread_mutex_unlock(&logFile_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_unlock_fail);
    }
}

void closing_log_file()
{
    int res  = pthread_mutex_lock(&logFile_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_lock_fail);
    }
    outputFile.close();
    res  = pthread_mutex_unlock(&logFile_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_unlock_fail);
    }
}


string get_cur_time()
{
    int res  = pthread_mutex_lock(&check_time_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_lock_fail);
    }
    auto t = time(nullptr);
    auto tm = *std::localtime(&t);

    stringstream ss;
    ss << put_time(&tm, "[%d.%m.%Y %H:%M:%S]");
    res  = pthread_mutex_unlock(&check_time_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_unlock_fail);
    }
    return ss.str();
}
/**
 * @brief  timeval structs to measure start and end time of different
 * operations in library
 */
struct timeval start_time, end_time;




// ------------------------------ PROGRAM FUNCTIONS --------------------------

/**
 * This function will release all the mutex resources that were in the
 * MapReduceFrameWork
 */
void release_mutex_resources()
{
    pthread_mutex_destroy(&pthreadToContainer_Map_mutex);

    pthread_mutex_destroy(&pthreadToContainer_Reduce_mutex);

    pthread_mutex_destroy(&nextValue_mutex);

    pthread_mutex_destroy(&logFile_mutex);

    pthread_mutex_destroy(&check_time_mutex);

    pthread_mutex_destroy(&finished_Map_Threads_mutex);

    pthread_mutex_destroy(&emit3_insert);

    for (auto &map: mutex_map)
    {
        pthread_mutex_destroy(&map.second);
    }
}

/**
 *
 */
void release_V2_K2_resources()
{
    for (unsign_i l = 0; l < shuffledVector.size(); ++l)
    {
        if(shuffledVector[l].first != nullptr)
        {
            for (unsign_i i = 0; i < shuffledVector[l].second.size(); ++i)
            {
                delete shuffledVector[l].second[i];
                shuffledVector[l].second[i] = nullptr;
            }
            delete shuffledVector[l].first;
            shuffledVector[l].first = nullptr;
        }
    }
}

shuffled_item create_new_item(v2Base* newVal, k2Base* newKey)
{
    V2_VEC new_vec;
    new_vec.push_back(newVal);
    shuffled_item new_item = pair<k2Base*, V2_VEC>(newKey, new_vec);
    return new_item;
}

/**
 * This function gets  a vector size and according to next_pair_to_read and
 * CHUNK_SIZE values calculates and returns the current chunk size.
 * @param vec_size an unsigned long represents a vector size.
 * @return an unsigned long represents the current chunk size.
 */
unsign_l set_chunk_size(unsign_l vec_size)
{
    unsign_l current_chunk_size = CHUNK_SIZE;
    if((next_pair_to_read + current_chunk_size) > vec_size)
    {
        current_chunk_size = vec_size - next_pair_to_read;
    }
    return current_chunk_size;
}

/**
 * This function implement the selection of the chunk the threads should work
 * on during the Map/Reduce operation
 * @param vec_size - the size of the unput vector
 * @return the begin and end indeces of the chunk the thread should work on
 */
//unsign_l selecting_chunk(unsign_l vec_size)         // TODO check if works
//{
//    // selecting a chunk of pairs to work on
//    unsign_l current_chunk_size = set_chunk_size(vec_size);
//    unsign_l begin = next_pair_to_read;
//    unsign_l end = next_pair_to_read + current_chunk_size;
//    next_pair_to_read += current_chunk_size;
//    return begin,end;
//}

void re_initialize_next_pair_to_read()
{
    // change the next pair to read value
    int res  = pthread_mutex_lock(&nextValue_mutex);
    if(res !=0)framework_function_fail(pthread_mutex_lock_fail);
    next_pair_to_read = 0;
    res  = pthread_mutex_unlock(&nextValue_mutex);
    if(res != 0)framework_function_fail(pthread_mutex_unlock_fail);
}

///////////////// The different Threads Functions /////////////////////////////
/**
 * In this Function the shuffle thread will run the shuffle operation in the
 * MapReduceFramework
 * @return nothing
 */
void *shuffle(void*)
{
    int sem_val;
    // Gets semaphore value
    int res = sem_getvalue(&shuffle_sem, &sem_val);
    if (res != 0)
    {
        framework_function_fail(sem_getvalue_fail);
    }
    // while semaphore value>0 and threads left shuffle keeps running
    res  = pthread_mutex_lock(&finished_Map_Threads_mutex);
    if(res != 0)framework_function_fail(pthread_mutex_lock_fail);
    // while semaphore value > 0 and Map threads left, shuffle keeps running
    while(sem_val > 0 || !finishedMapThreads)
    {
        // unlock finished_Map_Threads_mutex
        res  = pthread_mutex_unlock(&finished_Map_Threads_mutex);
        if(res != 0)framework_function_fail(pthread_mutex_lock_fail);
        for (auto &it :pthreadToContainer_Map)
        {
            res = pthread_mutex_lock(&(mutex_map[it.first]));
            if(res != 0)framework_function_fail(pthread_mutex_lock_fail);
            while(!(it.second.empty())) //while container not empty
            {
                bool is_key_exist = false;
                // check if the key is in the container
                for (unsign_i i = 0; i < shuffledVector.size(); ++i)
                {
                    if(!(*it.second.back().first < *(shuffledVector[i].first)) &&
                        !(*(shuffledVector[i].first) < *it.second.back().first))
                    {
                        shuffledVector[i].second.push_back(it.second.back().second);
                        if(deleteV2K2) delete it.second.back().first;
                        is_key_exist = true;
                        break;
                    }
                }
                // If key wasn't already in container, creates a new key
                if(!is_key_exist) shuffledVector.push_back
                            (create_new_item(it.second.back().second, it
                                    .second.back().first));

                // remove pair from vector
                pthreadToContainer_Map[it.first].erase
                        (pthreadToContainer_Map[it.first].begin() + it.second.size());


                int res = pthread_mutex_unlock(&(mutex_map[it.first]));
                if(res != 0)framework_function_fail(pthread_mutex_unlock_fail);

                // decrement semaphore
                int sem_res = sem_wait(&shuffle_sem);
                if(sem_res != 0)framework_function_fail(sem_wait_fail);
                res = pthread_mutex_lock(&(mutex_map[it.first]));
                if(res != 0)framework_function_fail(pthread_mutex_lock_fail);
            }
            int res = pthread_mutex_unlock(&(mutex_map[it.first]));
            if(res != 0)framework_function_fail(pthread_mutex_unlock_fail);
        }
        //Gets semaphore value
        int res = sem_getvalue(&shuffle_sem, &sem_val);
        if (res != 0)framework_function_fail(sem_getvalue_fail);
        // lock finished_Map_Threads_mutex for while loop
        res  = pthread_mutex_lock(&finished_Map_Threads_mutex);
        if(res != 0)framework_function_fail(pthread_mutex_lock_fail);
    }
    // unlock finished_Map_Threads_mutex
    res  = pthread_mutex_unlock(&finished_Map_Threads_mutex);
    if(res != 0)framework_function_fail(pthread_mutex_lock_fail);
    // change the next pair to read value
    re_initialize_next_pair_to_read();

    // destroy the shuffle_sem semaphore
    res = sem_destroy(&shuffle_sem);
    if(res != 0)framework_function_fail(sem_destroy_fail);
    log_file_message(finish_threadTypeShuffle + get_cur_time()+"\n");
    pthread_exit(NULL);
}


/**
 * In this Function the ExecMap threads will run the Map operation in the
 * MapReduceFramework
 * @return nothing
 */
void *ExecMapFunc(void*)
{
    //locking // to pthreadToContainer_mutex
    int res  = pthread_mutex_lock(&pthreadToContainer_Map_mutex);
    if(res != 0)framework_function_fail(pthread_mutex_lock_fail);

    //unlocking pthreadToContainer_Map_mutex
    res  = pthread_mutex_unlock(&pthreadToContainer_Map_mutex);
    if(res != 0)framework_function_fail(pthread_mutex_unlock_fail);
    while (true)
    {
        // locking nextValue_mutex
        int res  = pthread_mutex_lock(&nextValue_mutex);
        if(res != 0)framework_function_fail(pthread_mutex_lock_fail);
        //if there is nothing left to read then thread exit
        if(next_pair_to_read >= input_vec.size())
        {
            // unlocking nextValue_mutex
            res  = pthread_mutex_unlock(&nextValue_mutex);
            if(res != 0)framework_function_fail(pthread_mutex_unlock_fail);
            break;
        }
        // selecting a chunk of pairs to work on
        unsign_l current_chunk_size = set_chunk_size(input_vec.size());
        unsign_l begin = next_pair_to_read;
        unsign_l end = next_pair_to_read + current_chunk_size;
        next_pair_to_read += current_chunk_size;
        //unlocking nextValue_mutex
        res  = pthread_mutex_unlock(&nextValue_mutex);
        if(res != 0)framework_function_fail(pthread_mutex_unlock_fail);
        for (unsign_l i = begin; i < end; ++i)
        {
            // Reading the pairs in the input vector one-by-one to map
            mapReduceBase->Map(input_vec[i].first, input_vec[i].second);
        }
    }
    log_file_message(finish_threadTypeMap + get_cur_time()+"\n");
    pthread_exit(NULL);
}

/**
 * In this Function the ExecReduce threads will run the Reduce operation in the
 * MapReduceFramework
 * @return nothing
 */
void *ExecReduceFunc(void*)
{
    //locking mutex
    int res  = pthread_mutex_lock(&pthreadToContainer_Reduce_mutex);
    if(res != 0) framework_function_fail(pthread_mutex_lock_fail);

    //unlocking mutex
    res  = pthread_mutex_unlock(&pthreadToContainer_Reduce_mutex);
    if(res != 0) framework_function_fail(pthread_mutex_unlock_fail);

    while (true)
    {
        //locking mutex
        int res  = pthread_mutex_lock(&nextValue_mutex);
        if(res != 0)framework_function_fail(pthread_mutex_lock_fail);

        //if there is nothing left to read then thread exit
        if(next_pair_to_read >= shuffledVector.size())
        {
            //unlocking mutex
            res  = pthread_mutex_unlock(&nextValue_mutex);
            if(res != 0)framework_function_fail(pthread_mutex_unlock_fail);
            break;
        }
        // selecting a chunk of pairs to work on
        unsign_l current_chunk_size = set_chunk_size(shuffledVector.size());
        unsign_l begin = next_pair_to_read;
        unsign_l end = next_pair_to_read + current_chunk_size;
        next_pair_to_read += current_chunk_size;

        //unlocking mutex
        res  = pthread_mutex_unlock(&nextValue_mutex);
        if(res != 0)framework_function_fail(pthread_mutex_unlock_fail);
        for (unsign_l i = begin; i < end; ++i)
        {
            // Reading the pairs in the input vector one-by-one to reduce
            mapReduceBase->Reduce(shuffledVector[i].first, shuffledVector[i].second);
        }
    }
    log_file_message(finish_threadTypeReduce + get_cur_time()+"\n");
    pthread_exit(NULL);
}

/////////////////// The MapReduceFramework Functions //////////////////////////


/**
 *
 * @param multiThreadLevel
 */
void map_threads_creation_and_run(int multiThreadLevel)
{
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        // Threads creation and map running
        pthread_t ExecMap;
        int thread_res = pthread_create(&ExecMap, NULL, ExecMapFunc, NULL);
        if(thread_res != 0) framework_function_fail(pthread_create_fail);
        pthread_mutex_t container_mutex = PTHREAD_MUTEX_INITIALIZER;
        mutex_map[ExecMap] = container_mutex;
        multiThreadLevel_threads_Map.push_back(ExecMap);
        pthreadToContainer_Map[ExecMap];
        log_file_message(create_threadTypeMap + get_cur_time()+"\n");
    }
}

void reduce_threads_creation_and_run(int multiThreadLevel)
{
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_t ExecReduce;
        int reduce_res = pthread_create(&ExecReduce, NULL, ExecReduceFunc,NULL);
        if(reduce_res != 0) framework_function_fail(pthread_create_fail);
        multiThreadLevel_threads_Reduce.push_back(ExecReduce);
        pthreadToContainer_Reduce[ExecReduce];
        log_file_message(create_threadTypeReduce + get_cur_time()+"\n");
    }
}

/**
 * This function runs and manages the Map-Reduce Framework.
 * @param mapReduce A reference to a MapReduceBase
 * @param itemsVec An IN_ITEMS_VEC(contains all input data)
 * @param multiThreadLevel An integer (representing number of threads)
 * @param autoDeleteV2K2 A boolean - true means the framework need to release
 * resources otherwise not.
 * @return An OUT_ITEMS_VEC holds the MapReduce framework output
 */
OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce,
                                    IN_ITEMS_VEC&itemsVec,
                                    int multiThreadLevel,
                                    bool autoDeleteV2K2)
{
    //--- First initialize v2k2 auto delete global variable and mapReduceBase---
    deleteV2K2 = autoDeleteV2K2;
    mapReduceBase = &mapReduce;

    // ----Second creates and writes to log file and starts timer----
    create_log_file();
    double total_time = 0; // Map&Shuffle measure time
    //start TIME
    if (gettimeofday(&start_time, NULL) == TIME_MEASURE_FAIL)
    {
        framework_function_fail(gettimeofday_fail);
    };
    string num = to_string(multiThreadLevel);
    log_file_message(start_MapReduceFramwork1 + num + start_MapReduceFramwork2);

    // initialize semaphore for shuffle
    int sem_res = sem_init(&shuffle_sem, 0, 0);
    if (sem_res != 0) framework_function_fail(sem_init_fail);

    //LOCKING pthreadToContainer mutex
    int res = pthread_mutex_lock(&pthreadToContainer_Map_mutex);
    if(res != 0) framework_function_fail(pthread_mutex_lock_fail);


    //-----------start MAPPING---------
    input_vec = itemsVec;
    map_threads_creation_and_run(multiThreadLevel);

    // if pthreadToContainer is initialized unlock pthreadToContainer mutex
    if (pthreadToContainer_Map.size() == (unsign_i)multiThreadLevel)
    {//UNLOCKING pthreadToContainer mutex
        int res_map = pthread_mutex_unlock(&pthreadToContainer_Map_mutex);
        if(res_map != 0) framework_function_fail(pthread_mutex_unlock_fail);
    }

    // ----------------- SHUFFLE -----------
    pthread_t shuffleThread;
    int thread_res = pthread_create(&shuffleThread, NULL, shuffle, NULL);
    log_file_message(create_threadTypeShuffle + get_cur_time()+"\n");
    if(thread_res != 0) framework_function_fail(pthread_create_fail);
    for (int j = 0; j < multiThreadLevel; ++j) // join the ExecMap threads
    {
        int res_join1 = pthread_join(multiThreadLevel_threads_Map[j], NULL);
        if (res_join1 != 0) framework_function_fail(pthread_join_fail);
    }
    //after join threads finished mapping
    int res_fin  = pthread_mutex_lock(&finished_Map_Threads_mutex);
    if(res_fin != 0) framework_function_fail(pthread_mutex_lock_fail);
    finishedMapThreads = true; // change finished map threads flag
    res_fin  = pthread_mutex_unlock(&finished_Map_Threads_mutex);
    if(res_fin != 0) framework_function_fail(pthread_mutex_unlock_fail);
    // join the Shuffle thread
    int res_join2 = pthread_join(shuffleThread, NULL);
    if (res_join2 != 0) framework_function_fail(pthread_join_fail);
    //end TIME
    if (gettimeofday(&end_time, NULL) == TIME_MEASURE_FAIL)
    {
        framework_function_fail(gettimeofday_fail);
    }
    total_time = ((end_time.tv_sec - start_time.tv_sec) * SEC_TO_NANO_CONST +
                  (end_time.tv_usec - start_time.tv_usec) * MICRO_TO_NANO_CONST);
    log_file_message(time_for_Map_and_shuffle + to_string(total_time)
                     + time_format);


    //--------------------REDUCE-----------------
    //start TIME
    // Map&Shuffle measure time
    if (gettimeofday(&start_time, NULL) == TIME_MEASURE_FAIL)
    {
        framework_function_fail(gettimeofday_fail);
    };

    //LOCKING pthreadToContainer reduce mutex
    res = pthread_mutex_lock(&pthreadToContainer_Reduce_mutex);
    if(res != 0) framework_function_fail(pthread_mutex_lock_fail);

    //creates reduce pthreads
    reduce_threads_creation_and_run(multiThreadLevel);

    //UNLOCKING pthreadToContainer reduce mutex
    if (pthreadToContainer_Reduce.size() >= (unsign_i)multiThreadLevel)
    {
        //unlocking mutex
        int res_red = pthread_mutex_unlock(&pthreadToContainer_Reduce_mutex);
        if(res_red != 0) framework_function_fail(pthread_mutex_unlock_fail);
    }
    //join the ExecMap threads
    for (int k = 0; k < multiThreadLevel; ++k)
    {
        int res_join3 = pthread_join(multiThreadLevel_threads_Reduce[k], NULL);
        if (res_join3 != 0) framework_function_fail(pthread_join_fail);
    }
    //end TIME
    if (gettimeofday(&end_time, NULL) == TIME_MEASURE_FAIL)
    {
        framework_function_fail(gettimeofday_fail);
    }
    total_time = ((end_time.tv_sec - start_time.tv_sec) * SEC_TO_NANO_CONST +
                  (end_time.tv_usec - start_time.tv_usec) * MICRO_TO_NANO_CONST);
    log_file_message(time_for_Reduce + to_string(total_time) + time_format);
    log_file_message(finish_MapReduceFramwork);
    closing_log_file();

    //---------------SORT and return output---------
    std::sort(output_vector.begin(), output_vector.end(), sort_pred());
    //release V2/K2 resources
    if(deleteV2K2) release_V2_K2_resources();
    //kill mutexes
    release_mutex_resources();
    return output_vector;
}

/**
 * This function adds a new pair of <k2,v2> to the framework's internal data
 * structure.
 * @param key pointer to a k2Base object to add
 * @param val pointer to a v2Base object to add
 */
void Emit2 (k2Base* key, v2Base* val)
{
    // lock current thread container mutex before adding
    int res_mutex = pthread_mutex_lock(&mutex_map[pthread_self()]);
    if(res_mutex != 0) framework_function_fail(pthread_mutex_lock_fail);

    //add new pair
    mapped_item new_pair = pair<k2Base*, v2Base*>(key, val);
    pthreadToContainer_Map[pthread_self()].push_back(new_pair);
    //Increment semaphore
    int sem_res = sem_post(&shuffle_sem);
    if(sem_res != 0) framework_function_fail(sem_post_fail);

    // unlock current thread container mutex after adding
    res_mutex = pthread_mutex_unlock(&mutex_map[pthread_self()]);
    if(res_mutex != 0) framework_function_fail(pthread_mutex_unlock_fail);
}

/**
 * This function adds a pair of<k3,v3> to the final output vector
 * @param key pointer to a k3Base object to add
 * @param val pointer to a v3Base object to add
 */
void Emit3 (k3Base* key, v3Base* val)
{
    // lock emit3 mutex before adding
    int res = pthread_mutex_lock(&emit3_insert);
    if(res != 0) framework_function_fail(pthread_mutex_lock_fail);
    // add new pair to output_vector
    output_vector.push_back(pair<k3Base*, v3Base*>(key, val));
    res = pthread_mutex_unlock(&emit3_insert);
    // unlock emit3 mutex before adding
    if(res != 0) framework_function_fail(pthread_mutex_unlock_fail);
}

