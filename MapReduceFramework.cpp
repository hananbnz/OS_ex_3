
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

// -------------------------------- Constants ---------------------------------

/**
 * CHUNK_SIZE 10
 * @brief  the value of a chunk size
 */
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
 *
 */
bool deleteV2K2 = false;

/**
 *
 */
bool finishedMapThreads = false;

/**
 * Represents an index of the next pair to read
 */
unsign_l next_pair_to_read = 0;

ofstream outputFile;

char buf[LOG_BUF_SIZE];

// -------------------------- Mutexes / Semaphores ----------------------------

pthread_mutex_t pthreadToContainer_Map_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t pthreadToContainer_Reduce_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t nextValue_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t logFile_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t finished_Map_Threads_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t check_time_mutex = PTHREAD_MUTEX_INITIALIZER;
sem_t shuffle_sem;

//lock/unlock result varaiables
//int res;

int sem_val;


// -------------------------- SHARED DATA STRUCTURES  ------------------------


typedef std::pair<k2Base*, v2Base*> mapped_item;
typedef std::vector<mapped_item> mapped_vector;

//typedef vector<v2Base*> shuffled_vec;//TODO combine type from MapReduceFrameworkto
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
//    int res  = pthread_mutex_lock(&logFile_mutex);
//    if(res != 0)
//    {
//        framework_function_fail(pthread_mutex_lock_fail);
//    }
    char* r_buf;
    r_buf = getcwd(buf, LOG_BUF_SIZE);
    string file_to_open= (string)r_buf + Log_file_name;
    outputFile.open(file_to_open);
    if(!outputFile.is_open())
    {
        fprintf(stderr, "system error: %s\n", "ERROR opening Log File");
    }
//    res  = pthread_mutex_unlock(&logFile_mutex);
//    if(res != 0)
//    {
//        framework_function_fail(pthread_mutex_unlock_fail);
//    }
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

///**
// * This function gets a key pointer(K2Base*) and a pthread id and removes the
// * matching key pair from the pthread container
// * @param newKey K2Base pointer
// * @param id pthread_t represents the thread which from his container the
// * function removes the pair.
// */
//void remove_pair(k2Base* newKey, pthread_t id)
//{
//    for (int j = 0; j < pthreadToContainer_Map[id].size(); ++j)
//    {
//        if(!(*newKey < *(pthreadToContainer_Map[id][j].first)) &&
//                !(*(pthreadToContainer_Map[id][j].first) < *newKey))
//        {
//            pair<k2Base*, v2Base*> pair_to_delete =
//                    pthreadToContainer_Map[id][j];
//            pthreadToContainer_Map[id].erase(pthreadToContainer_Map[id].begin()
//                                             + j);
//            if(deleteV2K2)
//            {
////                pair_to_delete.first = nullptr;
////                pair_to_delete.second = nullptr;
////                delete pair_to_delete.first;
////                delete pair_to_delete.second;
//            }
//            break;
//        }
//    }
//}

shuffled_item create_new_item(v2Base* newVal, k2Base* newKey)
{
    V2_VEC new_vec;
    new_vec.push_back(newVal);
    shuffled_item new_item = pair<k2Base*, V2_VEC>(newKey, new_vec);
    return new_item;
}

void *shuffle(void*)
{
    // Gets semaphore value
    int res = sem_getvalue(&shuffle_sem, &sem_val);
    if (res != 0)
    {
        framework_function_fail(sem_getvalue_fail);
    }
    // while semaphore value>0 and threads left shuffle keeps running
    res  = pthread_mutex_lock(&finished_Map_Threads_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_lock_fail);
    }
    while(sem_val > 0 || !finishedMapThreads)
    {
        res  = pthread_mutex_unlock(&finished_Map_Threads_mutex);
        if(res != 0)
        {
            framework_function_fail(pthread_mutex_lock_fail);
        }
        for (auto &it :pthreadToContainer_Map)
        {
            res = pthread_mutex_lock(&(mutex_map[it.first]));
            if(res != 0)
            {
                framework_function_fail(pthread_mutex_lock_fail);
            }
            while(!(it.second.empty())) //while container not empty
            {

//                k2Base* newKey = it.second.back().first;
//                v2Base* newVal = it.second.back().second;
                bool is_key_exist = false;
                // search for the key in container

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
                // checks if key exists and if not found creates a new key
                if(!is_key_exist) shuffledVector.push_back
                            (create_new_item(it.second.back().second, it.second.back().first));
                // remove pair from vector

                garbage_collector.push_back(it.second.back());
                pthreadToContainer_Map[it.first].erase(pthreadToContainer_Map[it.first].begin() + it.second.size());
                int res = pthread_mutex_unlock(&(mutex_map[it.first]));
                if(res != 0)
                {
                    framework_function_fail(pthread_mutex_unlock_fail);
                }
                // decrement semaphore
                int sem_res = sem_wait(&shuffle_sem);
                if(sem_res != 0)
                {
                    framework_function_fail(sem_wait_fail);
                }
                res = pthread_mutex_lock(&(mutex_map[it.first]));
                if(res != 0)
                {
                    framework_function_fail(pthread_mutex_lock_fail);
                }
            }
            int res = pthread_mutex_unlock(&(mutex_map[it.first]));
            if(res != 0)
            {
                framework_function_fail(pthread_mutex_unlock_fail);
            }
        }
        //Gets semaphore value
        int res = sem_getvalue(&shuffle_sem, &sem_val);
        if (res != 0)
        {
            framework_function_fail(sem_getvalue_fail);
        }
        res  = pthread_mutex_lock(&finished_Map_Threads_mutex);
        if(res != 0)
        {
            framework_function_fail(pthread_mutex_lock_fail);
        }
    }
    res  = pthread_mutex_unlock(&finished_Map_Threads_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_lock_fail);
    }
    res  = pthread_mutex_lock(&nextValue_mutex);
    if(res !=0)
    {
        framework_function_fail(pthread_mutex_lock_fail);
    }
    next_pair_to_read = 0;
    res  = pthread_mutex_unlock(&nextValue_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_unlock_fail);
    }
    res = sem_destroy(&shuffle_sem);
    if(res != 0)
    {
        framework_function_fail(sem_destroy_fail);
    }
    log_file_message(finish_threadTypeShuffle + get_cur_time()+"\n");
    pthread_exit(NULL);
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


void *ExecMapFunc(void*)
{
    //locking mutex
    int res  = pthread_mutex_lock(&pthreadToContainer_Map_mutex);////TODO change
/// to pthreadToContainer_mutex
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_lock_fail);
    }
    //unlocking mutex
    res  = pthread_mutex_unlock(&pthreadToContainer_Map_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_unlock_fail);
    }
    // in the func will send one-by-one pairs to map
    while (true)
    {
        int res  = pthread_mutex_lock(&nextValue_mutex);
        if(res != 0)
        {
            framework_function_fail(pthread_mutex_lock_fail);
        }
        //if there is nothing left to read then thread exit
        if(next_pair_to_read >= input_vec.size())
        {
            res  = pthread_mutex_unlock(&nextValue_mutex);
            if(res != 0)
            {
                framework_function_fail(pthread_mutex_unlock_fail);
            }
            break;
        }

//        //locking next value mutex
//        res  = pthread_mutex_lock(&nextValue_mutex);
//        if(res != 0)
//        {
//            framework_function_fail(pthread_mutex_lock_fail);
//        }

        //get chunk size using outer function
        unsign_l current_chunk_size = set_chunk_size(input_vec.size());
        unsign_l begin = next_pair_to_read;
        unsign_l end = next_pair_to_read + current_chunk_size;
        next_pair_to_read += current_chunk_size;

        //unlocking next value mutex
        res  = pthread_mutex_unlock(&nextValue_mutex);
        if(res != 0)
        {
            framework_function_fail(pthread_mutex_unlock_fail);
        }
        for (unsign_l i = begin; i < end; ++i)
        {
            // Reading the pairs in the input vector one-by-one to map
            mapReduceBase->Map(input_vec[i].first, input_vec[i].second);
        }
    }
    log_file_message(finish_threadTypeMap + get_cur_time()+"\n");
    pthread_exit(NULL);
}


void *ExecReduceFunc(void*)
{
    //locking mutex
    int res  = pthread_mutex_lock(&pthreadToContainer_Reduce_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_lock_fail);
    }
    //unlocking mutex
    res  = pthread_mutex_unlock(&pthreadToContainer_Reduce_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_unlock_fail);
    }
    while (true)
    {
        int res  = pthread_mutex_lock(&nextValue_mutex);
        if(res != 0)
        {
            framework_function_fail(pthread_mutex_lock_fail);
        }
        //if there is nothing left to read then thread exit
        if(next_pair_to_read >= shuffledVector.size())
        {
            //unlocking mutex
            res  = pthread_mutex_unlock(&nextValue_mutex);
            if(res != 0)
            {
                framework_function_fail(pthread_mutex_unlock_fail);
            }
            break;
        }

        //locking mutex
//        res  = pthread_mutex_lock(&nextValue_mutex);
//        if(res != 0)
//        {
//            framework_function_fail(pthread_mutex_lock_fail);
//        }

        unsign_l current_chunk_size = set_chunk_size(shuffledVector.size());
        unsign_l begin = next_pair_to_read;
        unsign_l end = next_pair_to_read + current_chunk_size;
        next_pair_to_read += current_chunk_size;

        //unlocking mutex
        res  = pthread_mutex_unlock(&nextValue_mutex);
        if(res != 0)
        {
            framework_function_fail(pthread_mutex_unlock_fail);
        }
        for (unsign_l i = begin; i < end; ++i)
        {
            // Reading the pairs in the input vector one-by-one to reduce
            mapReduceBase->Reduce(shuffledVector[i].first, shuffledVector[i].second);
        }
    }
    log_file_message(finish_threadTypeReduce + get_cur_time()+"\n");
    pthread_exit(NULL);
}

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
    if (sem_res != 0)
    {
        framework_function_fail(sem_init_fail);
    }

    //LOCKING pthreadToContainer mutex
    int res = pthread_mutex_lock(&pthreadToContainer_Map_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_lock_fail);
    }

    //----Third start MAPPING-----
    input_vec = itemsVec;
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_t newExecMap;
        int thread_res = pthread_create(&newExecMap, NULL, ExecMapFunc, NULL);
        if(thread_res != 0)
        {
            framework_function_fail(pthread_create_fail);
        }
        pthread_mutex_t container_mutex = PTHREAD_MUTEX_INITIALIZER;
        mutex_map[newExecMap] = container_mutex;
        multiThreadLevel_threads_Map.push_back(newExecMap);
        pthreadToContainer_Map[newExecMap];
        log_file_message(create_threadTypeMap + get_cur_time()+"\n");
    }

    // if pthreadToContainer is initialized unlock pthreadToContainer mutex
    if (pthreadToContainer_Map.size() == (unsign_i)multiThreadLevel)
    {//UNLOCKING pthreadToContainer mutex
        int res = pthread_mutex_unlock(&pthreadToContainer_Map_mutex);
        if(res != 0)
        {
            framework_function_fail(pthread_mutex_unlock_fail);
        }
    }

    // -----Forth SHUFFLE-----
    pthread_t shuffleThread;
    int thread_res = pthread_create(&shuffleThread, NULL, shuffle, NULL);
    log_file_message(create_threadTypeShuffle + get_cur_time()+"\n");
    if(thread_res != 0)
    {
        framework_function_fail(pthread_create_fail);
    }
    for (int j = 0; j < multiThreadLevel; ++j) // join the ExecMap threads
    {
        int res = pthread_join(multiThreadLevel_threads_Map[j], NULL);
        if (res != 0)
        {
            framework_function_fail(pthread_join_fail);
        }
    }
    res  = pthread_mutex_lock(&finished_Map_Threads_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_lock_fail);
    }
    finishedMapThreads = true;
    res  = pthread_mutex_unlock(&finished_Map_Threads_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_unlock_fail);
    }
    res = pthread_join(shuffleThread, NULL); // join the Shuffle thread
    if (res != 0)
    {
        framework_function_fail(pthread_join_fail);
    }
    //end TIME
    if (gettimeofday(&end_time, NULL) == TIME_MEASURE_FAIL)
    {
        framework_function_fail(gettimeofday_fail);
    }
    total_time = ((end_time.tv_sec - start_time.tv_sec) * SEC_TO_NANO_CONST +
                  (end_time.tv_usec - start_time.tv_usec) * MICRO_TO_NANO_CONST);
    log_file_message(time_for_Map_and_shuffle + to_string(total_time)
                     + time_format);

    //------Fifth REDUCE-----
    //start TIME
    // Map&Shuffle measure time
    if (gettimeofday(&start_time, NULL) == TIME_MEASURE_FAIL)
    {
        framework_function_fail(gettimeofday_fail);
    };

    //LOCKING pthreadToContainer reduce mutex
    res = pthread_mutex_lock(&pthreadToContainer_Reduce_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_lock_fail);
    }

    //creates reduce pthreads
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_t ExecReduce;
        int reduce_res = pthread_create(&ExecReduce, NULL, ExecReduceFunc,NULL);
        if(reduce_res != 0)
        {
            framework_function_fail(pthread_create_fail);
        }
        multiThreadLevel_threads_Reduce.push_back(ExecReduce);
        pthreadToContainer_Reduce[ExecReduce];
        log_file_message(create_threadTypeReduce + get_cur_time()+"\n");
    }

    //UNLOCKING pthreadToContainer reduce mutex
    if (pthreadToContainer_Reduce.size() >= (unsign_i)multiThreadLevel)
    {
        //unlocking mutex
        int res = pthread_mutex_unlock(&pthreadToContainer_Reduce_mutex);
        if(res != 0)
        {
            framework_function_fail(pthread_mutex_unlock_fail);
        }
    }
    //join the ExecMap threads
    for (int k = 0; k < multiThreadLevel; ++k)
    {
        int res = pthread_join(multiThreadLevel_threads_Reduce[k], NULL);
        if (res != 0)
        {
            framework_function_fail(pthread_join_fail);
        }
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


    //------And last SORT and return output-------
    std::sort(output_vector.begin(), output_vector.end(), sort_pred());
    if(deleteV2K2)
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
    //kill mutex

    pthread_mutex_destroy(&pthreadToContainer_Map_mutex);

    pthread_mutex_destroy(&pthreadToContainer_Reduce_mutex);

    pthread_mutex_destroy(&nextValue_mutex);

    pthread_mutex_destroy(&logFile_mutex);

    pthread_mutex_destroy(&check_time_mutex);

    pthread_mutex_destroy(&finished_Map_Threads_mutex);

    for (auto &map: mutex_map)
    {
        pthread_mutex_destroy(&map.second);
    }
    return output_vector;

}

void Emit2 (k2Base* key, v2Base* val)
{
    int res = pthread_mutex_lock(&mutex_map[pthread_self()]);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_lock_fail);
    }

    mapped_item new_pair = pair<k2Base*, v2Base*>(key, val);
    pthreadToContainer_Map[pthread_self()].push_back(new_pair);
    //Increment semaphore
    int sem_res = sem_post(&shuffle_sem);
    if(sem_res != 0)
    {
        framework_function_fail(sem_post_fail);
    }

    res = pthread_mutex_unlock(&mutex_map[pthread_self()]);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_unlock_fail);
    }
}

void Emit3 (k3Base* key, v3Base* val)
{
    output_vector.push_back(pair<k3Base*, v3Base*>(key, val));
}


//ExecReduce
 //create a container for each thread
 //mapped_vector* newMapVec = new mapped_vector;
//    pthreadToContainer.insert(pair<pthread_t,
//            mapped_vector>(pthread_self(), newMapVec));
//    mapped_vector* newMapVec = new mapped_vector;
//    pthreadToContainer[pthread_self()];

//erase pair
//map<thread, vector<pair<k2Base*, v2Base*> >>
//it->second = vector<pair<k2Base*, v2Base*>
//                for (int j = 0; j < it->second.size(); ++j)
//                {
//                    if(!(*newKey < *(it->second[j].first)) && !(*(it->second[j].first) < *newKey))
//                    {
//
//                        pair<k2Base*, v2Base*> pair_to_delete =  it->second[j];
//                        it->second.erase(it->second.begin() + j);
//                        if(deleteV2K2)
//                        {
////                            delete pair_to_delete.first;
////                            delete pair_to_delete.second;
//                        }
//                        break;
//                    }
//                }
//                it->second.pop_back();

//emit2
// check what thread is running with self() and use the ID as a key
// add to the shared container {<key - thread ID, val- thread map output container>}
//    printf("called Emit2\n");
//    fflush(stdout);


//emit3
//create shuffle thread with the creation of execMap threads
//merge all containers with the same key
//converts list of <k2,v2> to list <k2,list(v2)>
// check what thread is running with self() and use the ID as a key
// add to the shared container {<key - thread ID, val- thread map output container>}
//    mapped_item new_pair = pair<k3Base*, v3Base*>(key, val);


/////void prepare_to_reduce()
//{
//    for( map<k2Base*, shuffled_vec>::iterator it = shuffledContainer.begin(); it != shuffledContainer.end(); ++it )
//    {
//        printf("size %d\n", it->second.size());
//        fflush(stdout);
//        shuffledVector.push_back(shuffled_item(it->first, it->second));
//    }
//}

//execMap function
//    mapped_vector newMapVec = new mapped_vector;
//    pthreadToContainer[pthread_self()]; // = newMapVec;
//    pthreadToContainer.insert(pair<pthread_t,
//            mapped_vector>(pthread_self(), newMapVec));
//    MapReduceBase& mapReduce1 = (MapReduceBase&)mapReduce;