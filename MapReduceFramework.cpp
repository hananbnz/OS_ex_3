
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

vector<pthread_t> multiThreadLevel_threads(4,NULL);

/////////////////////// SHARED DATA STRUCTURES  //////////////////////////////



//vector<pair<k1Base*, v1Base*>> IN_ITEMS_VEC;
IN_ITEMS_VEC input_vec;
//
map<pthread_t, mapped_vector> pthreadToContainer;

//vector <pair<k2Base*, vector<v2Base*>>> shuffled_item;
vector<shuffled_item> shuffledVector;


/* comperator for the shuffled map insertion */
//bool myComp(k2Base* x, k2Base* y){return ((*x) < (*y));}
/* The output of the shuffle  */
//static map<k2Base*, shuffled_vec, bool (*) (k2Base*, k2Base*)>
//        shuffledContainer(myComp);

//map<k2Base*, shuffled_vec> shuffledContainer;



OUT_ITEMS_VEC output_vector;

MapReduceBase* mapReduceBase;


bool finishedMapThreads = false;

bool autoReleaseResources = false;

/////////////////////////Framework Error Messages////////////////////////////

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

//////////////////////////// Framework Error FUNCTIONS ////////////////////////

void framework_function_fail(string text)
{
    fprintf(stderr, "MapReduceFramework Failure: %s failed\n", text.c_str());
    exit(1);
}

//////////////////////////// LOG FILE FUNCTIONS ///////////////////////////////


// A vriable to the log file
ofstream outputFile;
void create_log_file()
{

//    outputFile.open(getcwd(".MapReduceFramwork.log"));
    //TODO general directory
//    outputFile.open("/cs/usr/hananbnz/safe/OS/ex_3/.MapReduceFramework.log");
    outputFile.open("/cs/usr/reuveny/safe/OS/ex_3/.MapReduceFramework.log");
    if(!outputFile.is_open())
    {
        fprintf(stderr, "system error: %s\n", "ERROR opening Log File");
    }
}

void log_file_message(string txt)
{
    res  = pthread_mutex_lock(&logFile_mutex);
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
        framework_function_fail(sem_getvalue_fail);
    }
    while(sem_val > 0 ||  !finishedMapThreads) // every time will check
    {
        for (auto it = pthreadToContainer.begin(); it != pthreadToContainer.end(); ++it)
        {
            //TODO iterate different on pthreadContainer
            while(!(it->second.empty()))
            {
//                printf("thread %d vector size is :%ld    \n", pthread_self(),it->second.size());
//                fflush(stdout);
                k2Base* newKey = it->second.back().first;
                v2Base* newVal = it->second.back().second;
                bool is_key_exist = false;
                for (int i = 0; i < shuffledVector.size(); ++i)
                {
                    if(!(*newKey < *(shuffledVector[i].first)) && !(*(shuffledVector[i].first) < *newKey))
                    {
                        shuffledVector[i].second.push_back(newVal);
                        is_key_exist = true;
                        break;
                    }

                }
                if(!is_key_exist)
                {
//                    shuffled_vec new_vec(1, newVal);
                    shuffled_vec new_vec;
                    new_vec.push_back(newVal);
                    shuffled_item new_item = pair<k2Base*, shuffled_vec>(newKey, new_vec);
                    shuffledVector.push_back(new_item);
                }
//                mapped_vector vector = it->second;
//                vector.pop_back();

                for (int j = 0; j < it->second.size(); ++j)
                {
                    if(!(*newKey < *(it->second[j].first)) && !(*(it->second[j].first) < *newKey))
                    {
                        it->second.erase(it->second.begin() + j);
                        break;
                    }
                }
//                it->second.pop_back();
                int sem_res = sem_wait(&shuffle_sem);
                if(sem_res != 0)
                {
                    framework_function_fail(sem_wait_fail);
                }
            }
        }
        res = sem_getvalue(&shuffle_sem, &sem_val);
        if (res != 0)
        {
            framework_function_fail(sem_getvalue_fail);
        }
    }
    finished_shuffle = true;
    next_pair_to_read = 0;
    res = sem_destroy(&shuffle_sem);
//    printf("size of continer %d\n", shuffledVector.size());
    if(res != 0)
    {
        framework_function_fail(sem_destroy_fail);
    }
    log_file_message(finish_threadTypeShuffle + get_cur_time()+"\n");
    pthread_exit(NULL);
}

unsigned long set_chunk_size(unsigned long vec_size)
{
    unsigned long current_chunk_size = CHUNK_SIZE;
    if((next_pair_to_read + current_chunk_size) > vec_size)
    {
        current_chunk_size = vec_size - next_pair_to_read;
    }
    return current_chunk_size;
}

void *ExecMapFunc(void* mapReduce)
{
//    mapped_vector newMapVec = new mapped_vector;
//    pthreadToContainer[pthread_self()]; // = newMapVec;
//    pthreadToContainer.insert(pair<pthread_t,
//            mapped_vector>(pthread_self(), newMapVec));
    // TODO the execmap func lock and unlock mutex and than map in mapReduce
//    MapReduceBase& mapReduce1 = (MapReduceBase&)mapReduce; // TODO check
    //locking mutex
    res  = pthread_mutex_lock(&pthreadToContainer_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_lock_fail);
    }
    //unlocking mutex
    res  = pthread_mutex_unlock(&pthreadToContainer_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_unlock_fail);
    }
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
        if(res != 0)
        {
            framework_function_fail(pthread_mutex_lock_fail);
        }
        unsigned long current_chunk_size = set_chunk_size(input_vec.size());
        int begin = next_pair_to_read;
        unsigned long end = next_pair_to_read + current_chunk_size;
        next_pair_to_read += current_chunk_size;
        //unlocking mutex
        res  = pthread_mutex_unlock(&nextValue_mutex);
        if(res != 0)
        {
            framework_function_fail(pthread_mutex_unlock_fail);
        }
        for (int i = begin; i < end; ++i)
        {
            // Reading the pairs in the input vector one-by-one to map
            mapReduceBase->Map(input_vec[i].first, input_vec[i].second);

        }
        // thread will take CHUNK (or the last reminder) and read
        // TODO lock mutex if here or before the loop
    }
    // TODO need variable to
    log_file_message(finish_threadTypeMap + get_cur_time()+"\n");
    pthread_exit(NULL);
}

//void prepare_to_reduce()
//{
//    for( map<k2Base*, shuffled_vec>::iterator it = shuffledContainer.begin(); it != shuffledContainer.end(); ++it )
//    {
//        printf("size %d\n", it->second.size());
//        fflush(stdout);
//        shuffledVector.push_back(shuffled_item(it->first, it->second));
//    }
//}

void *ExecReduceFunc(void* mapReduce)
{

    //locking mutex
    res  = pthread_mutex_lock(&pthreadToContainer_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_lock_fail);
    }
    //unlocking mutex
    res  = pthread_mutex_unlock(&pthreadToContainer_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_unlock_fail);
    }
    // TODO the execmap func lock and unlock mutex and than map in mapReduce
//    MapReduceBase& mapReduce1 = (MapReduceBase&)mapReduce; // TODO check
    while (true)
    {
//        printf("next_pair_to_read : %d, shuffledVector.size() : %d \n",next_pair_to_read, shuffledVector.size());
//        fflush(stdout);
        if(next_pair_to_read >= shuffledVector.size())
        {
            break;
        }
        //locking mutex
        res  = pthread_mutex_lock(&nextValue_mutex);
        if(res != 0)
        {
            framework_function_fail(pthread_mutex_lock_fail);
        }
        unsigned long current_chunk_size = set_chunk_size(shuffledVector.size());
        int begin = next_pair_to_read;
        unsigned long end = next_pair_to_read + current_chunk_size;
        next_pair_to_read += current_chunk_size;
        //unlocking mutex
        res  = pthread_mutex_unlock(&nextValue_mutex);
        if(res != 0)
        {
            framework_function_fail(pthread_mutex_unlock_fail);
        }
        for (int i = begin; i < end; ++i)
        {
            // Reading the pairs in the input vector one-by-one to reduce
            mapReduceBase->Reduce(shuffledVector[i].first, shuffledVector[i]
                    .second);
        }
    }
    // TODO need variable to
    log_file_message(finish_threadTypeReduce + get_cur_time()+"\n");
    pthread_exit(NULL);
}


OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce,
                                    IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel,
                                    bool autoDeleteV2K2)
{
    autoReleaseResources = autoDeleteV2K2;
    // First creates and writes to log file and starts timer
    create_log_file();
    // Map&Shuffle measure time
    double total_time = 0;
    if (gettimeofday(&start_time, NULL) == TIME_MEASURE_FAIL)
    {
        framework_function_fail(gettimeofday_fail);
    };
    string num = to_string(multiThreadLevel);
    log_file_message(start_MapReduceFramwork1 + num + start_MapReduceFramwork2);

    //Second initialize mapReduceBase and semaphore
    mapReduceBase = &mapReduce;
//    pthread_t multiThreadLevel_threads[multiThreadLevel];
    // initialize semaphore for shuffle
    int sem_res = sem_init(&shuffle_sem, 0, 0);
    if (sem_res != 0)
    {
        framework_function_fail(sem_init_fail);
    }
    //locking mutex
    res = pthread_mutex_lock(&pthreadToContainer_mutex);
    if(res != 0)
    {
        framework_function_fail(pthread_mutex_lock_fail);
    }

    //Third start Mapping
    input_vec = itemsVec;
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_t newExecMap;
        int thread_res = pthread_create(&newExecMap, NULL, ExecMapFunc, NULL);
        if(thread_res != 0)
        {
            framework_function_fail(pthread_create_fail);
        }
        multiThreadLevel_threads[i] = newExecMap;
        pthreadToContainer[newExecMap];
        log_file_message(create_threadTypeMap + get_cur_time()+"\n");
    }
    /**
     * create the map size multiThreadLevel each with key - thread ID, val -
     * the thread ID container
     * check about the data structure that every container is in a different
     * location. create framework internal structure
     */
    if (pthreadToContainer.size() >= multiThreadLevel)
    {
        //unlocking mutex
        res = pthread_mutex_unlock(&pthreadToContainer_mutex);
        if(res != 0)
        {
            framework_function_fail(pthread_mutex_unlock_fail);
        }
    }
    // Forth shuffle
    pthread_t shuffleThread;
    int thread_res = pthread_create(&shuffleThread, NULL, shuffle, NULL);
    log_file_message(create_threadTypeShuffle + get_cur_time()+"\n");
    if(thread_res != 0)
    {
        framework_function_fail(pthread_create_fail);
    }
    for (int j = 0; j < multiThreadLevel; ++j) // join the ExecMap threads
    {
        int res = pthread_join(multiThreadLevel_threads[j], NULL);
        if (res != 0)
        {
            framework_function_fail(pthread_join_fail);
        }
    }
    finishedMapThreads = true;
    int res = pthread_join(shuffleThread, NULL); // join the Shuffle thread
    if (res != 0)
    {
        framework_function_fail(pthread_join_fail);
    }
    if (gettimeofday(&end_time, NULL) == TIME_MEASURE_FAIL)
    {
        framework_function_fail(gettimeofday_fail);
    }
    total_time = ((end_time.tv_sec - start_time.tv_sec) * SEC_TO_NANO_CONST +
                  (end_time.tv_usec - start_time.tv_usec) * MICRO_TO_NANO_CONST);
    log_file_message(time_for_Map_and_shuffle + to_string(total_time)
                     + time_format);
    // Map&Shuffle measure time
    total_time = 0;
    if (gettimeofday(&start_time, NULL) == TIME_MEASURE_FAIL)
    {
        framework_function_fail(gettimeofday_fail);
    };

//    printf("vector container size %d\n", shuffledVector.size());

    //Fifth Reduce part
//    prepare_to_reduce();
    //execReduce call
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_t ExecReduce;
        int reduce_res = pthread_create(&ExecReduce, NULL, ExecReduceFunc,NULL);
        if(reduce_res != 0)
        {
            framework_function_fail(pthread_create_fail);
        }
        multiThreadLevel_threads[i] = ExecReduce;
        pthreadToContainer[ExecReduce];
        log_file_message(create_threadTypeReduce + get_cur_time()+"\n");
    }
    for (int k = 0; k < multiThreadLevel; ++k)
    {
        int res = pthread_join(multiThreadLevel_threads[k], NULL);
        if (res != 0)
        {
            framework_function_fail(pthread_join_fail);
        }
    }

    if (gettimeofday(&end_time, NULL) == TIME_MEASURE_FAIL)
    {
        framework_function_fail(gettimeofday_fail);
    }
    total_time = ((end_time.tv_sec - start_time.tv_sec) * SEC_TO_NANO_CONST +
                  (end_time.tv_usec - start_time.tv_usec) * MICRO_TO_NANO_CONST);
    log_file_message(time_for_Reduce + to_string(total_time) + time_format);
    closing_log_file();


//    input_vec.clear();
//
//    pthreadToContainer.clear();
//
//    shuffledVector.clear();

    return output_vector;

}



void Emit2 (k2Base* key, v2Base* val)
{
    // check what thread is running with self() and use the ID as a key
    // add to the shared container {<key - thread ID, val- thread map output container>}
//    printf("called Emit2\n");
//    fflush(stdout);
    mapped_item new_pair = pair<k2Base*, v2Base*>(key, val);
    pthreadToContainer[pthread_self()].push_back(new_pair);
//    printf("%d\n",pthread_self());
//    fflush(stdout);
    int sem_res = sem_post(&shuffle_sem);
    if(sem_res != 0)
    {
        framework_function_fail(sem_post_fail);
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


//ExecReduce
 //create a container for each thread
 //mapped_vector* newMapVec = new mapped_vector;
//    pthreadToContainer.insert(pair<pthread_t,
//            mapped_vector>(pthread_self(), newMapVec));
//    mapped_vector* newMapVec = new mapped_vector;
//    pthreadToContainer[pthread_self()];
