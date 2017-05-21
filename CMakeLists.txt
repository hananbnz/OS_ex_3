cmake_minimum_required(VERSION 3.5)
project(ex_3)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")

set(SOURCE_FILES MapReduceClient.h MapReduceFramework.h MapReduceFramework.cpp
        search.cpp MapReduceClient.cpp)
add_executable(ex_3 ${SOURCE_FILES})