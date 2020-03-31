cmake_minimum_required(VERSION 3.10)

project(ursprung)

# set c++ standard
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# set include directories
set(CMAKE_REQUIRED_INCLUDES "/usr/local/include")

add_subdirectory(provd)
add_subdirectory(consumer)
