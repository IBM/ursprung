cmake_minimum_required(VERSION 3.13)

project(ursprung)

# set c++ standard
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# logging level
if ( INFO )
	add_compile_definitions(INFO=1)
endif()
if ( DEBUG )
	add_compile_definitions(INFO=1)
	add_compile_definitions(DEBUG=1)
endif()
if ( PERF )
	add_compile_definitions(PERF=1)
endif()

# set include directories
include_directories(util)
set(CMAKE_REQUIRED_INCLUDES "/usr/local/include")

add_subdirectory(provd)
add_subdirectory(consumer)

# testing
if ( BUILD_TESTS )
  enable_testing()
  add_subdirectory(test)
  add_subdirectory(lib/googletest)
endif()
