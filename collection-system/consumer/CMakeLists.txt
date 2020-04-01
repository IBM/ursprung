cmake_minimum_required(VERSION 3.10)

project(prov_consumer)

# source dependencies
file(GLOB util-deps ../util/*.cpp)

add_executable(prov_consumer prov_consumer.cpp ${util-deps})

# required on MacOS as it is not searched by default
target_include_directories(prov_consumer PUBLIC /usr/local/include ../util)
target_link_directories(prov_consumer PUBLIC /usr/local/lib)
target_link_libraries(prov_consumer yaml rdkafka)