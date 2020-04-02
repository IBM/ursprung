project(prov_consumer)

# source dependencies
file(GLOB util-deps ../util/*.cpp)

add_executable(prov-consumer prov-consumer.cpp ${util-deps})

# required on MacOS as it is not searched by default
target_include_directories(prov-consumer PUBLIC /usr/local/include ../util)
target_link_directories(prov-consumer PUBLIC /usr/local/lib)
target_link_libraries(prov-consumer yaml rdkafka)
