# source dependencies
file(GLOB consumer-deps *.cpp)
file(GLOB util-deps ../util/*.cpp)
file(GLOB io-deps ../io/*.cpp)
file(GLOB sql-deps ../sql/*.cpp)
file(GLOB event-deps ../event/*.cpp)
file(GLOB rules-deps ../rules/*.cpp)
file(GLOB provd-deps ../provd/provd-client.cpp)

add_executable(prov-consumer prov-consumer.cpp ${consumer-deps} ${util-deps}
  ${io-deps} ${sql-deps} ${event-deps} ${rules-deps} ${provd-deps})

# required on MacOS as it is not searched by default
target_include_directories(prov-consumer PUBLIC /usr/local/include ../util ../io ../sql ../event ../rules
  ../provd/ ../lib/c-hglib/hglib)
# include openssl/md5.h on MacOS (assuming it has been installed through homebrew)
if (APPLE)
  target_include_directories(prov-consumer PUBLIC /usr/local/Cellar/openssl@1.1/1.1.1d/include)
endif ()

target_link_directories(prov-consumer PUBLIC /usr/local/lib ${PROJECT_SOURCE_DIR}/lib/c-hglib)
if (APPLE)
  target_link_directories(prov-consumer PUBLIC /usr/local/Cellar/openssl@1.1/1.1.1d/lib)
endif ()

target_link_libraries(prov-consumer odbc boost_regex hg crypto pthread rdkafka++)
if (UNIX AND NOT APPLE)
	target_link_libraries(prov-consumer auparse audit)
endif()
