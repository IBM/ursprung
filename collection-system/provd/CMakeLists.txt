# source dependencies
file(GLOB provd-deps *.cpp)
file(GLOB util-deps ../util/*.cpp)

add_executable(provd provd.cpp ${provd-deps} ${util-deps})

if (Linux)
target_include_directories(provd PUBLIC /usr/local/include ../util ../lib/ptrace_do/)
else ()
target_include_directories(provd PUBLIC /usr/local/include ../util)
endif (Linux)

if (Linux)
target_link_directories(provd PUBLIC /usr/local/lib ptrace_do)
else ()
target_link_directories(provd PUBLIC /usr/local/lib)
endif (Linux)
