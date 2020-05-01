if(UNIX AND NOT APPLE)
	set(LINUX TRUE)
endif()

# source dependencies
file(GLOB provd-deps *.cpp)
file(GLOB util-deps ../util/*.cpp)

add_executable(provd provd.cpp ${provd-deps} ${util-deps})

if (LINUX)
target_include_directories(provd PUBLIC /usr/local/include ../util ../lib/ptrace_do/)
else ()
target_include_directories(provd PUBLIC /usr/local/include ../util)
endif ()

target_link_directories(provd PUBLIC /usr/local/lib)
if (LINUX)
target_link_libraries(provd PUBLIC pthread ptrace_do)
else ()
target_link_libraries(provd PUBLIC pthread)
endif ()
