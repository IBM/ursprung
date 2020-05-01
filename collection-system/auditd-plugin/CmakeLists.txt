set(AUDITD_PLUGIN_BIN auditd-plugin)

file(GLOB_RECURSE utils LIST_DIRECTORIES false ../util/*.cpp)
file(GLOB_RECURSE io LIST_DIRECTORIES false ../io/*.cpp)
file(GLOB_RECURSE event LIST_DIRECTORIES false ../event/*.cpp)
file(GLOB_RECURSE os-model LIST_DIRECTORIES false ../os-model/*.cpp)

set(SOURCES ${utils} ${io} ${event} ${os-model})

add_executable(${AUDITD_PLUGIN_BIN} ${utils} ${io} ${event} ${os-model})

target_include_directories(${AUDITD_PLUGIN_BIN} PUBLIC /usr/local/include ../util ../io ../event ../os-model)

target_link_directories(${AUDITD_PLUGIN_BIN} PUBLIC /usr/local/lib)
target_link_libraries(${AUDITD_PLUGIN_BIN} PUBLIC rdkafka++)
