set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/udns)
set(UDNS_INCLUDE_DIR ${LIBRARY_DIR})
add_definitions(-DHAVE_CONFIG_H)

add_custom_target(
    udns_config
    COMMAND ${CMAKE_COMMAND} -E env CFLAGS="-Wno-int-conversion;-Wno-implicit-function-declaration"
    ${LIBRARY_DIR}/configure
    WORKING_DIRECTORY ${LIBRARY_DIR})

file(GLOB UDNS_DIR_SRC "${LIBRARY_DIR}/*.c")

add_library(udns STATIC ${UDNS_DIR_SRC})

target_include_directories(udns PRIVATE ${LIBRARY_DIR})

target_compile_options(udns PRIVATE -Wno-builtin-macro-redefined)
