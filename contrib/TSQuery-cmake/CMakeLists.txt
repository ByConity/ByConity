set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/TSQuery")

#generate with flex and bison
execute_process(COMMAND flex -i TSQueryScanner.ll WORKING_DIRECTORY ${LIBRARY_DIR})
execute_process(COMMAND bison -d -v TSQueryParser.yy WORKING_DIRECTORY ${LIBRARY_DIR})

set (SRCS 
    "${LIBRARY_DIR}/TSQueryParserDriver.cpp" 
    "${LIBRARY_DIR}/TSQueryExpression.cpp"
    "${LIBRARY_DIR}/TSQueryParser.tab.cc" 
    "${LIBRARY_DIR}/lex.TSQueryScanner.cc"
)

add_library(tsquery ${SRCS})

target_include_directories(tsquery PUBLIC "${LIBRARY_DIR}")