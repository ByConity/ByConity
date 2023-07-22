if(NOT ENABLE_PYUDF)
    return()
endif()

find_package(Python3 REQUIRED COMPONENTS Development NumPy)
if (NOT Python3_FOUND)
    message(FATAL_ERROR "Couldn't find Python and Numpy installed which is required by Python UDF, you can pass -DENABLE_PYUDF=0 to cmake to skip it.")
endif()

add_executable(pysrv pysrv.cpp udfstring.cpp pyerror.cpp pycolumn.cpp pyrunner.cpp)
target_link_libraries(pysrv PRIVATE srvstub ${Python3_LIBRARIES} util)
target_include_directories(pysrv SYSTEM PRIVATE ${Python3_INCLUDE_DIRS} ${Python3_NumPy_INCLUDE_DIRS} ${PCG_RANDOM_INCLUDE_DIR})
add_dependencies(pysrv srvstub)

install(TARGETS pysrv RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT udf)
