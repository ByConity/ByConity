add_executable (filter_order_benchmark filter_order_benchmark.cpp)
target_link_libraries (filter_order_benchmark PRIVATE gbenchmark_all dbms)
SET_TARGET_PROPERTIES (filter_order_benchmark PROPERTIES COMPILE_FLAGS "-fno-access-control") 
