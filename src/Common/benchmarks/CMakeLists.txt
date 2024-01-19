add_executable(integer_hash_tables_and_hashes integer_hash_tables_and_hashes.cpp)
target_link_libraries (integer_hash_tables_and_hashes PRIVATE
    gbenchmark_all
    dbms
    ch_contrib::abseil_swiss_tables)

add_executable (MemcmpSmall_benchmark MemcmpSmall_benchmark.cpp)
target_link_libraries (MemcmpSmall_benchmark PRIVATE gbenchmark_all dbms)
SET_TARGET_PROPERTIES (MemcmpSmall_benchmark PROPERTIES COMPILE_FLAGS "-fno-access-control") 
