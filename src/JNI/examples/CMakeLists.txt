add_executable(jni_test jni_test.cpp)
target_link_libraries (jni_test PUBLIC clickhouse_jni_lib)