add_executable (wal_action_metadata wal_action_metadata.cpp)
target_link_libraries (wal_action_metadata PRIVATE dbms)

add_executable (bitengine_test BitEngineDictionaryTest.cpp)
target_link_libraries (bitengine_test PRIVATE dbms)
