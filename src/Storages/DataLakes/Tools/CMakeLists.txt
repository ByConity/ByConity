# libmockjvm will be build as shared lib. we use it to mock a libjvm.so to
# make sure BE can start without real java runtime
set_source_files_properties(mock_jvm.c PROPERTIES COMPILE_FLAGS "-shared")

add_library(mockjvm SHARED mock_jvm.c)

target_compile_options(mockjvm PRIVATE -w)

# output as libjvm.so
set_target_properties(mockjvm PROPERTIES OUTPUT_NAME "jvm")

# define `A` SUNWprivate_1.1 symbol to mockjvm to suppress some warning
target_link_options(mockjvm PRIVATE "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/mock_jvm.lds")
install(DIRECTORY DESTINATION lib)
install(TARGETS mockjvm DESTINATION lib)
