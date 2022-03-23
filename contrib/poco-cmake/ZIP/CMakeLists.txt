
if (USE_INTERNAL_POCO_LIBRARY)

    # Poco::ZIP 

    target_include_directories (_poco_xml_expat PUBLIC "${LIBRARY_DIR}/Zip/include")

    # Poco::XML

    file(GLOB SRCS "${LIBRARY_DIR}/Zip/src/*.cpp")

    add_library (_poco_zip ${SRCS})
    add_library (Poco::ZIP ALIAS _poco_zip)

    target_compile_options (_poco_zip PRIVATE -Wno-old-style-cast)
    target_include_directories (_poco_zip SYSTEM PUBLIC "${LIBRARY_DIR}/Zip/include")
    target_link_libraries (_poco_zip PUBLIC Poco::Foundation Poco::Util Poco::XML)
endif ()