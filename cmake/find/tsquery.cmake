option(ENABLE_TSQUERY "Enable TSQUERY" OFF)

if (ENABLE_TSQUERY)
    set (USE_TSQUERY 1)
    set (TSQEURY_DIR "${ClickHouse_SOURCE_DIR}/contrib/TSQuery")
    set (TSQEURY_INCLUDE_DIR "${TSQEURY_DIR}")
    set (TSQUERY_LIBRARY tsquery)

    message(STATUS "Using TSQuery = ${USE_TSQUERY} : ${TSQEURY_INCLUDE_DIR} ${TSQUERY_LIBRARY}")
else()
    set (USE_TSQUERY 0)
    message(STATUS "Using TSQuery = ${USE_TSQUERY} : ${TSQEURY_INCLUDE_DIR} ${TSQUERY_LIBRARY}")
endif()



