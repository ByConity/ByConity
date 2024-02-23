option(ENABLE_LASFS "Enable LASFS" OFF)

if (NOT ENABLE_LASFS)
    set(USE_LASFS 0)
    return()
endif()
