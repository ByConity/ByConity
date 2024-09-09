set (LIBDIR "${ClickHouse_SOURCE_DIR}/contrib/glog/src")

set (GOOGLE_NAMESPACE google)
set (_START_GOOGLE_NAMESPACE_ "namespace ${GOOGLE_NAMESPACE} {")
set (_END_GOOGLE_NAMESPACE_ "}")

# check_type_size ("unsigned __int16" HAVE___UINT16)
# check_type_size (u_int16_t HAVE_U_INT16_T)
# check_type_size (uint16_t HAVE_UINT16_T)
set (HAVE_UINT16_T 1)

if (HAVE___UINT16)
  set (ac_cv_have___uint16 1)
else (HAVE___UINT16)
  set (ac_cv_have___uint16 0)
endif (HAVE___UINT16)

if (HAVE_INTTYPES_H)
  set (ac_cv_have_inttypes_h 1)
else (HAVE_INTTYPES_H)
  set (ac_cv_have_inttypes_h 0)
endif (HAVE_INTTYPES_H)

if (HAVE_LIB_GFLAGS)
  set (ac_cv_have_libgflags 1)
else (HAVE_LIB_GFLAGS)
  set (ac_cv_have_libgflags 0)
endif (HAVE_LIB_GFLAGS)

if (HAVE_STDINT_H)
  set (ac_cv_have_stdint_h 1)
else (HAVE_STDINT_H)
  set (ac_cv_have_stdint_h 0)
endif (HAVE_STDINT_H)

if (HAVE_SYS_TYPES_H)
  set (ac_cv_have_systypes_h 1)
else (HAVE_SYS_TYPES_H)
  set (ac_cv_have_systypes_h 0)
endif (HAVE_SYS_TYPES_H)

if (HAVE_U_INT16_T)
  set (ac_cv_have_u_int16_t 1)
else (HAVE_U_INT16_T)
  set (ac_cv_have_u_int16_t 0)
endif (HAVE_U_INT16_T)

if (HAVE_UINT16_T)
  set (ac_cv_have_uint16_t 1)
else (HAVE_UINT16_T)
  set (ac_cv_have_uint16_t 0)
endif (HAVE_UINT16_T)

if (HAVE_UNISTD_H)
  set (ac_cv_have_unistd_h 1)
else (HAVE_UNISTD_H)
  set (ac_cv_have_unistd_h 0)
endif (HAVE_UNISTD_H)

set (ac_google_namespace ${GOOGLE_NAMESPACE})
set (ac_google_end_namespace ${_END_GOOGLE_NAMESPACE_})
set (ac_google_start_namespace ${_START_GOOGLE_NAMESPACE_})

if (HAVE___ATTRIBUTE__)
  set (ac_cv___attribute___noreturn "__attribute__((noreturn))")
  set (ac_cv___attribute___noinline "__attribute__((noinline))")
elseif (HAVE___DECLSPEC)
  set (ac_cv___attribute___noreturn "__declspec(noreturn)")
endif (HAVE___ATTRIBUTE__)

if (HAVE___BUILTIN_EXPECT)
  set (ac_cv_have___builtin_expect 1)
else (HAVE___BUILTIN_EXPECT)
  set (ac_cv_have___builtin_expect 0)
endif (HAVE___BUILTIN_EXPECT)

if (HAVE_USING_OPERATOR)
  set (ac_cv_cxx_using_operator 1)
else (HAVE_USING_OPERATOR)
  set (ac_cv_cxx_using_operator 0)
endif (HAVE_USING_OPERATOR)

if (HAVE_EXECINFO_H)
  set (HAVE_STACKTRACE 1)
endif (HAVE_EXECINFO_H)

if (WITH_THREADS AND Threads_FOUND)
  if (CMAKE_USE_PTHREADS_INIT)
    set (HAVE_PTHREAD 1)
  endif (CMAKE_USE_PTHREADS_INIT)
else (WITH_THREADS AND Threads_FOUND)
  set (NO_THREADS 1)
endif (WITH_THREADS AND Threads_FOUND)

configure_file (${LIBDIR}/glog/logging.h.in ${CMAKE_CURRENT_BINARY_DIR}/glog/logging.h @ONLY)
configure_file (${LIBDIR}/glog/raw_logging.h.in ${CMAKE_CURRENT_BINARY_DIR}/glog/raw_logging.h @ONLY)
configure_file (${LIBDIR}/glog/stl_logging.h.in ${CMAKE_CURRENT_BINARY_DIR}/glog/stl_logging.h @ONLY)
configure_file (${LIBDIR}/glog/vlog_is_on.h.in ${CMAKE_CURRENT_BINARY_DIR}/glog/vlog_is_on.h @ONLY)

set (GLOG_PUBLIC_H
  ${CMAKE_CURRENT_BINARY_DIR}/glog/logging.h
  ${CMAKE_CURRENT_BINARY_DIR}/glog/raw_logging.h
  ${CMAKE_CURRENT_BINARY_DIR}/glog/stl_logging.h
  ${CMAKE_CURRENT_BINARY_DIR}/glog/vlog_is_on.h
  ${LIBDIR}/glog/log_severity.h
)

file(COPY ${LIBDIR}/glog/log_severity.h DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/glog)

set (GLOG_SRCS
  ${GLOG_PUBLIC_H}
  ${LIBDIR}/base/commandlineflags.h
  ${LIBDIR}/base/googleinit.h
  ${LIBDIR}/base/mutex.h
  ${LIBDIR}/demangle.cc
  ${LIBDIR}/demangle.h
  ${LIBDIR}/logging.cc
  ${LIBDIR}/raw_logging.cc
  ${LIBDIR}/symbolize.cc
  ${LIBDIR}/symbolize.h
  ${LIBDIR}/utilities.cc
  ${LIBDIR}/utilities.h
  ${LIBDIR}/vlog_is_on.cc
  ${LIBDIR}/signalhandler.cc
)

add_library(glog OBJECT ${GLOG_SRCS})

add_library(glog::glog ALIAS glog)

target_link_libraries (glog PUBLIC gflags::gflags)

target_include_directories(glog PRIVATE ${CMAKE_CURRENT_LIST_DIR})
target_include_directories(glog SYSTEM BEFORE PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
