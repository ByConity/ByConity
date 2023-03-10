# - Find kerberos
# Find the native KERBEROS includes and library
#
#  KERBEROS_INCLUDE_DIRS - where to find krb5.h, etc.
#  KERBEROS_LIBRARIES    - List of libraries when using krb5.
#  KERBEROS_FOUND        - True if krb5 found.

IF (KERBEROS_INCLUDE_DIRS)
  # Already in cache, be silent
  SET(KERBEROS_FIND_QUIETLY TRUE)
ENDIF (KERBEROS_INCLUDE_DIRS)

FIND_PATH(KERBEROS_INCLUDE_DIRS krb5.h)

SET(KERBEROS_NAMES krb5 k5crypto com_err)
FIND_LIBRARY(KERBEROS_LIBRARIES NAMES ${KERBEROS_NAMES})

# handle the QUIETLY and REQUIRED arguments and set KERBEROS_FOUND to TRUE if
# all listed variables are TRUE
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(KERBEROS DEFAULT_MSG KERBEROS_LIBRARIES KERBEROS_INCLUDE_DIRS)

MARK_AS_ADVANCED(KERBEROS_LIBRARIES KERBEROS_INCLUDE_DIRS)
