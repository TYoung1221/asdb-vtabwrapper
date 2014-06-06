project (ASDB)

include (FindPkgConfig)

# Settings for this project
set(CMAKE_C_FLAGS "-std=c99")

# Each Path
include_directories(${PROJECT_SOURCE_DIR}/../include)
include_directories(${PROJECT_BINARY_DIR})
configure_file (
  "${PROJECT_SOURCE_DIR}/../include/ASDBConfig.h.in"
  "${PROJECT_BINARY_DIR}/ASDBConfig.h"
)

# IF(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
#       ADD_DEFINITIONS(-fPIC)
# ENDIF(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")

# SQLite3
pkg_check_modules(SQLITE3 REQUIRED sqlite3>=3.7.9)
link_directories(${SQLITE3_LIBRARY_DIRS})
link_libraries(${SQLITE3_LIBRARIES})
# GLib2.0
pkg_check_modules(GLIB2 REQUIRED glib-2.0)
include_directories(${GLIB2_INCLUDE_DIRS})
link_directories(${GLIB2_LIBRARY_DIRS})
link_libraries(${GLIB2_LIBRARIES})

# TestLibrary
if(EXECTEST)
  function(createTestLibrary)
    # Add definition for execution of test
    add_definitions(-D__TEST__)
    add_library(ASDBTest asdb.c array.c)
  endfunction()
  createTestLibrary()
else()
  # Package
  add_library (asdb SHARED asdb.c array.c)
  install (TARGETS asdb DESTINATION lib)
  install (FILES "${PROJECT_SOURCE_DIR}/../include/asdb.h" DESTINATION include)
endif()