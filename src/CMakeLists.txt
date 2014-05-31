project (ASDB)

include (FindPkgConfig)

# Settings for this project
set(CMAKE_C_FLAGS "-std=c99")

# Each Path
include_directories(${PROJECT_SOURCE_DIR}/../include)
configure_file (
  "${PROJECT_SOURCE_DIR}/../include/ASDBConfig.h.in"
  "${PROJECT_BINARY_DIR}/ASDBConfig.h"
)

# SQLite3
pkg_check_modules(SQLITE3 REQUIRED sqlite3>=3.8)
# GLib2.0
pkg_check_modules(GLIB2 REQUIRED glib-2.0)
include_directories(${GLIB2_INCLUDE_DIRS})
link_directories(${GLIB2_LIBRARY_DIRS})
link_libraries(${GLIB2_LIBRARIES})

# Package
add_library (asdb SHARED asdb.c array.c)
install (TARGETS asdb DESTINATION lib)
install (FILES "${PROJECT_SOURCE_DIR}/../include/asdb.h" DESTINATION include)