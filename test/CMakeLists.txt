project (ASDB)

# Settings for CppUTest
if (DEFINED ENV{CPPUTEST_HOME})
    message("Using CppUTest found in $ENV{CPPUTEST_HOME}")
    # include CppUTest headers
    include_directories($ENV{CPPUTEST_HOME}/include) 
    # add cpputest library
    add_library(imp_cpputest STATIC IMPORTED)
    set_property(TARGET imp_cpputest PROPERTY
                 IMPORTED_LOCATION $ENV{CPPUTEST_HOME}/lib/libCppUTest.a)
    link_libraries(imp_cpputest)
else ()
    # include CppUTest headers
    include (FindPkgConfig)
    pkg_check_modules(CPPUTEST REQUIRED cpputest>=3.0)
    message("Using CppUTest installed by system")
    include_directories(${CPPUTEST_INCLUDE_DIRS})
    link_directories(${CPPUTEST_LIBRARY_DIRS})
    link_libraries(${CPPUTEST_LIBRARIES})
endif()

# Build settings
include (FindPkgConfig)
# Settings for this project
set(CMAKE_C_FLAGS "-std=c99")
# SQLite3
pkg_check_modules(SQLITE3 REQUIRED sqlite3>=3.7.9)
link_directories(${SQLITE3_LIBRARY_DIRS})
link_libraries(${SQLITE3_LIBRARIES})
# GLib2.0
pkg_check_modules(GLIB2 REQUIRED glib-2.0)
include_directories(${GLIB2_INCLUDE_DIRS})
link_directories(${GLIB2_LIBRARY_DIRS})
link_libraries(${GLIB2_LIBRARIES})

# include project source
include_directories(${PROJECT_SOURCE_DIR}/../include)
include_directories(${PROJECT_SOURCE_DIR}/../src)

# # include mocks only for test code
# include_directories(${PROJECT_SOURCE_DIR}/../mocks)


# build test library
add_library(DummyTest dummy.cpp)
add_library(IndexTest idx_test.cpp)

add_executable(RunAllTests run_all_test.cpp)
target_link_libraries(RunAllTests DummyTest IndexTest ASDBTest ASDBTestAdapter)

add_custom_command(
  TARGET RunAllTests
  POST_BUILD
  COMMAND RunAllTests
  COMMENT "Running all tests"
#   VERBATIM
)
