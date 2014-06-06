project (ASDB)

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

# include project source
include_directories(${PROJECT_SOURCE_DIR}/../include)

# # include mocks only for test code
# include_directories(${PROJECT_SOURCE_DIR}/../mocks)

# build test library for LedDriver
add_library(DummyTest dummy.cpp)

add_executable(RunAllTests run_all_test.cpp)
target_link_libraries(RunAllTests DummyTest)

