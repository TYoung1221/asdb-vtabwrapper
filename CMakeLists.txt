# 2.8でしか確認していない
cmake_minimum_required (VERSION 2.8)

project (ASDB)
set (ASDB_VERSION_MAJOR 0)
set (ASDB_VERSION_MINOR 3)
set (ASDB_VERSION_PATCH 0)

enable_language(C)

add_subdirectory(src)
add_subdirectory(test)

# Installer Package
include (InstallRequiredSystemLibraries)
# common
set (CPACK_RESOURCE_FILE_LICENSE
    "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE.txt")
set (CPACK_PACKAGE_VERSION_MAJOR "${ASDB_VERSION_MAJOR}")
set (CPACK_PACKAGE_VERSION_MINOR "${ASDB_VERSION_MINOR}")
set (CPACK_PACKAGE_VERSION_PATCH "${ASDB_VERSION_PATCH}")
set (CPACK_PACKAGE_DESCRIPTION_SUMMARY "ASDB - Any Source Database (for development)")
set (CPACK_PACKAGE_NAME "libasdb-dev")
# deb
set (CPACK_DEBIAN_PACKAGE_NAME "libasdb-dev")
set (CPACK_DEBIAN_PACKAGE_MAINTAINER "Makoto Shimazu <makoto.shimaz@gmail.com>")
set (CPACK_DEBIAN_PACKAGE_DEPENDS "libsqlite3-dev (>=3.8.2), libglib2.0-dev (>=2.40)")
# rpm
# set (CPACK_RPM_PACKAGE_NAME "libasdb-devel")
# set (CPACK_RPM_PACKAGE_LICENSE "MIT License")
# set (CPACK_RPM_PACKAGE_DEPENDS "libsqlite3-devel (>=3.8.2), libglib2.0-devel (>=2.40)") 
# generator
# set (CPACK_GENERATOR DEB;RPM)
set (CPACK_GENERATOR DEB)
include (CPack)
