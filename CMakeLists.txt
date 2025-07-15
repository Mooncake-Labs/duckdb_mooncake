cmake_minimum_required(VERSION 3.5)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(TARGET_NAME mooncake)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

include(FetchContent)

FetchContent_Declare(
    Corrosion
    GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
    GIT_TAG v0.5
)
FetchContent_MakeAvailable(Corrosion)

set(NANOARROW_NAMESPACE ${TARGET_NAME})
set(NANOARROW_IPC ON)
FetchContent_Declare(
    nanoarrow
    URL "https://github.com/apache/arrow-nanoarrow/archive/refs/tags/apache-arrow-nanoarrow-0.7.0.zip"
    URL_HASH SHA256=bd40a2ee849c650b243818b12e925cac17f8fc300acacd141177345fd6b2e9f4
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(nanoarrow)

find_package(roaring REQUIRED)

corrosion_import_crate(MANIFEST_PATH moonlink_ffi/Cargo.toml)

include_directories(
    duckdb/extension/parquet/include
    duckdb/third_party/parquet
    duckdb/third_party/thrift
    src/include)

set(EXTENSION_SOURCES
    src/mooncake_extension.cpp
    src/moonlink/moonlink.cpp
    src/storage/mooncake_catalog.cpp
    src/storage/mooncake_scan.cpp
    src/storage/mooncake_schema.cpp
    src/storage/mooncake_storage.cpp
    src/storage/mooncake_table.cpp
    src/storage/mooncake_table_metadata.cpp
    src/storage/mooncake_transaction.cpp
    src/storage/mooncake_transaction_manager.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

set(LINK_LIBRARIES
    nanoarrow::nanoarrow
    nanoarrow::nanoarrow_ipc
    roaring::roaring)

target_link_libraries(${EXTENSION_NAME} moonlink_ffi-static ${LINK_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} moonlink_ffi ${LINK_LIBRARIES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
