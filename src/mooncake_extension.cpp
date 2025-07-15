#define DUCKDB_EXTENSION_MAIN

#include "duckdb/main/database.hpp"
#include "mooncake_extension.hpp"
#include "storage/mooncake_storage.hpp"

namespace duckdb {

void MooncakeExtension::Load(DuckDB &db) {
	auto &config = DBConfig::GetConfig(*db.instance);
	config.storage_extensions["mooncake"] = make_uniq<MooncakeStorageExtension>();
}

string MooncakeExtension::Name() {
	return "mooncake";
}

string MooncakeExtension::Version() const {
	return EXT_VERSION_MOONCAKE;
}

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void mooncake_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::MooncakeExtension>();
}

DUCKDB_EXTENSION_API const char *mooncake_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}
